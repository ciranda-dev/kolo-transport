package bbr

import (
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/quicgo/congestion"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/monotime"
)

type fixedRTT time.Duration

func (r fixedRTT) MinRTT() time.Duration { return time.Duration(r) }

func newTestSender() *bbrSender {
	b := NewBbrSender(DefaultClock{}, 1200)
	b.SetRTTStatsProvider(fixedRTT(50 * time.Millisecond))
	return b
}

func TestStartsInStartupWithTheInitialWindow(t *testing.T) {
	b := newTestSender()
	if !b.InSlowStart() || b.InRecovery() {
		t.Fatal("not in startup")
	}
	if got, want := b.GetCongestionWindow(), congestion.ByteCount(32*1200); got != want {
		t.Fatalf("initial window %d, want %d", got, want)
	}
	if !b.CanSend(0) || b.CanSend(b.GetCongestionWindow()) {
		t.Fatal("CanSend does not follow the window")
	}
}

// A datagram size is only ever raised by path MTU discovery; a smaller one must not crash the connection.
func TestASmallerDatagramSizeIsIgnored(t *testing.T) {
	b := newTestSender()
	b.SetMaxDatagramSize(1452)
	window := b.GetCongestionWindow()
	if want := congestion.ByteCount(32 * 1452); window != want {
		t.Fatalf("window after a larger datagram size %d, want %d", window, want)
	}
	b.SetMaxDatagramSize(1200)
	if got := b.GetCongestionWindow(); got != window || b.maxDatagramSize != 1452 {
		t.Fatalf("window %d, size %d after a smaller size, want %d and 1452", got, b.maxDatagramSize, window)
	}
}

func sendAt(s *bandwidthSampler, pn congestion.PacketNumber) {
	s.OnPacketSent(monotime.Now(), pn, 1200, 0, true)
}

// One packet and another a million numbers later must not cost a slot for every number in between.
func TestAGapInPacketNumbersIsBounded(t *testing.T) {
	s := newBandwidthSampler(10)
	sendAt(s, 1)
	sendAt(s, 1_000_001)
	q := s.connectionStateMap
	if got := q.EntrySlotsUsed(); got > maxPacketNumberSpan {
		t.Fatalf("%d slots after a gap of a million, want at most %d", got, maxPacketNumberSpan)
	}
	if q.NumberOfPresentEntries() != 1 || q.GetEntry(1) != nil || q.GetEntry(1_000_001) == nil {
		t.Fatal("the oldest sample must go and the newest stay")
	}
	if cap := len(q.entries.ring); cap > 2*maxPacketNumberSpan {
		t.Fatalf("ring of %d slots", cap)
	}
}

// A peer that acknowledges nothing and sends ack-eliciting packets with an ACK-only run between them, as a hostile one can.
func TestTheSpanStaysBoundedWhenNothingIsAcknowledged(t *testing.T) {
	s := newBandwidthSampler(10)
	for pn := congestion.PacketNumber(1); pn < 600_000; pn += 20 {
		sendAt(s, pn)
	}
	q := s.connectionStateMap
	if q.EntrySlotsUsed() > maxPacketNumberSpan || len(q.entries.ring) > 2*maxPacketNumberSpan {
		t.Fatalf("%d slots in a ring of %d, want at most %d", q.EntrySlotsUsed(), len(q.entries.ring), maxPacketNumberSpan)
	}
	if q.GetEntry(599_981) == nil {
		t.Fatal("the newest packet lost its sample")
	}
}

// A packet that fell out of the span is acknowledged without a sample and without harm, and the others still give one.
func TestAPacketPastTheSpanGivesNoSample(t *testing.T) {
	s := newBandwidthSampler(10)
	sendAt(s, 1)
	sendAt(s, 2*maxPacketNumberSpan)
	now := monotime.Now().Add(10 * time.Millisecond)
	if s.onPacketAcknowledged(now, 1).stateAtSend.isValid {
		t.Fatal("a sample for a packet that was dropped")
	}
	if !s.onPacketAcknowledged(now, 2*maxPacketNumberSpan).stateAtSend.isValid {
		t.Fatal("no sample for a packet inside the span")
	}
}

// An honest transfer keeps its samples: a full window of packets with ACK-only numbers between them is inside the span.
func TestAFullWindowKeepsEverySample(t *testing.T) {
	s := newBandwidthSampler(10)
	last := congestion.PacketNumber(0)
	for i := 0; i < congestion.MaxCongestionWindowPackets; i++ {
		last = congestion.PacketNumber(1 + 2*i)
		sendAt(s, last)
	}
	if s.connectionStateMap.NumberOfPresentEntries() != congestion.MaxCongestionWindowPackets || s.connectionStateMap.GetEntry(1) == nil {
		t.Fatal("a full window lost samples")
	}
}

// The products of a rate and a time exceed int64 on a fast link after a long idle epoch: the result must be right where it
// fits and saturate, never wrap to a negative or small number, where it does not.
func TestRateArithmeticDoesNotOverflow(t *testing.T) {
	const gbit = Bandwidth(1e9)
	for _, c := range []struct {
		name string
		got  congestion.ByteCount
		want congestion.ByteCount
	}{
		{"bdp 10 Gbit/s x 1 s", bdpFromRttAndBandwidth(time.Second, 10*gbit), 1_250_000_000},
		{"bdp 92 Gbit/s x 100 ms", bdpFromRttAndBandwidth(100*time.Millisecond, 92*gbit), 1_150_000_000},
		{"bdp 100 Gbit/s x 1 s", bdpFromRttAndBandwidth(time.Second, 100*gbit), 12_500_000_000},
		{"bdp infinite bandwidth", bdpFromRttAndBandwidth(time.Second, infBandwidth), maxBytes},
		{"bdp infinite rtt", bdpFromRttAndBandwidth(infRTT, 10*gbit), maxBytes},
		{"bdp without a rate", bdpFromRttAndBandwidth(time.Second, 0), 0},
		{"bytes 10 Gbit/s over 9 s", bytesFromBandwidthAndTimeDelta(10*gbit, 9*time.Second), 11_250_000_000},
		{"bytes 10 Gbit/s over 1 h", bytesFromBandwidthAndTimeDelta(10*gbit, time.Hour), 4_500_000_000_000},
		{"bytes of a huge rate over 10 s", bytesFromBandwidthAndTimeDelta(maxRate, 10*time.Second), maxBytes},
		{"bytes over a negative delta", bytesFromBandwidthAndTimeDelta(10*gbit, -time.Second), 0},
	} {
		if c.got != c.want {
			t.Errorf("%s: %d, want %d", c.name, c.got, c.want)
		}
	}
	if got := BandwidthFromDelta(1<<40, time.Nanosecond); got <= 0 || got > maxRate {
		t.Errorf("a terabyte in a nanosecond: %d", got)
	}
	if got, want := BandwidthFromDelta(1_250_000, time.Millisecond), 10*gbit; got != want {
		t.Errorf("1.25 MB in 1 ms: %d, want %d", got, want)
	}
	if BandwidthFromDelta(1200, 0) != 0 {
		t.Error("a zero delta must give no bandwidth")
	}
	p := NewPacer(func() congestion.ByteCount { return 1 << 60 })
	now := monotime.Now()
	p.SentPacket(now, 1200)
	if b := p.Budget(now.Add(time.Hour)); b <= 0 || b < 10*1200 {
		t.Errorf("pacer budget %d after an hour at a huge rate", b)
	}
}
