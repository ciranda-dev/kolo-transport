package bbr

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/quicgo/congestion"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/monotime"
)

type fakeClock struct{ now monotime.Time }

func (c *fakeClock) Now() monotime.Time { return c.now }

type varyingRTT struct{ rtt time.Duration }

func (r *varyingRTT) MinRTT() time.Duration { return r.rtt }

// drive feeds a sender a script of sends, acknowledgments, losses, datagram size changes and clock steps, in any order and
// with unknown, repeated and out-of-order packet numbers, as a connection or a hostile peer can produce them.
func drive(t testing.TB, script []byte) {
	clock := &fakeClock{now: monotime.Now()}
	rtt := &varyingRTT{rtt: 50 * time.Millisecond}
	b := NewBbrSender(clock, 1200)
	b.SetRTTStatsProvider(rtt)
	var next, inFlight congestion.ByteCount
	size := congestion.ByteCount(1200)
	var sent []congestion.PacketNumber
	for len(script) >= 3 {
		op, arg := script[0]%7, int(binary.BigEndian.Uint16(script[1:3]))
		script = script[3:]
		switch op {
		case 0, 1:
			pn := congestion.PacketNumber(next)
			next += congestion.ByteCount(1 + arg%30)
			b.OnPacketSent(clock.now, inFlight, pn, size, arg%5 != 0)
			sent = append(sent, pn)
			inFlight += size
		case 2, 3:
			if len(sent) == 0 {
				continue
			}
			i := arg % len(sent)
			var lost []congestion.LostPacketInfo
			if arg%4 == 0 {
				lost = []congestion.LostPacketInfo{{PacketNumber: sent[(i+1)%len(sent)], BytesLost: size}}
			}
			b.OnCongestionEventEx(inFlight, clock.now, []congestion.AckedPacketInfo{{PacketNumber: sent[i], BytesAcked: size}}, lost)
			inFlight = max(0, inFlight-size)
		case 4:
			if len(sent) != 0 {
				b.OnCongestionEventEx(inFlight, clock.now, nil, []congestion.LostPacketInfo{{PacketNumber: sent[arg%len(sent)], BytesLost: size}})
			}
		case 5:
			clock.now = clock.now.Add(time.Duration(arg) * 100 * time.Microsecond)
			rtt.rtt = time.Duration(arg%200) * time.Millisecond
		case 6:
			size = congestion.ByteCount(1200 + arg%300)
			b.SetMaxDatagramSize(congestion.ByteCount(1000 + arg%600))
		}
		if w := b.GetCongestionWindow(); w <= 0 {
			t.Fatalf("congestion window %d", w)
		}
		b.TimeUntilSend(inFlight)
		b.HasPacingBudget(clock.now)
	}
}

// FuzzSender must never panic, hang or leave the window at or below zero, whatever it is fed.
func FuzzSender(f *testing.F) {
	f.Add([]byte{0, 0, 1, 2, 0, 1, 5, 0, 200, 2, 0, 0})
	f.Add([]byte{0, 0, 0, 0, 255, 255, 5, 255, 255, 2, 0, 0, 4, 0, 0, 6, 0, 9})
	f.Add([]byte{5, 0, 0, 0, 0, 29, 2, 0, 3, 2, 0, 3, 6, 0, 0})
	f.Fuzz(func(t *testing.T, script []byte) { drive(t, script) })
}

// A train of acknowledgments at a steady rate gives that rate as the estimate, and a window that holds a bandwidth-delay
// product, as the model says it must.
func TestAScriptedAckTrainGivesTheRate(t *testing.T) {
	clock := &fakeClock{now: monotime.Now()}
	b := NewBbrSender(clock, 1200)
	b.SetRTTStatsProvider(&varyingRTT{rtt: 50 * time.Millisecond})
	const size, rtt, gap = 1200, 50 * time.Millisecond, time.Millisecond // 1.2 MB/s
	type flight struct {
		pn int
		at monotime.Time
	}
	var queue []flight
	var inFlight congestion.ByteCount
	for i := 0; i < 2000; i++ {
		b.OnPacketSent(clock.now, inFlight, congestion.PacketNumber(i), size, true)
		inFlight += size
		queue = append(queue, flight{i, clock.now})
		clock.now = clock.now.Add(gap)
		var acked []congestion.AckedPacketInfo
		for len(queue) != 0 && clock.now.Sub(queue[0].at) >= rtt {
			acked = append(acked, congestion.AckedPacketInfo{PacketNumber: congestion.PacketNumber(queue[0].pn), BytesAcked: size})
			queue = queue[1:]
		}
		if len(acked) != 0 {
			b.OnCongestionEventEx(inFlight, clock.now, acked, nil)
			inFlight -= congestion.ByteCount(len(acked)) * size
		}
	}
	if got := b.BandwidthEstimate(); got < 1_100_000 || got > 1_300_000 {
		t.Fatalf("estimate %d bytes/s, want about 1200000", got)
	}
	if w, bdp := b.GetCongestionWindow(), congestion.ByteCount(1_200_000*50/1000); w < bdp || w > 4*bdp {
		t.Fatalf("window %d bytes for a bandwidth-delay product of %d", w, bdp)
	}
	if b.InSlowStart() {
		t.Fatal("still in start-up after a steady rate")
	}
}
