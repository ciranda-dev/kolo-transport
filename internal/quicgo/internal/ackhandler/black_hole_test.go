package ackhandler

import (
	"fmt"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/monotime"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/protocol"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/utils"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/wire"
)

func TestBlackHoleDetector(t *testing.T) {
	burst := func(d *blackHoleDetector, pn protocol.PacketNumber, sizes ...protocol.ByteCount) bool {
		for i, s := range sizes {
			d.lost(pn+protocol.PacketNumber(i), s)
		}
		return d.detected()
	}
	t.Run("four bursts of large packets", func(t *testing.T) {
		d := newBlackHoleDetector()
		for i := range 4 {
			d.acked(protocol.PacketNumber(10*i+5), 100, false)
			if got := burst(&d, protocol.PacketNumber(10*i), 1400, 1452); got != (i == 3) {
				t.Fatalf("burst %d: detected %v", i+1, got)
			}
		}
		for i := range 3 {
			if burst(&d, protocol.PacketNumber(100+10*i), 1400) {
				t.Fatalf("detected again after %d bursts", i+1)
			}
		}
	})
	t.Run("a burst with a base-size packet", func(t *testing.T) {
		d := newBlackHoleDetector()
		for i := range 8 {
			if burst(&d, protocol.PacketNumber(10*i), 1452, 1200) {
				t.Fatal("detected")
			}
		}
	})
	t.Run("a later large packet acknowledged", func(t *testing.T) {
		d := newBlackHoleDetector()
		for i := range 3 {
			burst(&d, protocol.PacketNumber(10*i), 1400)
		}
		d.acked(35, 1400, false)
		if burst(&d, 40, 1400) {
			t.Fatal("bursts the acknowledgement explains were counted")
		}
	})
	t.Run("bursts older than an acknowledged packet of their size", func(t *testing.T) {
		d := newBlackHoleDetector()
		d.acked(100, 1452, false)
		for i := range 8 {
			if burst(&d, protocol.PacketNumber(10*i), 1400) {
				t.Fatal("detected")
			}
		}
	})
	t.Run("a smaller packet acknowledged", func(t *testing.T) {
		d := newBlackHoleDetector()
		for i := range 3 {
			burst(&d, protocol.PacketNumber(10*i), 1400)
		}
		d.acked(35, 1300, false)
		if !burst(&d, 40, 1400) {
			t.Fatal("a 1300-byte packet cleared bursts of 1400-byte packets")
		}
	})
	t.Run("a path MTU probe acknowledged", func(t *testing.T) {
		d := newBlackHoleDetector()
		for i := range 3 {
			burst(&d, protocol.PacketNumber(10*i), 1400)
		}
		d.acked(35, 1300, true)
		if burst(&d, 40, 1400) {
			t.Fatal("bursts before an acknowledged probe were counted")
		}
	})
}

// The sent packet handler reports a black hole when only the small packets of a connection are acknowledged, and
// after probe timeouts in a row with large packets outstanding, never for the loss of base-size packets.
func TestSentPacketHandlerBlackHole(t *testing.T) {
	newHandler := func() *sentPacketHandler {
		h := NewSentPacketHandler(0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false, nil,
			protocol.PerspectiveServer, nil, utils.DefaultLogger).(*sentPacketHandler)
		h.DropPackets(protocol.EncryptionInitial, monotime.Now())
		h.DropPackets(protocol.EncryptionHandshake, monotime.Now())
		return h
	}
	var acked []protocol.PacketNumber
	send := func(h *sentPacketHandler, now monotime.Time, size protocol.ByteCount) protocol.PacketNumber {
		pn := h.PopPacketNumber(protocol.PacketNumberSpaceAppData)
		h.SentPacket(now, pn, protocol.InvalidPacketNumber, nil, []Frame{{Frame: &wire.PingFrame{}}},
			protocol.Encryption1RTT, protocol.ECNNon, size, false, false)
		return pn
	}
	ack := func(h *sentPacketHandler, now monotime.Time, pns ...protocol.PacketNumber) {
		acked = append(acked, pns...)
		f := &wire.AckFrame{}
		for i := len(acked) - 1; i >= 0; i-- {
			if n := len(f.AckRanges); n > 0 && f.AckRanges[n-1].Smallest == acked[i]+1 {
				f.AckRanges[n-1].Smallest = acked[i]
				continue
			}
			f.AckRanges = append(f.AckRanges, wire.AckRange{Smallest: acked[i], Largest: acked[i]})
		}
		if _, err := h.ReceivedAck(f, protocol.Encryption1RTT, now); err != nil {
			t.Fatal(err)
		}
	}
	for _, large := range []protocol.ByteCount{1400, 1200} {
		acked = nil
		h := newHandler()
		now := monotime.Now()
		var got []bool
		for range 5 {
			send(h, now, large)
			small := []protocol.PacketNumber{send(h, now, 100), send(h, now, 100), send(h, now, 100)}
			now = now.Add(10 * time.Millisecond)
			ack(h, now, small...)
			got = append(got, h.BlackHoleDetected())
		}
		if want := large > 1200; got[3] != want || got[0] || got[1] || got[2] {
			t.Fatalf("losses of %d-byte packets with the small ones acknowledged: reports %v", large, got)
		}
	}
	for _, large := range []protocol.ByteCount{1400, 1200} {
		acked = nil
		h := newHandler()
		now := monotime.Now()
		ack(h, now, send(h, now, 100))
		send(h, now, large)
		var got []bool
		for range 4 {
			now = h.GetLossDetectionTimeout()
			if err := h.OnLossDetectionTimeout(now); err != nil {
				t.Fatal(err)
			}
			got = append(got, h.BlackHoleDetected())
			send(h, now, large)
		}
		if want := large > 1200; got[2] != want || got[0] || got[1] {
			t.Fatalf("probe timeouts with a %d-byte packet outstanding: reports %v", large, got)
		}
	}
}

// Large packets sent before a report and lost after it do not lead to another one: they were sent at the old size.
func TestBlackHoleIgnoresPacketsSentBeforeAReport(t *testing.T) {
	h := NewSentPacketHandler(0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false, nil,
		protocol.PerspectiveServer, nil, utils.DefaultLogger).(*sentPacketHandler)
	h.DropPackets(protocol.EncryptionInitial, monotime.Now())
	h.DropPackets(protocol.EncryptionHandshake, monotime.Now())
	now := monotime.Now()
	send := func(size protocol.ByteCount) protocol.PacketNumber {
		pn := h.PopPacketNumber(protocol.PacketNumberSpaceAppData)
		h.SentPacket(now, pn, protocol.InvalidPacketNumber, nil, []Frame{{Frame: &wire.PingFrame{}}},
			protocol.Encryption1RTT, protocol.ECNNon, size, false, false)
		return pn
	}
	var groups [][]protocol.PacketNumber
	for range 8 {
		send(1400)
		groups = append(groups, []protocol.PacketNumber{send(100), send(100), send(100)})
	}
	var acked []protocol.PacketNumber
	var got []bool
	for _, g := range groups {
		acked = append(acked, g...)
		f := &wire.AckFrame{}
		for i := len(acked) - 1; i >= 0; i-- {
			if n := len(f.AckRanges); n > 0 && f.AckRanges[n-1].Smallest == acked[i]+1 {
				f.AckRanges[n-1].Smallest = acked[i]
				continue
			}
			f.AckRanges = append(f.AckRanges, wire.AckRange{Smallest: acked[i], Largest: acked[i]})
		}
		if _, err := h.ReceivedAck(f, protocol.Encryption1RTT, now); err != nil {
			t.Fatal(err)
		}
		got = append(got, h.BlackHoleDetected())
	}
	if want := []bool{false, false, false, true, false, false, false, false}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("reports %v, want %v", got, want)
	}
}
