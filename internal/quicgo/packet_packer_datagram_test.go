package quic

import (
	"testing"

	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/monotime"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/protocol"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/utils"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/wire"
)

type ackForEveryPacket struct{}

func (ackForEveryPacket) GetAckFrame(protocol.EncryptionLevel, monotime.Time, bool) *wire.AckFrame {
	return &wire.AckFrame{AckRanges: []wire.AckRange{{Smallest: 1, Largest: 1}}}
}

// A queued DATAGRAM frame larger than a whole packet, as after the packet size fell back to 1200 bytes, is dropped
// even when every packet carries an ACK, so it does not hold back the datagrams behind it.
func TestDatagramLargerThanAPacketIsDropped(t *testing.T) {
	q := newDatagramQueue(func() {}, utils.DefaultLogger, nil)
	p := &packetPacker{acks: ackForEveryPacket{}, framer: newFramer(nil), retransmissionQueue: newRetransmissionQueue(), datagramQueue: q}
	small := &wire.DatagramFrame{DataLenPresent: true, Data: make([]byte, 100)}
	for _, f := range []*wire.DatagramFrame{{DataLenPresent: true, Data: make([]byte, 1300)}, small} {
		if ok, err := q.TryAdd(f); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	pl := p.composeNextPacket(1163, false, true, monotime.Now(), protocol.Version1)
	if pl.ack == nil || pl.numDatagramFrames != 1 || pl.frames[len(pl.frames)-1].Frame != small {
		t.Fatalf("packed %d datagram frames, want only the 100-byte one", pl.numDatagramFrames)
	}
	if q.Peek() != nil {
		t.Fatal("a datagram is still queued")
	}
}
