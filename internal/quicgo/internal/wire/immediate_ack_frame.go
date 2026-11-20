package wire

import (
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/protocol"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/quicvarint"
)

// An ImmediateAckFrame is an IMMEDIATE_ACK frame
type ImmediateAckFrame struct{}

func (f *ImmediateAckFrame) Append(b []byte, _ protocol.Version) ([]byte, error) {
	return quicvarint.Append(b, uint64(FrameTypeImmediateAck)), nil
}

// Length of a written frame
func (f *ImmediateAckFrame) Length(_ protocol.Version) protocol.ByteCount {
	return protocol.ByteCount(quicvarint.Len(uint64(FrameTypeImmediateAck)))
}
