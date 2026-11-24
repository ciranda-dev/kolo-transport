package ackhandler

import (
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/congestion"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/monotime"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/protocol"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/wire"
)

// SentPacketHandler handles ACKs received for outgoing packets
type SentPacketHandler interface {
	// SentPacket may modify the packet
	SentPacket(t monotime.Time, pn, largestAcked protocol.PacketNumber, streamFrames []StreamFrame, frames []Frame, encLevel protocol.EncryptionLevel, ecn protocol.ECN, size protocol.ByteCount, isPathMTUProbePacket, isPathProbePacket bool)
	// ReceivedAck processes an ACK frame.
	// It does not store a copy of the frame.
	ReceivedAck(f *wire.AckFrame, encLevel protocol.EncryptionLevel, rcvTime monotime.Time) (bool /* 1-RTT packet acked */, error)
	ReceivedPacket(protocol.EncryptionLevel, monotime.Time)
	ReceivedBytes(_ protocol.ByteCount, rcvTime monotime.Time)
	DropPackets(_ protocol.EncryptionLevel, rcvTime monotime.Time)
	ResetForRetry(rcvTime monotime.Time)

	// The SendMode determines if and what kind of packets can be sent.
	SendMode(now monotime.Time) SendMode
	// TimeUntilSend is the time when the next packet should be sent.
	// It is used for pacing packets.
	TimeUntilSend() monotime.Time
	SetMaxDatagramSize(count protocol.ByteCount)
	// SetCongestionControlFactory replaces the congestion controller now and after every path migration.
	SetCongestionControlFactory(f func(congestion.ByteCount) congestion.CongestionControl, initialSize protocol.ByteCount)
	// SetCubic replaces the Reno sender with the built-in Cubic one, now and after every path migration.
	SetCubic(initialSize protocol.ByteCount)
	// CongestionControlName is "reno" or "cubic" for the built-in sender and "custom" for a controller from a factory.
	CongestionControlName() string

	// only to be called once the handshake is complete
	QueueProbePacket(protocol.PacketNumberSpace) bool /* was a packet queued */

	ECNMode(isShortHeaderPacket bool) protocol.ECN // isShortHeaderPacket should only be true for non-coalesced 1-RTT packets
	PeekPacketNumber(protocol.PacketNumberSpace) (protocol.PacketNumber, protocol.PacketNumberLen)
	PopPacketNumber(protocol.PacketNumberSpace) protocol.PacketNumber

	GetLossDetectionTimeout() monotime.Time
	OnLossDetectionTimeout(now monotime.Time) error
	// BlackHoleDetected reports, once, that losses showed the path no longer carries packets larger than 1200 bytes.
	BlackHoleDetected() bool

	MigratedPath(now monotime.Time, initialMaxPacketSize protocol.ByteCount)
}
