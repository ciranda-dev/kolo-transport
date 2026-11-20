// Package congestion is the interface of a pluggable congestion controller.
package congestion

import (
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/protocol"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/monotime"
)

type (
	ByteCount    protocol.ByteCount
	PacketNumber protocol.PacketNumber
)

const (
	InitialPacketSize          = protocol.InitialPacketSize
	MaxPacketBufferSize        = protocol.MaxPacketBufferSize
	MaxCongestionWindowPackets = protocol.MaxCongestionWindowPackets
	PacketsPerConnectionID     = protocol.PacketsPerConnectionID
)

// MinPacingDelay is the value of the unexported constant of the same name in internal/congestion.
const MinPacingDelay = time.Millisecond

type AckedPacketInfo struct {
	PacketNumber PacketNumber
	BytesAcked   ByteCount
}

type LostPacketInfo struct {
	PacketNumber PacketNumber
	BytesLost    ByteCount
}

type CongestionControl interface {
	SetRTTStatsProvider(provider RTTStatsProvider)
	TimeUntilSend(bytesInFlight ByteCount) monotime.Time
	HasPacingBudget(now monotime.Time) bool
	OnPacketSent(sentTime monotime.Time, bytesInFlight ByteCount, packetNumber PacketNumber, bytes ByteCount, isRetransmittable bool)
	CanSend(bytesInFlight ByteCount) bool
	MaybeExitSlowStart()
	OnPacketAcked(number PacketNumber, ackedBytes ByteCount, priorInFlight ByteCount, eventTime monotime.Time)
	OnCongestionEvent(number PacketNumber, lostBytes ByteCount, priorInFlight ByteCount)
	OnRetransmissionTimeout(packetsRetransmitted bool)
	SetMaxDatagramSize(size ByteCount)
	InSlowStart() bool
	InRecovery() bool
	GetCongestionWindow() ByteCount
}

// CongestionControlEx also receives every acknowledgment and loss of one ACK or loss timer event together.
type CongestionControlEx interface {
	CongestionControl
	OnCongestionEventEx(priorInFlight ByteCount, eventTime monotime.Time, ackedPackets []AckedPacketInfo, lostPackets []LostPacketInfo)
}

type RTTStatsProvider interface {
	MinRTT() time.Duration
}
