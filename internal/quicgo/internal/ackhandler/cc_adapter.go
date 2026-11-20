package ackhandler

import (
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/congestion"
	cgInternal "github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/congestion"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/monotime"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/protocol"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/utils"
)

var (
	_ cgInternal.SendAlgorithmWithDebugInfos = &ccAdapter{}
	_ cgInternal.SendAlgorithmEx             = &ccAdapterEx{}
)

// ccAdapter lets a pluggable controller stand in for the built-in sender. The built-in sender
// keeps the connection stats current; the adapter does the same for the controller.
type ccAdapter struct {
	cc        congestion.CongestionControl
	connStats *utils.ConnectionStats
}

// Name is the controller's own name, or "custom" when it has none.
func (a *ccAdapter) Name() string {
	if n, ok := a.cc.(interface{ Name() string }); ok {
		return n.Name()
	}
	return "custom"
}

func (a *ccAdapter) pluggable() {}

func (a *ccAdapter) updateStats() {
	a.connStats.CongestionWindow.Store(uint64(a.cc.GetCongestionWindow()))
	if b, ok := a.cc.(interface{ BandwidthEstimate() uint64 }); ok {
		a.connStats.BandwidthEstimate.Store(b.BandwidthEstimate())
	}
}

func (a *ccAdapter) TimeUntilSend(bytesInFlight protocol.ByteCount) monotime.Time {
	return a.cc.TimeUntilSend(congestion.ByteCount(bytesInFlight))
}

func (a *ccAdapter) HasPacingBudget(now monotime.Time) bool {
	return a.cc.HasPacingBudget(now)
}

func (a *ccAdapter) OnPacketSent(sentTime monotime.Time, bytesInFlight protocol.ByteCount, packetNumber protocol.PacketNumber, bytes protocol.ByteCount, isRetransmittable bool) {
	a.cc.OnPacketSent(sentTime, congestion.ByteCount(bytesInFlight), congestion.PacketNumber(packetNumber), congestion.ByteCount(bytes), isRetransmittable)
}

func (a *ccAdapter) CanSend(bytesInFlight protocol.ByteCount) bool {
	return a.cc.CanSend(congestion.ByteCount(bytesInFlight))
}

func (a *ccAdapter) MaybeExitSlowStart() {
	a.cc.MaybeExitSlowStart()
}

func (a *ccAdapter) OnPacketAcked(number protocol.PacketNumber, ackedBytes protocol.ByteCount, priorInFlight protocol.ByteCount, eventTime monotime.Time) {
	a.cc.OnPacketAcked(congestion.PacketNumber(number), congestion.ByteCount(ackedBytes), congestion.ByteCount(priorInFlight), eventTime)
	a.updateStats()
}

func (a *ccAdapter) OnCongestionEvent(number protocol.PacketNumber, lostBytes protocol.ByteCount, priorInFlight protocol.ByteCount) {
	a.connStats.PacketsLost.Add(1)
	a.connStats.BytesLost.Add(uint64(lostBytes))
	a.cc.OnCongestionEvent(congestion.PacketNumber(number), congestion.ByteCount(lostBytes), congestion.ByteCount(priorInFlight))
	a.updateStats()
}

// OnSpuriousLoss does nothing: the controller interface has no way to undo a congestion event.
func (a *ccAdapter) OnSpuriousLoss(protocol.PacketNumber) {}

func (a *ccAdapter) OnRetransmissionTimeout(packetsRetransmitted bool) {
	a.cc.OnRetransmissionTimeout(packetsRetransmitted)
	a.updateStats()
}

func (a *ccAdapter) SetMaxDatagramSize(size protocol.ByteCount) {
	a.cc.SetMaxDatagramSize(congestion.ByteCount(size))
	a.updateStats()
}

func (a *ccAdapter) InSlowStart() bool {
	return a.cc.InSlowStart()
}

func (a *ccAdapter) InRecovery() bool {
	return a.cc.InRecovery()
}

func (a *ccAdapter) GetCongestionWindow() protocol.ByteCount {
	return protocol.ByteCount(a.cc.GetCongestionWindow())
}

// ccAdapterEx is the adapter of a controller that takes the acknowledgments and losses of an event together.
type ccAdapterEx struct {
	*ccAdapter
	ex congestion.CongestionControlEx
}

func (a *ccAdapterEx) OnCongestionEventEx(priorInFlight protocol.ByteCount, eventTime monotime.Time, ackedPackets []congestion.AckedPacketInfo, lostPackets []congestion.LostPacketInfo) {
	a.ex.OnCongestionEventEx(congestion.ByteCount(priorInFlight), eventTime, ackedPackets, lostPackets)
	a.updateStats()
}
