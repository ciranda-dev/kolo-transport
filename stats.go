package kolotransport

import "time"

// ConnStats is a snapshot of what a connection measured about its network path and its own traffic.
// Byte counts are QUIC packet bytes: they include QUIC headers and retransmissions, not UDP or IP headers.
type ConnStats struct {
	// SmoothedRTT is the exponentially weighted moving average of the round-trip time samples (RFC 9002, section 5.3).
	// It is 0 until the current path has a sample, which a new connection has once its handshake completes.
	SmoothedRTT time.Duration
	// MinRTT is the lowest round-trip time seen on the current path, which is close to the propagation delay.
	MinRTT time.Duration
	// LatestRTT is the last round-trip time sample on the current path.
	LatestRTT time.Duration
	// RTTVariation is the mean deviation of the samples from SmoothedRTT (the RTTVAR of RFC 9002), a measure of jitter.
	RTTVariation time.Duration

	// BytesSent counts every byte sent, including packets later declared lost and retransmitted data.
	BytesSent uint64
	// BytesReceived counts every byte received, including duplicates and packets that could not be processed.
	BytesReceived uint64
	// PacketsSent counts every packet sent, including packets later declared lost.
	PacketsSent uint64
	// PacketsReceived counts every packet received, including duplicates and packets that could not be processed.
	PacketsReceived uint64
	// PacketsLost counts the packets of this end the peer did not acknowledge in time, and each congestion signal of ECN
	// (a peer that marks packets can raise it without any loss, as the built-in sender counts it too). A packet that arrives
	// late after it was declared lost still counts, so PacketsLost over PacketsSent is an upper bound of the loss rate.
	PacketsLost uint64
	// BytesLost is the size in bytes of the packets counted in PacketsLost.
	BytesLost uint64

	// CongestionWindow is how many bytes this end may have in flight, as set by the congestion controller.
	// A transfer is limited by the congestion window when BytesInFlight stays near it.
	CongestionWindow uint64
	// BytesInFlight is how many bytes of packets this end sent that the peer has not yet acknowledged and this end has not declared lost.
	BytesInFlight uint64
	// CongestionController is the name of the controller sending on this connection: "reno", "cubic" or "bbr".
	CongestionController string
	// BandwidthEstimate is the bottleneck bandwidth the controller measured, in bytes per second: the highest delivery
	// rate of the last ten round trips. Only "bbr" measures it; it is 0 for "reno" and "cubic" and before the first sample.
	BandwidthEstimate uint64
	// MaxDatagramSize is the largest UDP payload in bytes this end sends on the current path. It starts at the
	// initial size and grows as path MTU discovery succeeds.
	MaxDatagramSize uint64
	// PathChanges counts how often the connection moved to a new network path, for example when the peer's NAT gave it a new port.
	// The RTT fields, the congestion window and BandwidthEstimate start over on a new path.
	PathChanges uint64
}

// Stats returns a snapshot of the connection's statistics. It is cheap, does not wait on a network read and is safe from
// any goroutine. The fields are read one by one, not atomically, so a moving value can disagree with its neighbours.
// After the connection has ended it returns the zero ConnStats.
func (c *Conn) Stats() ConnStats {
	if c.qc.Context().Err() != nil {
		return ConnStats{}
	}
	s := c.qc.ConnectionStats()
	out := ConnStats{
		BytesSent:            s.BytesSent,
		BytesReceived:        s.BytesReceived,
		PacketsSent:          s.PacketsSent,
		PacketsReceived:      s.PacketsReceived,
		PacketsLost:          s.PacketsLost,
		BytesLost:            s.BytesLost,
		CongestionWindow:     s.CongestionWindow,
		BytesInFlight:        s.BytesInFlight,
		CongestionController: c.qc.CongestionControlName(),
		BandwidthEstimate:    s.BandwidthEstimate,
		MaxDatagramSize:      s.MaxDatagramSize,
		PathChanges:          s.PathChanges,
	}
	if s.HasRTT {
		out.SmoothedRTT, out.MinRTT, out.LatestRTT, out.RTTVariation = s.SmoothedRTT, s.MinRTT, s.LatestRTT, s.MeanDeviation
	}
	return out
}
