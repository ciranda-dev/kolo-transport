package utils

import "sync/atomic"

// ConnectionStats stores stats for the connection. See the public
// ConnectionStats struct in connection.go for more information
type ConnectionStats struct {
	CongestionWindow atomic.Uint64
	BytesInFlight    atomic.Uint64
	// BandwidthEstimate is the bandwidth estimate of a controller that keeps one, in bytes per second.
	BandwidthEstimate atomic.Uint64
	PathChanges       atomic.Uint64

	BytesSent       atomic.Uint64
	PacketsSent     atomic.Uint64
	BytesReceived   atomic.Uint64
	PacketsReceived atomic.Uint64
	BytesLost       atomic.Uint64
	PacketsLost     atomic.Uint64
}
