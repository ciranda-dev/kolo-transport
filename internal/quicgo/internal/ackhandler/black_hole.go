package ackhandler

import "github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/protocol"

// blackHoleThreshold is MAX_PROBES of RFC 8899 section 5.1.2: more suspicious loss bursts than this, or this many
// probe timeouts in a row, show that the path stopped carrying the packet size in use (RFC 8899 section 4.3).
const blackHoleThreshold = 3

// A blackHoleDetector judges from losses whether the path stopped carrying packets larger than the base size of
// 1200 bytes (RFC 9000 section 14.1), as RFC 8899 section 4.3 suggests. The packets declared lost by one loss detection
// pass form a burst. A burst is suspicious when all its packets are larger than the base size and no packet at least as
// large as its smallest one, sent after it, was acknowledged: a smaller path MTU explains it fully.
type blackHoleDetector struct {
	suspicious [blackHoleThreshold + 1]protocol.ByteCount // the smallest packet of each suspicious burst
	n          int

	burstMin  protocol.ByteCount // 0 while no burst is open
	burstLast protocol.PacketNumber

	// ackedSize is the largest packet acknowledged since the latest suspicious burst (the base size if none), and
	// ackedPN the newest acknowledged packet of that size.
	ackedSize protocol.ByteCount
	ackedPN   protocol.PacketNumber
}

func newBlackHoleDetector() blackHoleDetector {
	return blackHoleDetector{ackedSize: protocol.MinInitialPacketSize, ackedPN: protocol.InvalidPacketNumber}
}

func (d *blackHoleDetector) acked(pn protocol.PacketNumber, size protocol.ByteCount, mtuProbe bool) {
	switch {
	case mtuProbe:
		// A probe is larger than every packet before it, so no earlier burst is explained by the path MTU.
		d.n = 0
	case size < d.ackedSize:
		return
	case size == d.ackedSize:
		d.ackedPN = max(d.ackedPN, pn)
		return
	default:
		kept := 0
		for _, s := range d.suspicious[:d.n] {
			if s > size {
				d.suspicious[kept] = s
				kept++
			}
		}
		d.n = kept
	}
	d.ackedSize = size
	d.ackedPN = pn
}

func (d *blackHoleDetector) lost(pn protocol.PacketNumber, size protocol.ByteCount) {
	if d.burstMin == 0 || size < d.burstMin {
		d.burstMin = size
	}
	d.burstLast = max(d.burstLast, pn)
}

// detected closes the open burst and reports whether the losses show a black hole, then starts counting again.
func (d *blackHoleDetector) detected() bool {
	minSize, last := d.burstMin, d.burstLast
	d.burstMin, d.burstLast = 0, 0
	if minSize == 0 || minSize <= protocol.MinInitialPacketSize || (last < d.ackedPN && minSize <= d.ackedSize) {
		return false
	}
	if last > d.ackedPN {
		// Packets sent after the latest acknowledged large one were lost, so it no longer shows what the path carries.
		d.ackedSize = protocol.MinInitialPacketSize
	}
	d.suspicious[d.n] = minSize
	d.n++
	if d.n <= blackHoleThreshold {
		return false
	}
	d.n = 0
	return true
}
