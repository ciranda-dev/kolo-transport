package bbr

import (
	"math"
	"math/bits"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/quicgo/congestion"
)

const (
	infBandwidth = Bandwidth(math.MaxUint64)

	// maxRate and maxBytes cap the results of the rate arithmetic far above any real link or window (about 1 Ebit/s and
	// 1 PiB) so that a product that would overflow int64 saturates instead of wrapping to a small or negative number.
	maxRate  = Bandwidth(1 << 60)
	maxBytes = congestion.ByteCount(1 << 50)
)

// mulDiv returns a*b/c without overflow, or math.MaxUint64 if the quotient does not fit. c must not be zero.
func mulDiv(a, b, c uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	if hi >= c {
		return math.MaxUint64
	}
	q, _ := bits.Div64(hi, lo, c)
	return q
}

// bytesAt is rate*ns/perSecond, the bytes moved in ns nanoseconds by a rate in units per second, at most maxBytes.
func bytesAt(rate uint64, ns int64, perSecond uint64) congestion.ByteCount {
	if ns <= 0 {
		return 0
	}
	return congestion.ByteCount(min(mulDiv(rate, uint64(ns), perSecond), uint64(maxBytes)))
}

// Bandwidth of a connection
type Bandwidth uint64

const (
	// BitsPerSecond is 1 bit per second
	BitsPerSecond Bandwidth = 1
	// BytesPerSecond is 1 byte per second
	BytesPerSecond = 8 * BitsPerSecond
)

// BandwidthFromDelta calculates the bandwidth from a number of bytes and a time delta
func BandwidthFromDelta(bytes congestion.ByteCount, delta time.Duration) Bandwidth {
	if bytes <= 0 || delta <= 0 {
		return 0
	}
	return Bandwidth(min(mulDiv(uint64(bytes), uint64(time.Second), uint64(delta)), uint64(maxRate/BytesPerSecond))) * BytesPerSecond
}
