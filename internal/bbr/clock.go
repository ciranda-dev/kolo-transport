package bbr

import "github.com/ciranda-dev/kolo-transport/internal/quicgo/monotime"

// A Clock returns the current time
type Clock interface {
	Now() monotime.Time
}

// DefaultClock implements the Clock interface using the Go stdlib clock.
type DefaultClock struct{}

var _ Clock = DefaultClock{}

// Now gets the current time
func (DefaultClock) Now() monotime.Time {
	return monotime.Now()
}
