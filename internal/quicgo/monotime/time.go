// Package monotime exports the monotonic clock type of the connection.
package monotime

import "github.com/ciranda-dev/kolo-transport/internal/quicgo/internal/monotime"

// A Time is an instant of the monotonic clock.
type Time = monotime.Time

// Now returns the current monotonic time.
func Now() Time { return monotime.Now() }
