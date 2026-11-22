package kolotransport

import (
	"fmt"
	"math"
	"net/netip"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

// DefaultMaxStreamReceiveWindow is the default MaxStreamReceiveWindow: 256 KiB.
const DefaultMaxStreamReceiveWindow = 256 << 10

// DefaultMaxConnReceiveWindow is the default MaxConnReceiveWindow: 512 KiB.
const DefaultMaxConnReceiveWindow = 512 << 10

// initialPacketSize is the UDP payload of the first packets of a connection, the 1200 bytes QUIC requires every path to
// carry (RFC 9000 section 14). quic-go's own 1280 makes an IP packet of 1308 bytes, which a path with an MTU of 1280,
// such as a tunnel or a mobile carrier's, drops. Path MTU discovery stays on and raises it where the path allows.
const initialPacketSize = 1200

// DefaultMaxMemory is the default MaxMemory: 1 GiB of receive window.
const DefaultMaxMemory = 1 << 30

// DefaultMaxGrownStreamReceiveWindow is the default MaxGrownStreamReceiveWindow: 4 MiB.
const DefaultMaxGrownStreamReceiveWindow = 4 << 20

// DefaultMaxGrownConnReceiveWindow is the default MaxGrownConnReceiveWindow: 8 MiB.
const DefaultMaxGrownConnReceiveWindow = 8 << 20

// DefaultMaxGrownMemory is the default MaxGrownMemory: 64 MiB.
const DefaultMaxGrownMemory = 64 << 20

const (
	congestionReno  = "reno"
	congestionCubic = "cubic"
	congestionBBR   = "bbr"
)

// defaultCongestionControl is the CongestionControl of a Config that leaves it empty. It is a variable only so the tests can run the suite with BBR.
var defaultCongestionControl = congestionReno

// Config sets the limits of a Listener or a dialed Conn. A zero field takes its default. Config is not comparable.
type Config struct {
	// MaxConns caps the connections of a Listener, handshaking or open. Listener only. Default 100, at least 1; MaxMemory is the upper bound.
	MaxConns int
	// MaxConnsPerIP caps them per source address, the one a connection started from. Listener only. Default 4 (or MaxConns if lower), at most MaxConns.
	MaxConnsPerIP int
	// MaxDialed caps the connections Listener.Dial has open, which MaxConns and MaxConnsPerIP do not count. Listener only. Default 32, at least 1; MaxMemory is the upper bound.
	MaxDialed int
	// MaxIncomingStreams caps the concurrent streams the peer may open, bidirectional and unidirectional each. Default 16, from 1 to 1000.
	MaxIncomingStreams int
	// HandshakeTimeout bounds the whole handshake and the life of a Retry token. Default 10s, from 1s to 60s.
	HandshakeTimeout time.Duration
	// IdleTimeout closes a connection with no network activity. Default 30s, from 1s to 10m.
	IdleTimeout time.Duration
	// KeepAlivePeriod sends a PING after this long without receiving, so connections stay open, also those never accepted.
	// Default 5s (or IdleTimeout/2 if lower), from 500ms to below IdleTimeout.
	KeepAlivePeriod time.Duration
	// MaxMemory must cover (MaxConns + MaxDialed) times MaxConnReceiveWindow (unread data, not heap), in bytes; the default leaves headroom and does not bound heap (see the README). Listener only. Default 1 GiB, from 64 MiB to 64 GiB.
	MaxMemory int64
	// AcceptFunc filters attempts by the source IP a connection starts from, never port or identity; false or a panic refuses.
	// Listener only. Nil allows all.
	AcceptFunc func(remote netip.AddrPort) bool
	// MaxStreamReceiveWindow caps the unread data buffered per stream, in bytes, on Listen and Dial. Advanced. Default 256 KiB (or MaxConnReceiveWindow if lower), from 64 KiB to 16 MiB.
	MaxStreamReceiveWindow int64
	// MaxConnReceiveWindow caps the unread data buffered per connection, in bytes; a node that dials N peers can hold N times it. Advanced. Default 512 KiB, from 64 KiB to 64 MiB, at least MaxStreamReceiveWindow.
	MaxConnReceiveWindow int64
	// WindowGrowth lets Conn.AllowWindowGrowth raise the receive windows of a connection the application trusts, up to the Grown values below; every other connection keeps its initial windows. Default false: no window ever grows.
	WindowGrowth bool
	// MaxGrownStreamReceiveWindow is the largest stream window a trusted connection can reach, in bytes. WindowGrowth only. Default 4 MiB (or MaxStreamReceiveWindow if higher), from MaxStreamReceiveWindow to 16 MiB.
	MaxGrownStreamReceiveWindow int64
	// MaxGrownConnReceiveWindow is the largest connection window a trusted connection can reach, in bytes. WindowGrowth only. Default 8 MiB (or MaxConnReceiveWindow if higher), from MaxConnReceiveWindow and MaxGrownStreamReceiveWindow to 64 MiB.
	MaxGrownConnReceiveWindow int64
	// MaxGrownMemory is the connection window growth all trusted connections of a Listener share, in bytes (a dialed Conn has its own); growth stops when it is used up and returns when a connection ends. WindowGrowth only. Default 64 MiB, from 1 MiB to 64 GiB.
	MaxGrownMemory int64
	// Network selects the UDP network of the socket Listen opens. Listener only. Default "udp4", the only accepted value: IPv6 is refused.
	Network string
	// CongestionControl picks the controller that paces what this end sends: "reno" (the default), "cubic" or "bbr". "bbr" keeps a link full where random loss makes the others crawl, but takes the bandwidth of other flows and holds a standing queue (see the guide).
	CongestionControl string
	// Datagrams offers QUIC datagrams (RFC 9221) on Listen and Dial; Conn.SendDatagram and Conn.ReceiveDatagram work only when both ends offer them. Default false.
	Datagrams bool
}

type limit[T int | int64 | time.Duration] struct {
	name          string
	def, min, max T
}

var (
	limitMemory    = limit[int64]{"MaxMemory", DefaultMaxMemory, 64 << 20, 64 << 30}
	limitStreams   = limit[int]{"MaxIncomingStreams", 16, 1, 1000}
	limitHandshake = limit[time.Duration]{"HandshakeTimeout", 10 * time.Second, time.Second, time.Minute}
	limitIdle      = limit[time.Duration]{"IdleTimeout", 30 * time.Second, time.Second, 10 * time.Minute}
	limitStreamWin = limit[int64]{"MaxStreamReceiveWindow", DefaultMaxStreamReceiveWindow, 64 << 10, 16 << 20}
	limitConnWin   = limit[int64]{"MaxConnReceiveWindow", DefaultMaxConnReceiveWindow, 64 << 10, 64 << 20}
	limitGrownMem  = limit[int64]{"MaxGrownMemory", DefaultMaxGrownMemory, 1 << 20, 64 << 30}
)

func (l limit[T]) apply(v *T) error {
	switch {
	case *v == 0:
		*v = l.def
	case *v < l.min || *v > l.max:
		return fmt.Errorf("%w: %s must be between %v and %v", ErrInvalidConfig, l.name, l.min, l.max)
	}
	return nil
}

func applyCount(name string, v *int, def int) error {
	switch {
	case *v == 0:
		*v = def
	case *v < 0:
		return fmt.Errorf("%w: %s must be at least 1", ErrInvalidConfig, name)
	}
	return nil
}

func (c *Config) withDefaults() (Config, error) {
	var out Config
	if c != nil {
		out = *c
	}
	errs := []error{
		limitStreams.apply(&out.MaxIncomingStreams),
		limitHandshake.apply(&out.HandshakeTimeout),
		limitIdle.apply(&out.IdleTimeout),
		limitConnWin.apply(&out.MaxConnReceiveWindow),
	}
	for _, err := range errs {
		if err != nil {
			return Config{}, err
		}
	}
	streamWin := limitStreamWin
	streamWin.def = min(streamWin.def, out.MaxConnReceiveWindow)
	if err := streamWin.apply(&out.MaxStreamReceiveWindow); err != nil {
		return Config{}, err
	}
	if out.MaxStreamReceiveWindow > out.MaxConnReceiveWindow {
		return Config{}, fmt.Errorf("%w: MaxStreamReceiveWindow %s is above MaxConnReceiveWindow %s",
			ErrInvalidConfig, size(out.MaxStreamReceiveWindow), size(out.MaxConnReceiveWindow))
	}
	if err := out.growthDefaults(); err != nil {
		return Config{}, err
	}
	switch out.CongestionControl {
	case "":
		out.CongestionControl = defaultCongestionControl
	case congestionReno, congestionCubic, congestionBBR:
	default:
		return Config{}, fmt.Errorf("%w: CongestionControl must be \"reno\", \"cubic\" or \"bbr\", not %q", ErrInvalidConfig, out.CongestionControl)
	}
	if out.KeepAlivePeriod == 0 {
		out.KeepAlivePeriod = min(5*time.Second, out.IdleTimeout/2)
	}
	if k := out.KeepAlivePeriod; k < 500*time.Millisecond || k >= out.IdleTimeout {
		return Config{}, fmt.Errorf("%w: KeepAlivePeriod must be at least 500ms and below IdleTimeout", ErrInvalidConfig)
	}
	return out, nil
}

func (c *Config) growthDefaults() error {
	if !c.WindowGrowth {
		if c.MaxGrownStreamReceiveWindow != 0 || c.MaxGrownConnReceiveWindow != 0 || c.MaxGrownMemory != 0 {
			return fmt.Errorf("%w: the MaxGrown options need WindowGrowth", ErrInvalidConfig)
		}
		return nil
	}
	stream := limit[int64]{"MaxGrownStreamReceiveWindow", max(DefaultMaxGrownStreamReceiveWindow, c.MaxStreamReceiveWindow), c.MaxStreamReceiveWindow, 16 << 20}
	if err := stream.apply(&c.MaxGrownStreamReceiveWindow); err != nil {
		return err
	}
	floor := max(c.MaxConnReceiveWindow, c.MaxGrownStreamReceiveWindow)
	conn := limit[int64]{"MaxGrownConnReceiveWindow", max(DefaultMaxGrownConnReceiveWindow, floor), floor, 64 << 20}
	if err := conn.apply(&c.MaxGrownConnReceiveWindow); err != nil {
		return err
	}
	return limitGrownMem.apply(&c.MaxGrownMemory)
}

func (c *Config) listenConfig() (Config, error) {
	out, err := c.withDefaults()
	if err != nil {
		return Config{}, err
	}
	if out.Network == "" {
		out.Network = "udp4"
	}
	if out.Network != "udp4" {
		return Config{}, fmt.Errorf("%w: Network must be \"udp4\", not %q; IPv6 is refused", ErrInvalidConfig, out.Network)
	}
	if err := applyCount("MaxConns", &out.MaxConns, 100); err != nil {
		return Config{}, err
	}
	if err := (limit[int]{"MaxConnsPerIP", min(4, out.MaxConns), 1, out.MaxConns}).apply(&out.MaxConnsPerIP); err != nil {
		return Config{}, err
	}
	if err := applyCount("MaxDialed", &out.MaxDialed, 32); err != nil {
		return Config{}, err
	}
	if err := limitMemory.apply(&out.MaxMemory); err != nil {
		return Config{}, err
	}
	// Even 1<<31 connections of 64 KiB need 128 TiB, above any MaxMemory, so clamping there keeps the products below from overflowing.
	conns := min(int64(out.MaxConns), math.MaxInt32) + min(int64(out.MaxDialed), math.MaxInt32)
	need := conns * out.MaxConnReceiveWindow
	var grown int64
	if out.WindowGrowth {
		grown = min(out.MaxGrownMemory, conns*(out.MaxGrownConnReceiveWindow-out.MaxConnReceiveWindow))
		need += grown
	}
	if need > out.MaxMemory {
		if out.WindowGrowth {
			return Config{}, fmt.Errorf("%w: (MaxConns %d + MaxDialed %d) × MaxConnReceiveWindow %s plus window growth of up to %s needs %s of receive window, above MaxMemory %s; raise MaxMemory or lower MaxConns, MaxDialed, MaxConnReceiveWindow or MaxGrownMemory",
				ErrInvalidConfig, out.MaxConns, out.MaxDialed, size(out.MaxConnReceiveWindow), size(grown), size(need), size(out.MaxMemory))
		}
		return Config{}, fmt.Errorf("%w: (MaxConns %d + MaxDialed %d) × MaxConnReceiveWindow %s needs %s of receive window, above MaxMemory %s; raise MaxMemory or lower MaxConns, MaxDialed or MaxConnReceiveWindow",
			ErrInvalidConfig, out.MaxConns, out.MaxDialed, size(out.MaxConnReceiveWindow), size(need), size(out.MaxMemory))
	}
	return out, nil
}

func size(n int64) string {
	switch {
	case n%(1<<20) == 0:
		return fmt.Sprintf("%d MiB", n>>20)
	case n%(1<<10) == 0:
		return fmt.Sprintf("%d KiB", n>>10)
	}
	return fmt.Sprintf("%d bytes", n)
}

func (c Config) quic() *quic.Config {
	maxStream, maxConn := c.MaxStreamReceiveWindow, c.MaxConnReceiveWindow
	if c.WindowGrowth {
		maxStream, maxConn = c.MaxGrownStreamReceiveWindow, c.MaxGrownConnReceiveWindow
	}
	return &quic.Config{
		Versions:                       []quic.Version{quic.Version1},
		HandshakeIdleTimeout:           c.HandshakeTimeout / 2,
		MaxIdleTimeout:                 c.IdleTimeout,
		KeepAlivePeriod:                c.KeepAlivePeriod,
		InitialStreamReceiveWindow:     uint64(c.MaxStreamReceiveWindow),
		MaxStreamReceiveWindow:         uint64(maxStream),
		InitialConnectionReceiveWindow: uint64(c.MaxConnReceiveWindow),
		MaxConnectionReceiveWindow:     uint64(maxConn),
		MaxIncomingStreams:             int64(c.MaxIncomingStreams),
		MaxIncomingUniStreams:          int64(c.MaxIncomingStreams),
		Allow0RTT:                      false,
		EnableDatagrams:                c.Datagrams,
		InitialPacketSize:              initialPacketSize,
	}
}
