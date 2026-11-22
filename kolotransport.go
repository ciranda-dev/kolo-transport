package kolotransport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
	"github.com/ciranda-dev/kolo-transport/internal/sockopt"
)

var (
	errListenerFull = errors.New("kolotransport: listener full")
	errUnverified   = errors.New("kolotransport: source address not validated")
	errNotAllowed   = errors.New("kolotransport: source address not allowed")
)

// MaxPacketSize is the largest non-QUIC datagram WritePacket sends and ReadPacket returns.
const MaxPacketSize = 1200

// maxDatagram is quic-go's read buffer size; longer datagrams arrive truncated to it.
const maxDatagram = 1452

// Listener accepts connections on one UDP socket and can dial from it.
type Listener struct {
	pc   net.PacketConn
	tr   *quic.Transport
	ln   *quic.Listener
	cfg  Config
	grow *growth
	max  int
	once sync.Once
	shut atomic.Bool

	mu        sync.Mutex
	conns     int
	dialed    int
	maxDialed int
	maxPerIP  int
	perIP     map[netip.Addr]int
	open      map[*quic.Conn]struct{}
	closed    bool
	accept    func(netip.AddrPort) bool

	done    chan struct{}
	readSem chan struct{}
	failed  bool
	readBuf []byte
	pending []byte
	from    net.Addr
}

// Listen opens a UDP socket on addr, such as "127.0.0.1:0", and accepts connections on it.
func Listen(ctx context.Context, addr string, cfg *Config) (*Listener, error) {
	c, err := cfg.listenConfig()
	if err != nil {
		return nil, err
	}
	network, err := listenAddr(c, addr)
	if err != nil {
		return nil, err
	}
	var lc net.ListenConfig
	pc, err := lc.ListenPacket(ctx, network, addr)
	if err != nil {
		return nil, fmt.Errorf("kolotransport: listen: %w", err)
	}
	if err := sockopt.NoBroadcast(pc); err != nil {
		pc.Close()
		return nil, fmt.Errorf("kolotransport: listen: %w", err)
	}
	return newListener(pc, c)
}

// listenAddr returns the network for addr; "udp4" alone would silently turn "[::]:0" into 0.0.0.0.
func listenAddr(c Config, addr string) (string, error) {
	if ipv6Host(addr) {
		return "", fmt.Errorf("%w: listen address %q is IPv6, which is refused", ErrInvalidArgument, addr)
	}
	return c.Network, nil
}

func ipv6Host(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && !ip.Is4()
}

// ListenConn is Listen on pc, which the Listener owns and closes, also when ListenConn fails (see the README).
// quic-go may set the socket's buffer sizes and DF bit.
func ListenConn(ctx context.Context, pc net.PacketConn, cfg *Config) (*Listener, error) {
	if pc == nil {
		return nil, ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		pc.Close()
		return nil, err
	}
	c, err := cfg.listenConfig()
	if err != nil {
		pc.Close()
		return nil, err
	}
	if ua, ok := pc.LocalAddr().(*net.UDPAddr); !ok || !ua.AddrPort().Addr().Unmap().Is4() {
		pc.Close()
		return nil, fmt.Errorf("%w: PacketConn address %v is not IPv4", ErrInvalidArgument, pc.LocalAddr())
	}
	return newListener(ipv4Only(pc), c)
}

// ipv4Only drops senders that validDest refuses, since quic-go would move a connection to them; an IPv4 *net.UDPConn never receives IPv6.
func ipv4Only(pc net.PacketConn) net.PacketConn {
	if _, ok := pc.(*net.UDPConn); ok {
		return pc
	}
	c := ipv4Conn{pc}
	if sc, ok := pc.(syscall.Conn); ok {
		return ipv4SyscallConn{c, sc}
	}
	return c
}

type ipv4Conn struct{ net.PacketConn }

// ipv4SyscallConn keeps SyscallConn, which quic-go uses to set the Don't-Fragment bit.
type ipv4SyscallConn struct {
	ipv4Conn
	syscall.Conn
}

func (c ipv4Conn) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, from, err := c.PacketConn.ReadFrom(b)
		if ua, ok := from.(*net.UDPAddr); err != nil || ok && validDest(unmap(ua.AddrPort())) {
			return n, from, err
		}
	}
}

func (c ipv4Conn) SetReadBuffer(n int) error {
	if b, ok := c.PacketConn.(interface{ SetReadBuffer(int) error }); ok {
		return b.SetReadBuffer(n)
	}
	return errors.New("kolotransport: PacketConn has no SetReadBuffer")
}

func (c ipv4Conn) SetWriteBuffer(n int) error {
	if b, ok := c.PacketConn.(interface{ SetWriteBuffer(int) error }); ok {
		return b.SetWriteBuffer(n)
	}
	return errors.New("kolotransport: PacketConn has no SetWriteBuffer")
}

func newListener(pc net.PacketConn, c Config) (*Listener, error) {
	cert, err := newCertificate()
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("kolotransport: certificate: %w", err)
	}
	l := &Listener{
		pc:        pc,
		cfg:       c,
		grow:      newGrowth(c),
		max:       c.MaxConns,
		maxDialed: c.MaxDialed,
		maxPerIP:  c.MaxConnsPerIP,
		accept:    c.AcceptFunc,
		perIP:     make(map[netip.Addr]int),
		open:      make(map[*quic.Conn]struct{}),
		done:      make(chan struct{}),
		readSem:   make(chan struct{}, 1),
		readBuf:   make([]byte, maxDatagram),
	}
	l.tr = &quic.Transport{
		Conn:                pc,
		VerifySourceAddress: func(net.Addr) bool { return true },
		// The token's age is wall-clock time, so it can be zero (a Windows clock ticks every 0.5 to 15.6 ms) or
		// negative (the clock stepped back). The smallest duration expires it at any age, so each new connection goes
		// through Retry. Zero would mean 24 hours.
		MaxTokenAge: time.Duration(math.MinInt64),
		ConnContext: l.connContext,
	}
	l.ln, err = l.tr.Listen(serverTLS(cert), l.quicConfig())
	if err != nil {
		l.tr.Close()
		pc.Close()
		return nil, fmt.Errorf("kolotransport: listen: %w", err)
	}
	l.startPacketQueue()
	return l, nil
}

// startPacketQueue makes quic-go queue non-QUIC packets from now on, not only from the first ReadPacket.
func (l *Listener) startPacketQueue() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if n, from, err := l.tr.ReadNonQUICPacket(ctx, l.readBuf); err == nil {
		l.pending, l.from = bytes.Clone(l.readBuf[:n]), from
	}
}

func (l *Listener) connContext(ctx context.Context, info *quic.ClientInfo) (context.Context, error) {
	ua, ok := info.RemoteAddr.(*net.UDPAddr)
	if !info.AddrVerified || !ok {
		return nil, errUnverified
	}
	ap := unmap(ua.AddrPort())
	if !ap.Addr().Is4() {
		return nil, errNotAllowed
	}
	// Retry proves only that the IP received a packet recently; the port is the sender's choice.
	if l.accept != nil && !allowed(l.accept, ap) {
		return nil, errNotAllowed
	}
	ip := ap.Addr()
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conns >= l.max || l.perIP[ip] >= l.maxPerIP {
		return nil, errListenerFull
	}
	l.conns++
	l.perIP[ip]++
	context.AfterFunc(ctx, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.conns--
		if l.perIP[ip]--; l.perIP[ip] == 0 {
			delete(l.perIP, ip)
		}
	})
	return ctx, nil
}

func allowed(accept func(netip.AddrPort) bool, ap netip.AddrPort) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false
		}
	}()
	return accept(ap)
}

// Accept waits for the next connection whose handshake has completed.
// Up to 32 that are never accepted stay open until Close.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {
	qc, err := l.ln.Accept(ctx)
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		qc.CloseWithError(0, "")
		return nil, ErrClosed
	}
	l.open[qc] = struct{}{}
	l.mu.Unlock()
	useCongestionControl(qc, l.cfg)
	context.AfterFunc(qc.Context(), func() {
		l.mu.Lock()
		delete(l.open, qc)
		l.mu.Unlock()
	})
	return &Conn{qc: qc, grow: l.grow, datagrams: datagramsOn(qc)}, nil
}

// quicConfig is the listener's Config for quic-go, with the window growth hook when growth is on.
func (l *Listener) quicConfig() *quic.Config {
	q := l.cfg.quic()
	if l.grow != nil {
		q.AllowConnectionWindowIncrease = l.grow.allow
	}
	return q
}

// Dial connects to addr from the listener's socket, with the listener's Config; MaxDialed bounds these connections.
func (l *Listener) Dial(ctx context.Context, addr netip.AddrPort) (*Conn, error) {
	if !validDest(addr) {
		return nil, ErrInvalidArgument
	}
	l.mu.Lock()
	switch {
	case l.closed || l.shut.Load():
		l.mu.Unlock()
		return nil, ErrClosed
	case l.dialed >= l.maxDialed:
		l.mu.Unlock()
		return nil, ErrMaxDialed
	}
	l.dialed++
	l.mu.Unlock()
	release := func() {
		l.mu.Lock()
		l.dialed--
		l.mu.Unlock()
	}
	qc, err := l.tr.Dial(ctx, net.UDPAddrFromAddrPort(addr), clientTLS(), l.quicConfig())
	if err != nil {
		release()
		return nil, ctxErr(ctx, err)
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		qc.CloseWithError(0, "")
		release()
		return nil, ErrClosed
	}
	l.open[qc] = struct{}{}
	l.mu.Unlock()
	useCongestionControl(qc, l.cfg)
	context.AfterFunc(qc.Context(), func() {
		l.mu.Lock()
		delete(l.open, qc)
		l.dialed--
		l.mu.Unlock()
	})
	return &Conn{qc: qc, grow: l.grow, datagrams: datagramsOn(qc)}, nil
}

// WritePacket sends b as one non-QUIC datagram from the listener's socket, blocking as long as its WriteTo does; b[0]&0xC0 must be 0.
func (l *Listener) WritePacket(b []byte, to netip.AddrPort) error {
	if len(b) == 0 || len(b) > MaxPacketSize || b[0]&0xC0 != 0 || !validDest(to) {
		return ErrInvalidArgument
	}
	if l.shut.Load() {
		return ErrClosed
	}
	if _, err := l.tr.WriteTo(b, net.UDPAddrFromAddrPort(to)); err != nil {
		if l.shut.Load() {
			return ErrClosed
		}
		return ErrTransport
	}
	return nil
}

// ReadPacket waits for the next non-QUIC datagram of at most MaxPacketSize bytes; the sender may be spoofed (see the README).
// A b shorter than the datagram gets its first len(b) bytes and the rest is dropped without an error; the count is the bytes copied.
func (l *Listener) ReadPacket(ctx context.Context, b []byte) (int, netip.AddrPort, error) {
	select {
	case l.readSem <- struct{}{}:
	case <-ctx.Done():
		return 0, netip.AddrPort{}, ctx.Err()
	case <-l.done:
		return 0, netip.AddrPort{}, ErrClosed
	}
	defer func() { <-l.readSem }()
	for {
		if err := ctx.Err(); err != nil {
			return 0, netip.AddrPort{}, err
		}
		if l.shut.Load() {
			return 0, netip.AddrPort{}, ErrClosed
		}
		if l.failed {
			return 0, netip.AddrPort{}, ErrTransport
		}
		data, from := l.pending, l.from
		l.pending, l.from = nil, nil
		if data == nil {
			n, addr, err := l.tr.ReadNonQUICPacket(ctx, l.readBuf)
			if err != nil {
				switch {
				case ctx.Err() != nil:
					return 0, netip.AddrPort{}, ctx.Err()
				case l.shut.Load():
					return 0, netip.AddrPort{}, ErrClosed
				}
				l.failed = true
				return 0, netip.AddrPort{}, ErrTransport
			}
			data, from = l.readBuf[:n], addr
		}
		ua, ok := from.(*net.UDPAddr)
		if !ok || len(data) > MaxPacketSize {
			continue
		}
		ap := unmap(ua.AddrPort())
		if !validDest(ap) {
			continue
		}
		return copy(b, data), ap, nil
	}
}

var (
	thisNetwork = netip.MustParsePrefix("0.0.0.0/8")
	reserved    = netip.MustParsePrefix("240.0.0.0/4")
)

// validDest is the destination policy of every send path; 240.0.0.0/4 holds 255.255.255.255.
func validDest(ap netip.AddrPort) bool {
	a := ap.Addr()
	return ap.IsValid() && ap.Port() != 0 && a.Is4() && !a.IsUnspecified() && !a.IsMulticast() &&
		!thisNetwork.Contains(a) && !reserved.Contains(a)
}

func unmap(ap netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(ap.Addr().Unmap(), ap.Port())
}

// MaxConns returns the connection cap of the listener, with the default applied.
func (l *Listener) MaxConns() int { return l.max }

// Addr returns the local address of the listener.
func (l *Listener) Addr() net.Addr { return l.pc.LocalAddr() }

// Close refuses new connections, closes every connection of the listener with code 0, then the socket.
// It is safe to call more than once.
func (l *Listener) Close() error {
	var err error
	l.once.Do(func() {
		l.shut.Store(true)
		close(l.done)
		l.ln.Close()
		for {
			qc, acceptErr := l.ln.Accept(context.Background())
			if acceptErr != nil {
				break
			}
			qc.CloseWithError(0, "")
		}
		l.mu.Lock()
		l.closed = true
		open := slices.Collect(maps.Keys(l.open))
		l.mu.Unlock()
		for _, qc := range open {
			qc.CloseWithError(0, "")
		}
		err = l.closeTransport()
	})
	return err
}

// closeTransport closes pc itself if tr.Close is stuck on a PacketConn that ignores read deadlines.
func (l *Listener) closeTransport() error {
	stopped := make(chan struct{})
	go func() {
		l.tr.Close()
		close(stopped)
	}()
	select {
	case <-stopped:
		return l.pc.Close()
	case <-time.After(time.Second):
		err := l.pc.Close()
		<-stopped
		return err
	}
}

// Conn is a connection to one peer, carrying multiplexed streams.
type Conn struct {
	qc        *quic.Conn
	grow      *growth
	datagrams bool
	released  chan struct{}
	ctxOnce   sync.Once
	ctx       context.Context
}

// datagramsOn reports whether both ends offered datagrams; a Conn exists only after the handshake, so the peer's offer is known.
func datagramsOn(qc *quic.Conn) bool {
	d := qc.ConnectionState().SupportsDatagrams
	return d.Local && d.Remote
}

// Context is canceled when the connection ends; its context.Cause is the error the connection's calls return.
func (c *Conn) Context() context.Context {
	c.ctxOnce.Do(func() {
		ctx, cancel := context.WithCancelCause(context.Background())
		context.AfterFunc(c.qc.Context(), func() { cancel(mapErr(context.Cause(c.qc.Context()))) })
		c.ctx = ctx
	})
	return c.ctx
}

// Dial connects to the listener at an IPv4 addr from a new udp4 socket (on loopback for a loopback addr), released when the connection ends.
func Dial(ctx context.Context, addr string, cfg *Config) (*Conn, error) {
	c, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := dialAddr(addr); err != nil {
		return nil, err
	}
	raddr, err := resolve(ctx, addr)
	if err != nil {
		if e := ctx.Err(); e != nil {
			return nil, e
		}
		return nil, fmt.Errorf("kolotransport: resolve: %w", err)
	}
	if !validDest(raddr) {
		return nil, fmt.Errorf("%w: dial destination %v is refused", ErrInvalidArgument, raddr)
	}
	pc, err := dialConn(raddr)
	if err != nil {
		return nil, err
	}
	tr := &quic.Transport{Conn: pc}
	grow := newGrowth(c)
	q := c.quic()
	if grow != nil {
		q.AllowConnectionWindowIncrease = grow.allow
	}
	qc, err := tr.Dial(ctx, net.UDPAddrFromAddrPort(raddr), clientTLS(), q)
	if err != nil {
		tr.Close()
		pc.Close()
		return nil, ctxErr(ctx, err)
	}
	useCongestionControl(qc, c)
	conn := &Conn{qc: qc, grow: grow, datagrams: datagramsOn(qc), released: make(chan struct{})}
	context.AfterFunc(qc.Context(), func() {
		tr.Close()
		pc.Close()
		close(conn.released)
	})
	return conn, nil
}

// dialConn opens Dial's udp4 socket, on loopback for a loopback raddr, without SO_BROADCAST.
func dialConn(raddr netip.AddrPort) (*net.UDPConn, error) {
	var laddr *net.UDPAddr
	if raddr.Addr().IsLoopback() {
		laddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)}
	}
	pc, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return nil, fmt.Errorf("kolotransport: dial: %w", err)
	}
	if err := sockopt.NoBroadcast(pc); err != nil {
		pc.Close()
		return nil, fmt.Errorf("kolotransport: dial: %w", err)
	}
	return pc, nil
}

func dialAddr(addr string) error {
	if ipv6Host(addr) {
		return fmt.Errorf("%w: dial address %q is IPv6, which is refused", ErrInvalidArgument, addr)
	}
	return nil
}

func resolve(ctx context.Context, addr string) (netip.AddrPort, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return netip.AddrPort{}, err
	}
	var r net.Resolver
	p, err := r.LookupPort(ctx, "udp", port)
	if err != nil {
		return netip.AddrPort{}, err
	}
	ips, err := r.LookupNetIP(ctx, "ip4", host)
	if err != nil {
		return netip.AddrPort{}, err
	}
	if len(ips) == 0 {
		return netip.AddrPort{}, errors.New("no address")
	}
	return netip.AddrPortFrom(ips[0].Unmap(), uint16(p)), nil
}

// OpenStream opens a bidirectional stream, waiting while the peer's stream limit is reached.
// The peer sees the stream only once data or a Close is sent on it.
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error) {
	s, err := c.qc.OpenStreamSync(ctx)
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return &Stream{s: s}, nil
}

// AcceptStream waits for the next bidirectional stream opened by the peer.
func (c *Conn) AcceptStream(ctx context.Context) (*Stream, error) {
	s, err := c.qc.AcceptStream(ctx)
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return &Stream{s: s}, nil
}

// OpenUniStream opens a unidirectional stream, waiting while the peer's stream limit is reached.
func (c *Conn) OpenUniStream(ctx context.Context) (*SendStream, error) {
	s, err := c.qc.OpenUniStreamSync(ctx)
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return &SendStream{s: s}, nil
}

// AcceptUniStream waits for the next unidirectional stream opened by the peer.
func (c *Conn) AcceptUniStream(ctx context.Context) (*ReceiveStream, error) {
	s, err := c.qc.AcceptUniStream(ctx)
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return &ReceiveStream{s: s}, nil
}

// LocalAddr returns the local UDP address.
func (c *Conn) LocalAddr() net.Addr { return c.qc.LocalAddr() }

// RemoteAddr returns the peer's UDP address.
func (c *Conn) RemoteAddr() net.Addr { return c.qc.RemoteAddr() }

// AllowWindowGrowth marks the connection as trusted, so its receive windows can grow up to the MaxGrown values while MaxGrownMemory lasts.
// Call it once the peer is one the application authorized, such as after a handshake that proved its identity; a connection never marked keeps its initial windows.
// It reports whether growth is on for the connection: false when Config.WindowGrowth is off or the connection has ended. It is safe to call more than once.
func (c *Conn) AllowWindowGrowth() bool { return c.grow.trust(c.qc) }

// ExportKey derives 32 bytes from the TLS session (RFC 9846 exporter); both ends get the same bytes only without a man in the middle.
// Use a unique non-empty label such as "kolo-<lib>/<purpose>/v0", and sign label, role and key together, never the key alone.
func (c *Conn) ExportKey(label string) ([32]byte, error) {
	var key [32]byte
	if label == "" {
		return key, ErrInvalidArgument
	}
	st := c.qc.ConnectionState().TLS
	b, err := st.ExportKeyingMaterial(label, nil, len(key))
	if err != nil {
		return key, ErrTransport
	}
	copy(key[:], b)
	return key, nil
}

// SupportsDatagrams reports whether both ends set Config.Datagrams, which SendDatagram and ReceiveDatagram need.
func (c *Conn) SupportsDatagrams() bool { return c.datagrams }

// SendDatagram queues p as one QUIC datagram (RFC 9221), which may be lost, and never blocks; p can be reused once it returns.
// A p that does not fit in one packet on the current path is ErrDatagramTooLarge, and a full send queue drops p with ErrDatagramDropped.
func (c *Conn) SendDatagram(p []byte) error {
	if !c.datagrams {
		return ErrDatagramsUnsupported
	}
	queued, err := c.qc.TrySendDatagram(p)
	switch {
	case err != nil:
		return mapErr(err)
	case !queued:
		return ErrDatagramDropped
	}
	return nil
}

// ReceiveDatagram waits for the next datagram from the peer.
func (c *Conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if !c.datagrams {
		return nil, ErrDatagramsUnsupported
	}
	p, err := c.qc.ReceiveDatagram(ctx)
	if err != nil {
		return nil, ctxErr(ctx, err)
	}
	return p, nil
}

// Close closes the connection with code 0.
func (c *Conn) Close() error { return c.CloseWithError(0, "") }

// CloseWithError closes the connection, sending code and msg to the peer.
// An invalid code or msg (see ErrInvalidArgument) still closes the connection, with code 0 and no message.
func (c *Conn) CloseWithError(code uint64, msg string) error {
	var err error
	if code > MaxCode || len(msg) > maxCloseMessage {
		code, msg, err = 0, "", ErrInvalidArgument
	}
	c.qc.CloseWithError(quic.ApplicationErrorCode(code), msg)
	if c.released != nil {
		<-c.released
	}
	return err
}
