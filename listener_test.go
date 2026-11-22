package kolotransport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

// memConn is one end of an in-memory datagram pipe that reports *net.UDPAddr addresses.
type memConn struct {
	local    *net.UDPAddr
	peer     *memConn
	in       chan memPacket
	readErr  chan error
	writeErr error
	drop     atomic.Bool
	from     atomic.Pointer[net.UDPAddr]
	buffers  atomic.Int32
	done     chan struct{}
	once     sync.Once

	mu       sync.Mutex
	deadline time.Time
	kick     chan struct{}
}

type memPacket struct {
	data []byte
	from net.Addr
}

func newMemConn(ip net.IP, port int) *memConn {
	return &memConn{
		local:   &net.UDPAddr{IP: ip, Port: port},
		in:      make(chan memPacket, 256),
		readErr: make(chan error, 1),
		done:    make(chan struct{}),
		kick:    make(chan struct{}),
	}
}

func memPair() (*memConn, *memConn) {
	a, b := newMemConn(net.IPv4(10, 0, 0, 1), 1001), newMemConn(net.IPv4(10, 0, 0, 2), 1002)
	a.peer, b.peer = b, a
	return a, b
}

// inject queues a datagram for c as if it came from from.
func (c *memConn) inject(data []byte, from net.Addr) {
	c.in <- memPacket{data: data, from: from}
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

func (c *memConn) ReadFrom(p []byte) (int, net.Addr, error) {
	for {
		c.mu.Lock()
		deadline, kick := c.deadline, c.kick
		c.mu.Unlock()
		var timer <-chan time.Time
		if !deadline.IsZero() {
			d := time.Until(deadline)
			if d <= 0 {
				return 0, nil, timeoutError{}
			}
			timer = time.After(d)
		}
		select {
		case pk := <-c.in:
			return copy(p, pk.data), pk.from, nil
		case err := <-c.readErr:
			return 0, nil, err
		case <-c.done:
			return 0, nil, net.ErrClosed
		case <-kick:
		case <-timer:
		}
	}
}

func (c *memConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	if c.drop.Load() {
		return len(p), nil
	}
	from := c.local
	if a := c.from.Load(); a != nil {
		from = a
	}
	select {
	case c.peer.in <- memPacket{data: bytes.Clone(p), from: from}:
	default:
	}
	return len(p), nil
}

func (c *memConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *memConn) closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

func (c *memConn) LocalAddr() net.Addr              { return c.local }
func (c *memConn) SetDeadline(t time.Time) error    { return c.SetReadDeadline(t) }
func (c *memConn) SetWriteDeadline(time.Time) error { return nil }
func (c *memConn) SetReadBuffer(int) error          { c.buffers.Add(1); return nil }
func (c *memConn) SetWriteBuffer(int) error         { c.buffers.Add(1); return nil }
func (c *memConn) addrPort() netip.AddrPort         { return unmap(c.local.AddrPort()) }
func (c *memConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.deadline = t
	close(c.kick)
	c.kick = make(chan struct{})
	c.mu.Unlock()
	return nil
}

// noDeadlineConn ignores read deadlines.
type noDeadlineConn struct{ *memConn }

func (noDeadlineConn) SetReadDeadline(time.Time) error { return nil }
func (noDeadlineConn) SetDeadline(time.Time) error     { return nil }

// noSyscallConn makes quic-go fail to set up the socket.
type noSyscallConn struct{ *memConn }

func (noSyscallConn) SyscallConn() (syscall.RawConn, error) {
	return nil, errors.New("no syscall conn")
}

func addrPort(t *testing.T, l *Listener) netip.AddrPort {
	t.Helper()
	return l.Addr().(*net.UDPAddr).AddrPort()
}

func listenerDial(t *testing.T, l *Listener, to netip.AddrPort) *Conn {
	t.Helper()
	c, err := l.Dial(testCtx(t), to)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func echoOnce(t *testing.T, client, server *Conn) {
	t.Helper()
	ctx := testCtx(t)
	cs, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	cs.Close()
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(ss); err != nil || string(got) != "ping" {
		t.Fatalf("server read %q, %v", got, err)
	}
	ss.Write([]byte("pong"))
	ss.Close()
	if got, err := io.ReadAll(cs); err != nil || string(got) != "pong" {
		t.Fatalf("client read %q, %v", got, err)
	}
}

func TestListenerDialSharesSocket(t *testing.T) {
	checkGoroutines(t)
	a, b := listen(t, nil), listen(t, nil)
	c := listenerDial(t, a, addrPort(t, b))
	s := accept(t, b)
	if c.LocalAddr().(*net.UDPAddr).Port != a.Addr().(*net.UDPAddr).Port {
		t.Fatalf("dialed from %v, listener is %v", c.LocalAddr(), a.Addr())
	}
	if s.RemoteAddr().String() != a.Addr().String() {
		t.Fatalf("peer sees %v, listener is %v", s.RemoteAddr(), a.Addr())
	}
	echoOnce(t, c, s)
	other := dial(t, b.Addr().String(), nil)
	accept(t, b)
	if other.LocalAddr().(*net.UDPAddr).Port == a.Addr().(*net.UDPAddr).Port {
		t.Fatal("package Dial reused the listener's port")
	}
}

func TestListenerCloseClosesDialed(t *testing.T) {
	checkGoroutines(t)
	a, b := listen(t, nil), listen(t, nil)
	c := listenerDial(t, a, addrPort(t, b))
	s := accept(t, b)
	a.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.AcceptStream(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("dialed conn: %v", err)
	}
	if _, err := s.AcceptStream(ctx); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("peer: %v", err)
	}
	if _, err := a.Dial(ctx, addrPort(t, b)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Dial after Close: %v", err)
	}
}

func TestListenerMaxConns(t *testing.T) {
	checkGoroutines(t)
	if n := listen(t, nil).MaxConns(); n != 100 {
		t.Fatalf("default: %d", n)
	}
	if n := listen(t, &Config{MaxConns: 7}).MaxConns(); n != 7 {
		t.Fatalf("set: %d", n)
	}
}

func TestListenerDialLimit(t *testing.T) {
	checkGoroutines(t)
	a, b := listen(t, &Config{MaxDialed: 1}), listen(t, nil)
	c := listenerDial(t, a, addrPort(t, b))
	accept(t, b)
	if _, err := a.Dial(testCtx(t), addrPort(t, b)); !errors.Is(err, ErrMaxDialed) {
		t.Fatalf("second dial: %v", err)
	}
	if total, _ := a.counts(); total != 0 {
		t.Fatalf("dialed conn counted as incoming: %d", total)
	}
	c.Close()
	waitFor(t, "dial slot released", func() bool {
		c, err := a.Dial(testCtx(t), addrPort(t, b))
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
	if _, err := a.Dial(testCtx(t), netip.AddrPort{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("invalid address: %v", err)
	}
}

func TestWritePacketRejects(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	to := netip.MustParseAddrPort("127.0.0.1:9")
	for name, b := range map[string][]byte{
		"empty":     {},
		"oversize":  make([]byte, MaxPacketSize+1),
		"QUIC 0x40": {0x40, 1},
		"QUIC 0x80": {0x80, 1},
		"QUIC 0xc0": {0xc0, 1},
	} {
		if err := l.WritePacket(b, to); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := l.WritePacket([]byte{1}, netip.AddrPort{}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("invalid address: %v", err)
	}
	if err := l.WritePacket(make([]byte, MaxPacketSize), to); err != nil {
		t.Errorf("full size: %v", err)
	}
	l.Close()
	if err := l.WritePacket([]byte{1}, to); !errors.Is(err, ErrClosed) {
		t.Errorf("after Close: %v", err)
	}
}

func readPacket(t *testing.T, l *Listener) ([]byte, netip.AddrPort) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testCtx(t), 5*time.Second)
	defer cancel()
	b := make([]byte, MaxPacketSize)
	n, from, err := l.ReadPacket(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	return b[:n], from
}

func TestPacketRoundTrip(t *testing.T) {
	checkGoroutines(t)
	a, b := listen(t, nil), listen(t, nil)
	c := listenerDial(t, a, addrPort(t, b))
	s := accept(t, b)
	if err := a.WritePacket([]byte("\x01hello"), addrPort(t, b)); err != nil {
		t.Fatal(err)
	}
	// The packet must be queued even though it arrives before the first ReadPacket.
	time.Sleep(200 * time.Millisecond)
	if got, from := readPacket(t, b); string(got) != "\x01hello" || from != addrPort(t, a) {
		t.Fatalf("b got %q from %v", got, from)
	}
	mapped := netip.AddrPortFrom(netip.AddrFrom16(addrPort(t, a).Addr().As16()), addrPort(t, a).Port())
	if err := b.WritePacket([]byte("\x02back"), mapped); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("mapped destination: %v", err)
	}
	if err := b.WritePacket([]byte("\x02back"), addrPort(t, a)); err != nil {
		t.Fatal(err)
	}
	got, from := readPacket(t, a)
	if string(got) != "\x02back" || from != addrPort(t, b) || !from.Addr().Is4() {
		t.Fatalf("a got %q from %v", got, from)
	}
	echoOnce(t, c, s)
}

func TestReadPacketDropsOversize(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	raw := udpSocket(t)
	for _, size := range []int{MaxPacketSize + 1, 1452, 2000} {
		if _, err := raw.WriteTo(make([]byte, size), l.Addr()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := raw.WriteTo([]byte{0x3f, 'o', 'k'}, l.Addr()); err != nil {
		t.Fatal(err)
	}
	got, from := readPacket(t, l)
	if string(got) != "\x3fok" || from.Port() != uint16(raw.LocalAddr().(*net.UDPAddr).Port) {
		t.Fatalf("got %q from %v", got, from)
	}
}

func TestReadPacketClosed(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	errs := make(chan error, 1)
	go func() {
		_, _, err := l.ReadPacket(context.Background(), make([]byte, MaxPacketSize))
		errs <- err
	}()
	waitFor(t, "ReadPacket to take the read slot", func() bool { return len(l.readSem) == 1 })
	l.Close()
	select {
	case err := <-errs:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked read: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReadPacket did not return after Close")
	}
	if _, _, err := l.ReadPacket(context.Background(), make([]byte, 1)); !errors.Is(err, ErrClosed) {
		t.Fatalf("read after Close: %v", err)
	}
}

func TestReadPacketContext(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	start := time.Now()
	if _, _, err := l.ReadPacket(ctx, make([]byte, 1)); err != context.Canceled {
		t.Fatalf("got %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("took %v", d)
	}
	if _, _, err := l.ReadPacket(ctx, make([]byte, 1)); err != context.Canceled {
		t.Fatalf("canceled context: %v", err)
	}
}

func TestListenConn(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	a, err := ListenConn(testCtx(t), pa, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := ListenConn(testCtx(t), pb, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if a.Addr().String() != pa.local.String() {
		t.Fatalf("Addr %v", a.Addr())
	}
	c := listenerDial(t, b, pa.addrPort())
	s := accept(t, a)
	echoOnce(t, c, s)
	kc, err := c.ExportKey("kolo-test/listen-conn/v0")
	if err != nil {
		t.Fatal(err)
	}
	if ks, _ := s.ExportKey("kolo-test/listen-conn/v0"); ks != kc {
		t.Fatal("ends disagree")
	}
	a.Close()
	if !pa.closed() {
		t.Fatal("Close left pc open")
	}

	for name, run := range map[string]func(*memConn) error{
		"invalid config": func(pc *memConn) error {
			_, err := ListenConn(context.Background(), pc, &Config{MaxConns: -1})
			return err
		},
		"canceled context": func(pc *memConn) error {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := ListenConn(ctx, pc, nil)
			return err
		},
	} {
		pc, _ := memPair()
		if err := run(pc); err == nil || !pc.closed() {
			t.Errorf("%s: err %v, pc closed %v", name, err, pc.closed())
		}
	}
	if _, err := ListenConn(context.Background(), nil, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("nil pc: %v", err)
	}
}

func TestConnContext(t *testing.T) {
	checkGoroutines(t)
	done := func(c *Conn) bool {
		select {
		case <-c.Context().Done():
			return true
		default:
			return false
		}
	}
	wait := func(c *Conn) error {
		t.Helper()
		select {
		case <-c.Context().Done():
			return context.Cause(c.Context())
		case <-time.After(5 * time.Second):
			t.Fatal("context not canceled")
			return nil
		}
	}

	client, server := pair(t, nil, nil)
	if done(client) || done(server) || client.Context() != client.Context() {
		t.Fatal("context of an open conn is done or unstable")
	}
	client.CloseWithError(7, "peer-text")
	if err := wait(client); !errors.Is(err, ErrClosed) {
		t.Fatalf("local close cause: %v", err)
	}
	err := wait(server)
	wantCode(t, err, ErrPeerClosed, 7)
	if strings.Contains(err.Error(), "peer-text") {
		t.Fatalf("cause leaks peer text: %v", err)
	}

	idleClient, _, cut := deadPathPair(t, &Config{IdleTimeout: time.Second})
	cut()
	if err := wait(idleClient); !errors.Is(err, ErrTimeout) {
		t.Fatalf("idle cause: %v", err)
	}

	l := listen(t, nil)
	dial(t, l.Addr().String(), nil)
	accepted := accept(t, l)
	l.Close()
	if err := wait(accepted); !errors.Is(err, ErrClosed) {
		t.Fatalf("listener close cause: %v", err)
	}
	if !errors.Is(accepted.Context().Err(), context.Canceled) {
		t.Fatalf("Err %v", accepted.Context().Err())
	}
}

// deadPathPair connects two memConn listeners with cfg; cut then drops every packet both ways.
func deadPathPair(t *testing.T, cfg *Config) (client, server *Conn, cut func()) {
	t.Helper()
	pa, pb := memPair()
	a, b := memListener(t, pa, cfg), memListener(t, pb, cfg)
	client = listenerDial(t, b, pa.addrPort())
	server = accept(t, a)
	return client, server, func() {
		pa.drop.Store(true)
		pb.drop.Store(true)
	}
}

func memListener(t *testing.T, pc net.PacketConn, cfg *Config) *Listener {
	t.Helper()
	l, err := ListenConn(testCtx(t), pc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestReadPacketWaitsOnContext(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	blocked := make(chan error, 1)
	go func() {
		_, _, err := l.ReadPacket(context.Background(), make([]byte, 1))
		blocked <- err
	}()
	waitFor(t, "the first reader to hold the turn", func() bool { return len(l.readSem) == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() {
		_, _, err := l.ReadPacket(ctx, make([]byte, 1))
		second <- err
	}()
	select {
	case err := <-second:
		if err != context.DeadlineExceeded {
			t.Errorf("second reader: %v", err)
		}
	case <-time.After(time.Second):
		t.Error("second reader waited past its deadline")
		l.Close()
		<-second
	}
	l.Close()
	if err := <-blocked; !errors.Is(err, ErrClosed) {
		t.Fatalf("first reader: %v", err)
	}
}

func TestReadPacketKeepsDatagramOnTimeout(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	l := memListener(t, pa, nil)
	var backlog atomic.Int32
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 100 {
			for backlog.Load() >= 16 {
				select {
				case <-stop:
					return
				case <-time.After(100 * time.Microsecond):
				}
			}
			backlog.Add(1)
			pa.inject([]byte{0x01, byte(i)}, pb.local)
			time.Sleep(200 * time.Microsecond)
		}
	})
	defer wg.Wait()
	defer close(stop)
	seen := make(map[byte]bool)
	reads := 0
	b := make([]byte, MaxPacketSize)
	for deadline := time.Now().Add(10 * time.Second); reads < 100 && time.Now().Before(deadline); {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Microsecond)
		n, _, err := l.ReadPacket(ctx, b)
		cancel()
		if err == context.DeadlineExceeded {
			continue
		}
		if err != nil || n != 2 {
			t.Fatalf("read %d bytes: %v", n, err)
		}
		if seen[b[1]] {
			t.Fatalf("datagram %d read twice", b[1])
		}
		seen[b[1]] = true
		reads++
		backlog.Add(-1)
	}
	if reads != 100 {
		t.Fatalf("got %d of 100 datagrams", reads)
	}
}

func TestListenerCloseIgnoringDeadlines(t *testing.T) {
	checkGoroutines(t)
	pa, _ := memPair()
	l, err := ListenConn(testCtx(t), noDeadlineConn{pa}, nil)
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() {
		l.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		pa.Close()
		<-closed
		t.Fatal("Close hung on a PacketConn that ignores read deadlines")
	}
	if !pa.closed() {
		t.Fatal("pc left open")
	}
}

func TestDestinationsRejected(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	for _, s := range []string{
		"127.0.0.1:0", "0.0.0.0:9", "[::]:9", "224.0.0.1:9", "[ff02::1]:9",
		"255.255.255.255:9", "[::ffff:224.0.0.1]:9", "[::ffff:255.255.255.255]:9", "[::ffff:0.0.0.0]:9",
	} {
		ap := netip.MustParseAddrPort(s)
		if _, err := l.Dial(testCtx(t), ap); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Dial %s: %v", s, err)
		}
		if err := l.WritePacket([]byte{1}, ap); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("WritePacket %s: %v", s, err)
		}
	}
	if _, err := l.Dial(testCtx(t), netip.AddrPort{}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("Dial zero: %v", err)
	}
}

func TestReadPacketSkipsBadSenders(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	l := memListener(t, pa, nil)
	pa.inject([]byte{0x01, 'a'}, &net.UDPAddr{IP: []byte{1, 2, 3}, Port: 5})
	pa.inject([]byte{0x01, 'b'}, &net.UDPAddr{Port: 6})
	pa.inject([]byte{0x01, 'c'}, &net.IPAddr{IP: net.IPv4(10, 0, 0, 3)})
	pa.inject([]byte{0x01, 'd'}, pb.local)
	got, from := readPacket(t, l)
	if string(got) != "\x01d" || from != pb.addrPort() {
		t.Fatalf("got %q from %v", got, from)
	}
}

func TestReadPacketAfterSocketFailure(t *testing.T) {
	checkGoroutines(t)
	pa, _ := memPair()
	l := memListener(t, pa, nil)
	pa.readErr <- errors.New("socket failed")
	for i := range 2 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, err := l.ReadPacket(ctx, make([]byte, 1))
		cancel()
		if !errors.Is(err, ErrTransport) {
			t.Fatalf("read %d: %v", i, err)
		}
	}
}

func TestWritePacketHidesError(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	pa.writeErr = errors.New("secret-virtual-conn 10.9.9.9")
	l := memListener(t, pa, nil)
	err := l.WritePacket([]byte{1}, pb.addrPort())
	if !errors.Is(err, ErrTransport) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("got %v", err)
	}
}

func TestListenerDialFailureReleasesSlot(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, &Config{MaxDialed: 1, HandshakeTimeout: time.Second})
	silent := netip.MustParseAddrPort(silentAddr(t))
	if _, err := l.Dial(testCtx(t), silent); !errors.Is(err, ErrTimeout) {
		t.Fatalf("silent dial: %v", err)
	}
	b := listen(t, nil)
	listenerDial(t, l, addrPort(t, b))
	accept(t, b)
}

func TestListenConnClosesPCWhenListenFails(t *testing.T) {
	checkGoroutines(t)
	pa, _ := memPair()
	if _, err := ListenConn(testCtx(t), noSyscallConn{pa}, nil); err == nil {
		t.Fatal("ListenConn succeeded")
	}
	if !pa.closed() {
		t.Fatal("pc left open")
	}
}

func TestReadPacketDeliversEagerDatagram(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	l := memListener(t, pa, nil)
	for i := 0; l.pending == nil; i++ {
		if i == 50 {
			t.Fatal("the eager read never kept a datagram")
		}
		pa.inject([]byte{0x01, byte(i)}, pb.local)
		waitFor(t, "the transport to read the datagram", func() bool { return len(pa.in) == 0 })
		l.startPacketQueue()
	}
	want := l.pending[1]
	pa.inject([]byte{0x01, 0xff}, pb.local)
	waitFor(t, "the transport to read the datagram", func() bool { return len(pa.in) == 0 })
	if got, _ := readPacket(t, l); got[1] != want {
		t.Fatalf("first read %d, want the kept datagram %d", got[1], want)
	}
}

func TestReadPacketShortBuffer(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	l := memListener(t, pa, nil)
	pa.inject([]byte{0x01, 2, 3, 4, 5, 6, 7, 8, 9, 10}, pb.local)
	pa.inject([]byte{0x01, 'z'}, pb.local)
	ctx := testCtx(t)
	b := make([]byte, 4)
	n, _, err := l.ReadPacket(ctx, b)
	if err != nil || n != 4 || !bytes.Equal(b, []byte{0x01, 2, 3, 4}) {
		t.Fatalf("got %d %v, %v", n, b, err)
	}
	n, _, err = l.ReadPacket(ctx, b)
	if err != nil || n != 2 || !bytes.Equal(b[:2], []byte{0x01, 'z'}) {
		t.Fatalf("second read %d %v, %v", n, b[:n], err)
	}
}

func TestValidDest(t *testing.T) {
	for _, s := range []string{
		"0.0.0.1:9", "0.255.255.255:9", "240.0.0.1:9", "255.255.255.254:9", "255.255.255.255:9",
		"[::ffff:240.0.0.1]:9", "[::ffff:0.1.2.3]:9", "10.0.0.2:0", "[::]:9", "[ff02::1]:9",
		"[::1]:9", "[::1%1]:9", "[2001:db8::1]:9", "[fe80::1%1]:9", "[64:ff9b::7f00:1]:9",
		"[::ffff:127.0.0.1]:9", "[::ffff:1.0.0.1]:9",
	} {
		if validDest(netip.MustParseAddrPort(s)) {
			t.Errorf("%s accepted", s)
		}
	}
	for _, s := range []string{"1.0.0.1:9", "223.255.255.255:9", "127.0.0.1:9"} {
		if !validDest(netip.MustParseAddrPort(s)) {
			t.Errorf("%s rejected", s)
		}
	}
}

func TestReadPacketStaysFailed(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	l := memListener(t, pa, nil)
	for i := range 8 {
		pa.inject([]byte{0x01, byte(i)}, pb.local)
	}
	waitFor(t, "the transport to read the datagrams", func() bool { return len(pa.in) == 0 })
	pa.readErr <- errors.New("socket failed")
	waitFor(t, "the transport to read the error", func() bool { return len(pa.readErr) == 0 })
	failed := false
	for i := range 20 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, err := l.ReadPacket(ctx, make([]byte, MaxPacketSize))
		cancel()
		switch {
		case errors.Is(err, ErrTransport):
			failed = true
		case failed:
			t.Fatalf("call %d after ErrTransport: %v", i, err)
		case err != nil:
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if !failed {
		t.Fatal("never returned ErrTransport")
	}
}

func TestReadPacketDropsRefusedSenders(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	l := memListener(t, pa, nil)
	pa.inject([]byte{0x01, 'a'}, &net.UDPAddr{IP: net.IPv4(10, 0, 0, 2), Port: 0})
	pa.inject([]byte{0x01, 'b'}, &net.UDPAddr{IP: net.IPv4(240, 0, 0, 1), Port: 9})
	pa.inject([]byte{0x01, 'c'}, &net.UDPAddr{IP: net.IPv4(224, 0, 0, 1), Port: 9})
	pa.inject([]byte{0x01, '6'}, &net.UDPAddr{IP: net.IPv6loopback, Port: 9})
	pa.inject([]byte{0x01, 'd'}, pb.local)
	got, from := readPacket(t, l)
	if string(got) != "\x01d" || from != pb.addrPort() {
		t.Fatalf("got %q from %v", got, from)
	}
}

func TestListenAddr(t *testing.T) {
	c, err := (*Config)(nil).listenConfig()
	if err != nil {
		t.Fatal(err)
	}
	for _, addr := range []string{":0", "0.0.0.0:0", "127.0.0.1:0", "localhost:0", "node.example:4000"} {
		if network, err := listenAddr(c, addr); err != nil || network != "udp4" {
			t.Errorf("%s: %q, %v", addr, network, err)
		}
	}
	for _, addr := range []string{
		"[::]:0", "[::1]:0", "[::1%1]:0", "[::ffff:127.0.0.1]:0", "[::ffff:0.0.0.0]:0", "[fe80::1%1]:0", "[2001:db8::1]:0",
	} {
		if _, err := listenAddr(c, addr); !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "IPv6") {
			t.Errorf("%s: %v", addr, err)
		}
	}
}

func TestListenRefusesIPv6(t *testing.T) {
	checkGoroutines(t)
	for _, addr := range []string{"[::1]:0", "[::ffff:127.0.0.1]:0", "[::1%1]:0"} {
		l, err := Listen(testCtx(t), addr, nil)
		if err == nil {
			l.Close()
		}
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "IPv6") {
			t.Errorf("%s: %v", addr, err)
		}
	}
}

// TestListenIPv4 is a loopback smoke test; 127.0.0.1 binds AF_INET with "udp" too.
func TestListenIPv4(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	la := addrPort(t, l)
	if la.Addr().Unmap() != netip.MustParseAddr("127.0.0.1") {
		t.Fatalf("listening on %v", la)
	}
	v4, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer v4.Close()
	if _, err := v4.WriteTo([]byte("\x014"), net.UDPAddrFromAddrPort(la)); err != nil {
		t.Fatal(err)
	}
	if got, from := readPacket(t, l); string(got) != "\x014" || from != v4.LocalAddr().(*net.UDPAddr).AddrPort() {
		t.Fatalf("got %q from %v", got, from)
	}
}

func TestIPv6DestinationsRefused(t *testing.T) {
	checkGoroutines(t)
	pa, _ := memPair()
	l := listen(t, nil)
	for name, ln := range map[string]*Listener{"Listen": l, "ListenConn": memListener(t, pa, nil)} {
		for _, s := range []string{"[::1]:9", "[::1%1]:9", "[::ffff:127.0.0.1]:9"} {
			ap := netip.MustParseAddrPort(s)
			ctx, cancel := context.WithTimeout(testCtx(t), 2*time.Second)
			_, err := ln.Dial(ctx, ap)
			cancel()
			if !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("%s: Dial %s: %v", name, s, err)
			}
			if err := ln.WritePacket([]byte{1}, ap); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("%s: WritePacket %s: %v", name, s, err)
			}
		}
	}
	b := listen(t, nil)
	ap := addrPort(t, b)
	mapped := netip.AddrPortFrom(netip.AddrFrom16(ap.Addr().As16()), ap.Port())
	if err := l.WritePacket([]byte("\x01m"), mapped); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("WritePacket %v: %v", mapped, err)
	}
	if _, err := l.Dial(testCtx(t), mapped); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Dial %v: %v", mapped, err)
	}
	if err := l.WritePacket([]byte("\x014"), ap); err != nil {
		t.Fatalf("WritePacket %v: %v", ap, err)
	}
	if got, from := readPacket(t, b); string(got) != "\x014" || from != addrPort(t, l) {
		t.Fatalf("got %q from %v", got, from)
	}
	listenerDial(t, l, ap)
	if c := accept(t, b); c.RemoteAddr().String() != l.Addr().String() {
		t.Fatalf("peer sees %v, listener is %v", c.RemoteAddr(), l.Addr())
	}
}

func TestListenIPv6OnlyName(t *testing.T) {
	checkGoroutines(t)
	name := ipv6OnlyName(t)
	l, err := Listen(testCtx(t), name+":0", nil)
	if err == nil {
		l.Close()
		t.Fatalf("listening on %v", l.Addr())
	}
	if ae := (*net.AddrError)(nil); !errors.As(err, &ae) {
		t.Fatalf("not refused before binding: %v", err)
	}
}

func TestListenConnRefusesIPv6Sources(t *testing.T) {
	checkGoroutines(t)
	pa, pb := memPair()
	pb.from.Store(&net.UDPAddr{IP: net.IPv6loopback, Port: 1002})
	var called atomic.Bool
	a := memListener(t, pa, &Config{AcceptFunc: func(netip.AddrPort) bool {
		called.Store(true)
		return true
	}})
	b := memListener(t, pb, nil)
	ctx, cancel := context.WithTimeout(testCtx(t), 2*time.Second)
	defer cancel()
	if c, err := b.Dial(ctx, pa.addrPort()); err == nil {
		c.Close()
		t.Fatal("connected from an IPv6 source")
	}
	if called.Load() {
		t.Fatal("AcceptFunc saw an IPv6 source")
	}
	if total, _ := a.counts(); total != 0 {
		t.Fatalf("%d connections counted", total)
	}
	v6 := &net.UDPAddr{IP: net.IPv6loopback, Port: 1002}
	if _, err := a.connContext(context.Background(), &quic.ClientInfo{RemoteAddr: v6, AddrVerified: true}); err == nil || called.Load() {
		t.Fatal("connContext let an IPv6 source through")
	}
}

func TestListenConnRefusesIPv6Socket(t *testing.T) {
	checkGoroutines(t)
	for _, ip := range []net.IP{net.IPv6unspecified, net.ParseIP("2001:db8::1"), nil} {
		pc := newMemConn(ip, 1001)
		if l, err := ListenConn(testCtx(t), pc, nil); err == nil {
			l.Close()
			t.Errorf("%v: accepted", ip)
		} else if !errors.Is(err, ErrInvalidArgument) || !pc.closed() {
			t.Errorf("%v: %v, pc closed %v", ip, err, pc.closed())
		}
	}
	pc, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	if l, err := ListenConn(testCtx(t), pc, nil); err == nil {
		l.Close()
		t.Fatalf("accepted %v", pc.LocalAddr())
	} else if !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if pc.Close() == nil {
		t.Fatal("pc left open")
	}
}

func TestListenConnIgnoresRefusedMigration(t *testing.T) {
	checkGoroutines(t)
	for _, to := range []string{"[2001:db8::7]:4444", "224.0.0.1:4444"} {
		pa, pb := memPair()
		a, b := memListener(t, pa, nil), memListener(t, pb, nil)
		client := listenerDial(t, b, pa.addrPort())
		server := accept(t, a)
		pb.from.Store(net.UDPAddrFromAddrPort(netip.MustParseAddrPort(to)))
		s, err := client.OpenStream(testCtx(t))
		if err != nil {
			t.Fatal(err)
		}
		s.Write([]byte("moved"))
		for end := time.Now().Add(2 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
			if r := unmap(server.RemoteAddr().(*net.UDPAddr).AddrPort()); r != pb.addrPort() {
				t.Fatalf("server moved to %v", r)
			}
		}
	}
}

func TestListenConnKeepsBufferMethods(t *testing.T) {
	checkGoroutines(t)
	pa, _ := memPair()
	memListener(t, pa, nil)
	if pa.buffers.Load() != 2 {
		t.Fatalf("%d buffer calls reached the PacketConn", pa.buffers.Load())
	}
}
