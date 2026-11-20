package kolotransport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

func TestStreams(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, nil, nil)
	ctx := testCtx(t)

	cs, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := cs.Close(); err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(ss)
	if err != nil || string(got) != "ping" {
		t.Fatalf("server read %q, %v", got, err)
	}
	if _, err := ss.Write([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	ss.Close()
	got, err = io.ReadAll(cs)
	if err != nil || string(got) != "pong" {
		t.Fatalf("client read %q, %v", got, err)
	}

	us, err := server.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := us.Write([]byte("uni")); err != nil {
		t.Fatal(err)
	}
	us.Close()
	rs, err := client.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, err = io.ReadAll(rs)
	if err != nil || string(got) != "uni" {
		t.Fatalf("uni read %q, %v", got, err)
	}

	if !client.LocalAddr().(*net.UDPAddr).IP.IsLoopback() {
		t.Fatalf("dialed from %v, not loopback", client.LocalAddr())
	}
	if client.LocalAddr().String() != server.RemoteAddr().String() ||
		client.RemoteAddr().String() != server.LocalAddr().String() {
		t.Fatal("addresses do not match")
	}
}

func TestDeadlines(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, nil, nil)
	ctx := testCtx(t)
	cs, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cs.SetDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := cs.Read(make([]byte, 1)); err != os.ErrDeadlineExceeded {
		t.Fatalf("Stream.Read: %v", err)
	}
	cs.SetReadDeadline(time.Now().Add(-time.Second))
	if _, err := cs.Read(make([]byte, 1)); err != os.ErrDeadlineExceeded {
		t.Fatalf("Stream.Read: %v", err)
	}
	cs.SetWriteDeadline(time.Now().Add(-time.Second))
	if _, err := cs.Write([]byte("x")); err != os.ErrDeadlineExceeded {
		t.Fatalf("Stream.Write: %v", err)
	}

	us, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	us.SetWriteDeadline(time.Now().Add(-time.Second))
	if _, err := us.Write([]byte("x")); err != os.ErrDeadlineExceeded {
		t.Fatalf("SendStream.Write: %v", err)
	}
	us.SetWriteDeadline(time.Time{})
	if _, err := us.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	rs, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rs.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if _, err := io.ReadAll(rs); err != os.ErrDeadlineExceeded {
		t.Fatalf("ReceiveStream.Read: %v", err)
	}
}

func TestMaxIncomingStreams(t *testing.T) {
	checkGoroutines(t)
	client, _ := pair(t, &Config{MaxIncomingStreams: 2}, nil)
	ctx := testCtx(t)
	for range 2 {
		if _, err := client.OpenStream(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := client.OpenUniStream(ctx); err != nil {
			t.Fatal(err)
		}
	}
	short, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := client.OpenStream(short); err != context.DeadlineExceeded {
		t.Fatalf("third stream: %v", err)
	}
	short, cancel = context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := client.OpenUniStream(short); err != context.DeadlineExceeded {
		t.Fatalf("third uni stream: %v", err)
	}
}

func TestStreamLimitFreesOnCompletion(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, &Config{MaxIncomingStreams: 1}, nil)
	ctx := testCtx(t)
	for range 3 {
		cs, err := client.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cs.Close()
		ss, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(ss); err != nil {
			t.Fatal(err)
		}
		ss.Close()
		if _, err := io.ReadAll(cs); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMaxConns(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, &Config{MaxConns: 2})
	addr := l.Addr().String()
	first := dial(t, addr, nil)
	dial(t, addr, nil)
	if _, err := Dial(testCtx(t), addr, nil); !errors.Is(err, ErrRefused) {
		t.Fatalf("third conn: %v", err)
	}
	first.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c, err := Dial(testCtx(t), addr, nil)
		if err == nil {
			c.Close()
			return
		}
		if !errors.Is(err, ErrRefused) || time.Now().After(deadline) {
			t.Fatalf("after a close: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMaxConnsPerIP(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, &Config{MaxConns: 3, MaxConnsPerIP: 2})
	addr := l.Addr().String()
	g := newGate()
	a := stalledDial(t, addr, g)
	b := stalledDial(t, addr, g)
	waitFor(t, "two stalled handshakes", func() bool { total, _ := l.counts(); return total == 2 })
	if _, err := Dial(testCtx(t), addr, nil); !errors.Is(err, ErrRefused) {
		t.Fatalf("third conn from one IP: %v", err)
	}
	if total, perIP := l.counts(); total != 2 || perIP != 2 {
		t.Fatalf("counted %d in total and %d for the IP", total, perIP)
	}
	g.open()
	<-a
	<-b
	waitFor(t, "slots released", func() bool { total, perIP := l.counts(); return total == 0 && perIP == 0 })
	if n := l.perIPEntries(); n != 0 {
		t.Fatalf("%d per-IP entries left", n)
	}
	dial(t, addr, nil)
}

func TestFailedHandshakesReleaseSlots(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, &Config{MaxConns: 1, HandshakeTimeout: time.Second})
	addr := l.Addr().String()
	conf := clientTLS()
	conf.NextProtos = []string{"other/0"}
	if c, err := rawTransport(t).Dial(testCtx(t), udpAddr(t, addr), conf, nil); err == nil {
		c.CloseWithError(0, "")
		t.Fatal("listener accepted another ALPN")
	}
	waitFor(t, "slot after an ALPN failure", func() bool { total, _ := l.counts(); return total == 0 })

	stalledDial(t, addr, newGate())
	waitFor(t, "stalled handshake counted", func() bool { total, _ := l.counts(); return total == 1 })
	waitFor(t, "slot after a handshake timeout", func() bool { total, _ := l.counts(); return total == 0 })
	if n := l.perIPEntries(); n != 0 {
		t.Fatalf("%d per-IP entries left", n)
	}
	dial(t, addr, nil)
}

func TestListenerRefusesUnvalidatedAddress(t *testing.T) {
	l := &Listener{max: 10, maxPerIP: 10, perIP: make(map[netip.Addr]int)}
	for name, info := range map[string]*quic.ClientInfo{
		"unvalidated": {RemoteAddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}},
		"not UDP":     {RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}, AddrVerified: true},
		"no address":  {AddrVerified: true},
	} {
		if _, err := l.connContext(context.Background(), info); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if l.conns != 0 || len(l.perIP) != 0 {
		t.Fatalf("counted %d, %d per-IP entries", l.conns, len(l.perIP))
	}
}

func TestDialIgnoresListenerLimits(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	dial(t, l.Addr().String(), &Config{
		MaxConns:      10001,
		MaxConnsPerIP: -1,
		MaxMemory:     1,
		AcceptFunc:    func(netip.AddrPort) bool { panic("called on Dial") },
	})
}

func TestAcceptFuncRejectedNeverTakesSlot(t *testing.T) {
	checkGoroutines(t)
	var allow atomic.Bool
	var calls atomic.Int32
	l := listen(t, &Config{MaxConns: 1, AcceptFunc: func(netip.AddrPort) bool {
		calls.Add(1)
		return allow.Load()
	}})
	addr := l.Addr().String()
	for range 3 {
		if _, err := Dial(testCtx(t), addr, nil); !errors.Is(err, ErrRefused) {
			t.Fatalf("rejected source: %v", err)
		}
		if total, _ := l.counts(); total != 0 || l.perIPEntries() != 0 {
			t.Fatalf("rejected attempt counted: %d", total)
		}
	}
	if n := calls.Load(); n < 3 {
		t.Fatalf("hook called %d times for 3 attempts", n)
	}
	allow.Store(true)
	dial(t, addr, nil)
}

func TestAcceptFuncPanicRefuses(t *testing.T) {
	checkGoroutines(t)
	var panics atomic.Bool
	panics.Store(true)
	l := listen(t, &Config{MaxConns: 1, AcceptFunc: func(netip.AddrPort) bool {
		if panics.Load() {
			panic("hook failure")
		}
		return true
	}})
	addr := l.Addr().String()
	_, err := Dial(testCtx(t), addr, nil)
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("got %v, want ErrRefused", err)
	}
	if strings.Contains(err.Error(), "hook failure") {
		t.Fatalf("%v leaks the panic value", err)
	}
	if total, _ := l.counts(); total != 0 {
		t.Fatalf("counted %d", total)
	}
	panics.Store(false)
	dial(t, addr, nil)
	accept(t, l)
}

func TestAcceptFuncRunsBeforeCounters(t *testing.T) {
	checkGoroutines(t)
	var calls atomic.Int32
	l := listen(t, &Config{MaxConns: 1, AcceptFunc: func(netip.AddrPort) bool {
		calls.Add(1)
		return true
	}})
	addr := l.Addr().String()
	stalledDial(t, addr, newGate())
	waitFor(t, "stalled handshake counted", func() bool { total, _ := l.counts(); return total == 1 })
	before := calls.Load()
	if _, err := Dial(testCtx(t), addr, nil); !errors.Is(err, ErrRefused) {
		t.Fatalf("got %v, want ErrRefused", err)
	}
	if calls.Load() == before {
		t.Fatal("hook not called for an attempt refused by MaxConns")
	}
}

func udpSocket(t *testing.T) net.PacketConn {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc
}

// TestAcceptFuncPortNotValidated relays the attempt from one port before Retry and another after it.
func TestAcceptFuncPortNotValidated(t *testing.T) {
	checkGoroutines(t)
	ports := make(chan uint16, 8)
	l := listen(t, &Config{AcceptFunc: func(ap netip.AddrPort) bool {
		select {
		case ports <- ap.Port():
		default:
		}
		return true
	}})
	front, before, after := udpSocket(t), udpSocket(t), udpSocket(t)
	var retried atomic.Bool
	var client atomic.Value
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := front.ReadFrom(buf)
			if err != nil {
				return
			}
			client.Store(from)
			out := before
			if retried.Load() {
				out = after
			}
			out.WriteTo(buf[:n], l.Addr())
		}
	}()
	for _, pc := range []net.PacketConn{before, after} {
		go func() {
			buf := make([]byte, 2048)
			for {
				n, _, err := pc.ReadFrom(buf)
				if err != nil {
					return
				}
				if n >= 5 && buf[0]&0xf0 == 0xf0 && bytes.Equal(buf[1:5], []byte{0, 0, 0, 1}) {
					retried.Store(true)
				}
				if to, ok := client.Load().(net.Addr); ok {
					front.WriteTo(buf[:n], to)
				}
			}
		}()
	}
	c := dial(t, front.LocalAddr().String(), nil)
	accept(t, l)
	want := uint16(after.LocalAddr().(*net.UDPAddr).Port)
	select {
	case got := <-ports:
		if got != want || !retried.Load() {
			t.Fatalf("hook saw port %d, want %d from the second socket (Retry seen: %v)", got, want, retried.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("hook not called")
	}
	if _, err := c.ExportKey("kolo-test/relay/v0"); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptFuncBlocksClose(t *testing.T) {
	checkGoroutines(t)
	entered := make(chan struct{}, 1)
	g := newGate()
	l, err := Listen(testCtx(t), "127.0.0.1:0", &Config{AcceptFunc: func(netip.AddrPort) bool {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-g.ch
		return false
	}})
	if err != nil {
		t.Fatal(err)
	}
	dialCtx, cancelDial := context.WithCancel(context.Background())
	dialed := make(chan struct{})
	go func() {
		defer close(dialed)
		Dial(dialCtx, l.Addr().String(), nil)
	}()
	closed := make(chan struct{})
	var closeOnce sync.Once
	startClose := func() {
		closeOnce.Do(func() {
			go func() {
				l.Close()
				close(closed)
			}()
		})
	}
	t.Cleanup(func() {
		g.open()
		startClose()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Error("Close did not return after the hook was released")
		}
		cancelDial()
		<-dialed
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("hook not called")
	}
	startClose()
	select {
	case <-closed:
		t.Fatal("Close returned while the hook was running")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestAcceptFuncNilAllowsAll(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, &Config{AcceptFunc: nil})
	dial(t, l.Addr().String(), nil)
	accept(t, l)
}

func TestAcceptFuncSeesUnmappedAddr(t *testing.T) {
	checkGoroutines(t)
	seen := make(chan netip.AddrPort, 8)
	l := listen(t, &Config{AcceptFunc: func(ap netip.AddrPort) bool {
		seen <- ap
		return true
	}})
	c := dial(t, l.Addr().String(), nil)
	var got netip.AddrPort
	select {
	case got = <-seen:
	case <-time.After(5 * time.Second):
		t.Fatal("hook not called")
	}
	if got.Addr() != netip.MustParseAddr("127.0.0.1") || got.Port() == 0 ||
		got.Port() != uint16(c.LocalAddr().(*net.UDPAddr).Port) {
		t.Fatalf("hook saw %v, dialer is %v", got, c.LocalAddr())
	}

	var called netip.AddrPort
	unit := &Listener{max: 10, maxPerIP: 10, perIP: make(map[netip.Addr]int), accept: func(ap netip.AddrPort) bool {
		called = ap
		return false
	}}
	mapped := &net.UDPAddr{IP: net.ParseIP("::ffff:127.0.0.1"), Port: 4242}
	if _, err := unit.connContext(context.Background(), &quic.ClientInfo{RemoteAddr: mapped}); err == nil || called.IsValid() {
		t.Fatalf("unvalidated address reached the hook: %v", called)
	}
	if _, err := unit.connContext(context.Background(), &quic.ClientInfo{RemoteAddr: mapped, AddrVerified: true}); err == nil {
		t.Fatal("rejected address accepted")
	}
	if called != netip.MustParseAddrPort("127.0.0.1:4242") {
		t.Fatalf("hook saw %v", called)
	}
}

func TestListenWithLargerMaxMemory(t *testing.T) {
	checkGoroutines(t)
	big := &Config{MaxConns: 100, MaxConnReceiveWindow: 16 << 20}
	if l, err := Listen(testCtx(t), "127.0.0.1:0", big); !errors.Is(err, ErrInvalidConfig) {
		if err == nil {
			l.Close()
		}
		t.Fatalf("default MaxMemory: %v", err)
	}
	big.MaxMemory = 3 << 30
	l := listen(t, big)
	dial(t, l.Addr().String(), nil)
	accept(t, l)
}

type deadlineWriter interface {
	io.Writer
	SetWriteDeadline(time.Time) error
}

type deadlineReader interface {
	io.Reader
	SetReadDeadline(time.Time) error
}

func TestReceiveWindowBoundsBuffering(t *testing.T) {
	checkGoroutines(t)
	small := &Config{MaxStreamReceiveWindow: 64 << 10, MaxConnReceiveWindow: 128 << 10}
	for _, tc := range []struct {
		name           string
		listen, dialer *Config
		dialerReads    bool
		uni            bool
	}{
		{"defaults", nil, nil, false, false},
		{"custom", small, nil, false, false},
		{"dialer windows", nil, small, true, false},
		{"unidirectional", small, nil, false, true},
		{"unidirectional dialer windows", nil, small, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			readerCfg := tc.listen
			if tc.dialerReads {
				readerCfg = tc.dialer
			}
			want, err := readerCfg.withDefaults()
			if err != nil {
				t.Fatal(err)
			}
			streamWin, connWin := int(want.MaxStreamReceiveWindow), int(want.MaxConnReceiveWindow)
			client, server := pair(t, tc.listen, tc.dialer)
			writer, reader := client, server
			if tc.dialerReads {
				writer, reader = server, client
			}
			ctx := testCtx(t)
			var sent []int
			for range 3 {
				var w deadlineWriter
				if tc.uni {
					w, err = writer.OpenUniStream(ctx)
				} else {
					w, err = writer.OpenStream(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
				w.SetWriteDeadline(time.Now().Add(500 * time.Millisecond))
				n, err := w.Write(make([]byte, 2*streamWin))
				if err != os.ErrDeadlineExceeded {
					t.Fatalf("write: %v", err)
				}
				if n > streamWin {
					t.Fatalf("peer accepted %d bytes over a %d byte stream window", n, streamWin)
				}
				sent = append(sent, n)
			}
			total := sent[0] + sent[1] + sent[2]
			if total > connWin || total == 0 {
				t.Fatalf("peer accepted %d bytes for a %d byte connection window", total, connWin)
			}
			for _, n := range sent {
				if n == 0 {
					continue
				}
				var r deadlineReader
				if tc.uni {
					r, err = reader.AcceptUniStream(ctx)
				} else {
					r, err = reader.AcceptStream(ctx)
				}
				if err != nil {
					t.Fatal(err)
				}
				r.SetReadDeadline(time.Now().Add(time.Second))
				got, _ := io.ReadAll(r)
				if len(got) != n {
					t.Fatalf("read %d of %d bytes", len(got), n)
				}
			}
		})
	}
}

func TestHandshakeTimeout(t *testing.T) {
	checkGoroutines(t)
	start := time.Now()
	_, err := Dial(testCtx(t), silentAddr(t), &Config{HandshakeTimeout: time.Second})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestIdleTimeout(t *testing.T) {
	checkGoroutines(t)
	client, server, cut := deadPathPair(t, &Config{IdleTimeout: time.Second})
	cut()
	start := time.Now()
	if _, err := server.AcceptStream(testCtx(t)); !errors.Is(err, ErrTimeout) {
		t.Fatalf("server: %v", err)
	}
	if _, err := client.AcceptUniStream(testCtx(t)); !errors.Is(err, ErrTimeout) {
		t.Fatalf("client: %v", err)
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("took %v", d)
	}
}

func TestKeepAlive(t *testing.T) {
	checkGoroutines(t)
	cfg := &Config{IdleTimeout: 2 * time.Second}
	client, server := pair(t, cfg, cfg)
	stillOpen(t, client, server, 6*time.Second)
}

// stillOpen waits for d without application traffic, then checks a stream round trip.
func stillOpen(t *testing.T, client, server *Conn, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(testCtx(t), d)
	defer cancel()
	errs := make(chan error, 1)
	go func() {
		_, err := server.AcceptStream(ctx)
		errs <- err
	}()
	if _, err := client.AcceptUniStream(ctx); err != context.DeadlineExceeded {
		t.Fatalf("client: %v", err)
	}
	if err := <-errs; err != context.DeadlineExceeded {
		t.Fatalf("server: %v", err)
	}
	cs, err := client.OpenStream(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("still open")); err != nil {
		t.Fatal(err)
	}
	cs.Close()
	ss, err := server.AcceptStream(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(ss); err != nil || string(got) != "still open" {
		t.Fatalf("read %q, %v", got, err)
	}
}

func TestContextCanceled(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, &Config{MaxIncomingStreams: 1})
	client := dial(t, l.Addr().String(), nil)
	accept(t, l)
	if _, err := client.OpenStream(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenUniStream(testCtx(t)); err != nil {
		t.Fatal(err)
	}
	silent := silentAddr(t)

	calls := map[string]func(context.Context) error{
		"Dial":            func(ctx context.Context) error { _, err := Dial(ctx, silent, nil); return err },
		"Accept":          func(ctx context.Context) error { _, err := l.Accept(ctx); return err },
		"OpenStream":      func(ctx context.Context) error { _, err := client.OpenStream(ctx); return err },
		"AcceptStream":    func(ctx context.Context) error { _, err := client.AcceptStream(ctx); return err },
		"OpenUniStream":   func(ctx context.Context) error { _, err := client.OpenUniStream(ctx); return err },
		"AcceptUniStream": func(ctx context.Context) error { _, err := client.AcceptUniStream(ctx); return err },
	}
	for name, call := range calls {
		ctx, cancel := context.WithCancel(context.Background())
		time.AfterFunc(50*time.Millisecond, cancel)
		start := time.Now()
		if err := call(ctx); err != context.Canceled {
			t.Errorf("%s: got %v", name, err)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("%s: took %v", name, d)
		}

		ctx, cancel = context.WithCancel(context.Background())
		cancel()
		if err := call(ctx); err != context.Canceled {
			t.Errorf("%s with a canceled context: got %v", name, err)
		}
	}
}

type ticketSignal struct {
	tls.ClientSessionCache
	put chan struct{}
}

func (c *ticketSignal) Put(key string, cs *tls.ClientSessionState) {
	c.ClientSessionCache.Put(key, cs)
	if cs != nil {
		select {
		case c.put <- struct{}{}:
		default:
		}
	}
}

// resumes dials addr twice with one session cache, as a client that wants 0-RTT.
func resumes(t *testing.T, addr string) (ticket, resumed, used0RTT bool) {
	t.Helper()
	cache := &ticketSignal{ClientSessionCache: tls.NewLRUClientSessionCache(4), put: make(chan struct{}, 1)}
	conf := clientTLS()
	conf.ClientSessionCache = cache
	tr := rawTransport(t)
	for i := range 2 {
		c, err := tr.DialEarly(testCtx(t), udpAddr(t, addr), conf, nil)
		if err != nil {
			t.Fatal(err)
		}
		<-c.HandshakeComplete()
		if i == 0 {
			select {
			case <-cache.put:
				ticket = true
			case <-time.After(time.Second):
			}
		}
		st := c.ConnectionState()
		c.CloseWithError(0, "")
		resumed, used0RTT = st.TLS.DidResume, st.Used0RTT
	}
	return ticket, resumed, used0RTT
}

func TestNo0RTT(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	if ticket, resumed, used0RTT := resumes(t, l.Addr().String()); ticket || resumed || used0RTT {
		t.Fatalf("ticket %v, resumed %v, 0-RTT %v", ticket, resumed, used0RTT)
	}
}

func TestResumptionDetected(t *testing.T) {
	checkGoroutines(t)
	cert, err := newCertificate()
	if err != nil {
		t.Fatal(err)
	}
	conf := serverTLS(cert)
	conf.SessionTicketsDisabled = false
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	tr := &quic.Transport{Conn: pc}
	defer tr.Close()
	ln, err := tr.ListenEarly(conf, &quic.Config{Allow0RTT: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if ticket, resumed, used0RTT := resumes(t, ln.Addr().String()); !ticket || !resumed || !used0RTT {
		t.Fatalf("ticket %v, resumed %v, 0-RTT %v", ticket, resumed, used0RTT)
	}
}

type retrySniffer struct {
	net.PacketConn
	retries atomic.Int32
}

func (s *retrySniffer) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := s.PacketConn.ReadFrom(p)
	if n >= 5 && p[0]&0xf0 == 0xf0 && bytes.Equal(p[1:5], []byte{0, 0, 0, 1}) {
		s.retries.Add(1)
	}
	return n, addr, err
}

type tokenSignal struct {
	quic.TokenStore
	put chan struct{}
}

func (s *tokenSignal) Put(key string, token *quic.ClientToken) {
	s.TokenStore.Put(key, token)
	select {
	case s.put <- struct{}{}:
	default:
	}
}

func TestListenerSendsRetry(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	sniffer := &retrySniffer{PacketConn: pc}
	tr := &quic.Transport{Conn: sniffer}
	defer tr.Close()
	store := &tokenSignal{TokenStore: quic.NewLRUTokenStore(4, 4), put: make(chan struct{}, 1)}
	conf := (&Config{}).mustDefaults(t).quic()
	conf.TokenStore = store
	raddr := l.Addr().(*net.UDPAddr)

	c, err := tr.Dial(testCtx(t), raddr, clientTLS(), conf)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.put:
	case <-time.After(5 * time.Second):
		t.Fatal("no NEW_TOKEN token")
	}
	c.CloseWithError(0, "")
	first := sniffer.retries.Load()
	if first == 0 {
		t.Fatal("no Retry packet")
	}

	seen := first
	// Each dial presents the NEW_TOKEN token of the one before, sent a moment earlier: a clock that ticks only
	// every few milliseconds (Windows) must not make the listener accept it.
	for i := range 20 {
		c, err = tr.Dial(testCtx(t), raddr, clientTLS(), conf)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-store.put:
		case <-time.After(5 * time.Second):
			t.Fatalf("dial %d: no NEW_TOKEN token", i+2)
		}
		c.CloseWithError(0, "")
		got := sniffer.retries.Load()
		if got == seen {
			t.Fatalf("dial %d: a NEW_TOKEN token skipped Retry", i+2)
		}
		seen = got
	}
}

// TestNewTokenAge pins the NEW_TOKEN expiry: a token's age is wall-clock time, so after the clock steps back it is
// negative, and only the smallest duration still expires it. TestListenerSendsRetry cannot step the clock.
func TestNewTokenAge(t *testing.T) {
	l, err := Listen(testCtx(t), "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if got := l.tr.MaxTokenAge; got != time.Duration(math.MinInt64) {
		t.Fatalf("MaxTokenAge = %v, want the smallest duration", got)
	}
}

func (c *Config) mustDefaults(t *testing.T) Config {
	t.Helper()
	out, err := c.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestListenerCloseClosesConns(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	client := dial(t, l.Addr().String(), nil)
	server := accept(t, l)
	queued := dial(t, l.Addr().String(), nil)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := l.Accept(testCtx(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Accept after Close: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := server.AcceptStream(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("server: %v", err)
	}
	if _, err := client.AcceptStream(ctx); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("client: %v", err)
	}
	if _, err := queued.AcceptStream(ctx); !errors.Is(err, ErrPeerClosed) && !errors.Is(err, ErrRefused) {
		t.Fatalf("not accepted client: %v", err)
	}
}

func TestCloseReleasesEverything(t *testing.T) {
	checkGoroutines(t)
	ctx := testCtx(t)
	l, err := Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	var conns []*Conn
	for range 3 {
		c, err := Dial(ctx, l.Addr().String(), nil)
		if err != nil {
			t.Fatal(err)
		}
		s, err := l.Accept(ctx)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c, s)
		bs, err := c.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		bs.Write([]byte("x"))
		us, err := c.OpenUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		us.Write([]byte("y"))
		if _, err := s.AcceptStream(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := s.AcceptUniStream(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range conns {
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDialedConnReleasesSocketWhenPeerCloses(t *testing.T) {
	checkGoroutines(t)
	ctx := testCtx(t)
	l := listen(t, nil)
	c, err := Dial(ctx, l.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s := accept(t, l)
	s.Close()
	if _, err := c.AcceptStream(ctx); !errors.Is(err, ErrPeerClosed) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-c.released:
	case <-time.After(5 * time.Second):
		t.Fatal("socket not released")
	}
}

func TestDialIPv4Only(t *testing.T) {
	checkGoroutines(t)
	for _, addr := range []string{"[::1]:9", "[::1%1]:9", "[::ffff:127.0.0.1]:9"} {
		ctx, cancel := context.WithTimeout(testCtx(t), 2*time.Second)
		c, err := Dial(ctx, addr, nil)
		cancel()
		if err == nil {
			c.Close()
		}
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "IPv6") {
			t.Errorf("%s: %v", addr, err)
		}
	}
	l := listen(t, nil)
	port := strconv.Itoa(int(addrPort(t, l).Port()))
	for _, addr := range []string{"127.0.0.1:" + port, "localhost:" + port} {
		ctx, cancel := context.WithTimeout(testCtx(t), 3*time.Second)
		c, err := Dial(ctx, addr, nil)
		cancel()
		if err != nil {
			t.Errorf("%s: %v", addr, err)
			continue
		}
		accept(t, l)
		if a := c.LocalAddr().(*net.UDPAddr).AddrPort().Addr().Unmap(); a != netip.MustParseAddr("127.0.0.1") {
			t.Errorf("%s: dialed from %v", addr, a)
		}
		if r := c.RemoteAddr().(*net.UDPAddr).AddrPort(); r.Addr().Unmap() != netip.MustParseAddr("127.0.0.1") || r.Port() != addrPort(t, l).Port() {
			t.Errorf("%s: connected to %v", addr, r)
		}
		c.Close()
	}
}

func TestDialAddr(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:9", "localhost:9", "node.example:4000"} {
		if err := dialAddr(addr); err != nil {
			t.Errorf("%s: %v", addr, err)
		}
	}
	for _, addr := range []string{
		"[::]:9", "[::1]:9", "[::1%1]:9", "[2001:db8::1]:9", "[fe80::1%1]:9", "[64:ff9b::7f00:1]:9",
		"[::ffff:127.0.0.1]:9", "[::ffff:1.0.0.1]:9",
	} {
		if err := dialAddr(addr); !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "IPv6") {
			t.Errorf("%s: %v", addr, err)
		}
	}
}

func TestDialIPv6OnlyName(t *testing.T) {
	checkGoroutines(t)
	name := ipv6OnlyName(t)
	ctx, cancel := context.WithTimeout(testCtx(t), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, name+":9", nil)
	if err == nil {
		c.Close()
		t.Fatal("connected")
	}
	if ae := (*net.AddrError)(nil); !errors.As(err, &ae) {
		t.Fatalf("not refused before sending: %v", err)
	}
}

func TestDialRefusesDestinations(t *testing.T) {
	checkGoroutines(t)
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := strconv.Itoa(pc.LocalAddr().(*net.UDPAddr).Port)
	pc.Close()
	// Loopback-safe cases come first, so a regression stops before anything is sent off the host.
	for _, addr := range []string{
		"127.0.0.1:0", "0.0.0.0:" + closed, "0.1.2.3:4000", "255.255.255.255:4000", "240.0.0.1:4000", "224.0.0.1:4000",
	} {
		ctx, cancel := context.WithTimeout(testCtx(t), 2*time.Second)
		c, err := Dial(ctx, addr, nil)
		cancel()
		if err == nil {
			c.Close()
		}
		if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "dial destination") {
			t.Fatalf("%s: %v", addr, err)
		}
	}
}
