package kolotransport

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

// checkGoroutines fails the test if goroutines started during it outlive its cleanups.
func checkGoroutines(t *testing.T) {
	t.Helper()
	before := goroutines()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for {
			var leaked []string
			for id, stack := range goroutines() {
				if _, ok := before[id]; !ok {
					leaked = append(leaked, stack)
				}
			}
			if len(leaked) == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("%d goroutines leaked:\n%s", len(leaked), strings.Join(leaked, "\n\n"))
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func goroutines() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	out := make(map[string]string)
	for _, stack := range strings.Split(string(buf), "\n\n") {
		if id, _, ok := strings.Cut(strings.TrimPrefix(stack, "goroutine "), " "); ok {
			out[id] = stack
		}
	}
	return out
}

// rawTransport returns a quic-go transport on a loopback socket.
func rawTransport(t *testing.T) *quic.Transport {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tr := &quic.Transport{Conn: pc}
	t.Cleanup(func() {
		tr.Close()
		pc.Close()
	})
	return tr
}

func udpAddr(t *testing.T, addr string) *net.UDPAddr {
	t.Helper()
	a, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

type gate struct {
	once sync.Once
	ch   chan struct{}
}

func newGate() *gate { return &gate{ch: make(chan struct{})} }

func (g *gate) open() { g.once.Do(func() { close(g.ch) }) }

// stalledDial starts a handshake whose client side blocks until g opens.
func stalledDial(t *testing.T, addr string, g *gate) <-chan error {
	t.Helper()
	tr := rawTransport(t)
	conf := clientTLS()
	conf.VerifyPeerCertificate = func([][]byte, [][]*x509.Certificate) error {
		<-g.ch
		return errors.New("stalled")
	}
	done := make(chan error, 1)
	go func() {
		c, err := tr.Dial(context.Background(), udpAddr(t, addr), conf, nil)
		if err == nil {
			c.CloseWithError(0, "")
		}
		done <- err
		close(done)
	}()
	t.Cleanup(func() {
		g.open()
		<-done
	})
	return done
}

func (l *Listener) counts() (total, perIP int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conns, l.perIP[netip.MustParseAddr("127.0.0.1")]
}

func (l *Listener) perIPEntries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.perIP)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// eventually runs a measurement up to n times and passes on the first run that reports no error. A throughput figure
// depends on the scheduler as well as on the code: a busy machine can spoil one run, a regression spoils all of them.
func eventually(t *testing.T, n int, f func() error) {
	t.Helper()
	var errs []string
	for i := 1; i <= n; i++ {
		err := f()
		if err == nil {
			return
		}
		t.Logf("attempt %d of %d: %v", i, n, err)
		errs = append(errs, err.Error())
	}
	t.Fatalf("failed %d times: %s", n, strings.Join(errs, "; "))
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func listen(t *testing.T, cfg *Config) *Listener {
	t.Helper()
	l, err := Listen(testCtx(t), "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func dial(t *testing.T, addr string, cfg *Config) *Conn {
	t.Helper()
	c, err := Dial(testCtx(t), addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func accept(t *testing.T, l *Listener) *Conn {
	t.Helper()
	c, err := l.Accept(testCtx(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// pair returns a dialed client and the server side of the same connection.
func pair(t *testing.T, serverCfg, clientCfg *Config) (client, server *Conn) {
	t.Helper()
	l := listen(t, serverCfg)
	client = dial(t, l.Addr().String(), clientCfg)
	server = accept(t, l)
	return client, server
}

// silentAddr returns a UDP address that never answers.
func silentAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close() })
	return pc.LocalAddr().String()
}

func wantCode(t *testing.T, err, sentinel error, code uint64) {
	t.Helper()
	var e *Error
	if !errors.Is(err, sentinel) || !errors.As(err, &e) || e.Code != code {
		t.Fatalf("got %v, want %v with code %d", err, sentinel, code)
	}
}

// ipv6OnlyName returns a name with no IPv4 address, such as Debian's ip6-localhost, or skips the test.
func ipv6OnlyName(t *testing.T) string {
	t.Helper()
	const name = "ip6-localhost"
	if _, err := net.DefaultResolver.LookupNetIP(testCtx(t), "ip", name); err != nil {
		t.Skipf("%s does not resolve: %v", name, err)
	}
	if ips, err := net.DefaultResolver.LookupNetIP(testCtx(t), "ip4", name); err == nil {
		t.Skipf("%s resolves to IPv4 %v", name, ips)
	}
	return name
}
