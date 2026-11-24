package kolotransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/netsim"
	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

// grownCfg keeps the windows small so that the tests fill them quickly: a 2 MiB stream and 3 MiB connection ceiling.
func grownCfg(memory int64) *Config {
	return &Config{WindowGrowth: true, MaxGrownStreamReceiveWindow: 2 << 20, MaxGrownConnReceiveWindow: 3 << 20, MaxGrownMemory: memory}
}

func (g *growth) usedBytes() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.used
}

func (g *growth) grownBy(c *Conn) (int64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, ok := g.conns[c.qc]
	return n, ok
}

func (g *growth) tracked() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.conns)
}

// sumGrown is the growth of every connection; it must equal usedBytes.
func (g *growth) sumGrown() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	var n int64
	for _, v := range g.conns {
		n += v
	}
	return n
}

func rttLink(t *testing.T, target string, oneWay time.Duration, rate int) *netsim.Link {
	t.Helper()
	p := netsim.Profile{Rate: rate, Delay: oneWay}
	link, err := netsim.New(target, p, p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { link.Close() })
	return link
}

// pump sends size bytes from sender on a new stream and has receiver read them all at once, the traffic that makes
// quic-go try to grow the windows.
func pump(t *testing.T, sender, receiver *Conn, size int) {
	t.Helper()
	ctx := testCtx(t)
	st, err := sender.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		buf := make([]byte, 64<<10)
		for left := size; left > 0; left -= len(buf) {
			if _, err := st.Write(buf[:min(len(buf), left)]); err != nil {
				return
			}
		}
		st.Close()
	}()
	rs, err := receiver.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, rs); err != nil || int(n) != size {
		t.Fatalf("read %d of %d bytes: %v", n, size, err)
	}
}

// growWait bounds how long a test waits for the windows to grow. quic-go doubles a window only when the reader consumes
// it within about three round trips, which a busy machine can delay but not prevent.
const growWait = 60 * time.Second

// growOneWay is the link delay of the growth tests: at a 50 ms round trip the auto-tuning needs a reader of only
// about 5 MB/s to double a 1 MiB window, well under what the loopback gives.
const growOneWay = 25 * time.Millisecond

// pumpUntil streams data from sender to receiver, read as fast as it comes, until done reports true, and fails the test
// if that takes longer than growWait.
func pumpUntil(t *testing.T, sender, receiver *Conn, what string, done func() bool) {
	t.Helper()
	ctx := testCtx(t)
	st, err := sender.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		buf := make([]byte, 64<<10)
		for {
			select {
			case <-stop:
				st.Close()
				return
			default:
			}
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	})
	rs, err := receiver.AcceptStream(ctx)
	if err != nil {
		close(stop)
		t.Fatal(err)
	}
	wg.Go(func() { io.Copy(io.Discard, rs) })
	deadline := time.Now().Add(growWait)
	ok := done()
	for !ok && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		ok = done()
	}
	close(stop)
	wg.Wait()
	if !ok {
		t.Fatalf("timed out after %v waiting for %s", growWait, what)
	}
}

// pumpGrown pumps until g has grown receiver by want bytes.
func pumpGrown(t *testing.T, g *growth, sender, receiver *Conn, want int64) {
	t.Helper()
	pumpUntil(t, sender, receiver, "the connection to grow", func() bool {
		n, _ := g.grownBy(receiver)
		return n >= want
	})
}

// warmGrown connects a new sender through the link, marks the accepted conn and pumps until it has grown by want.
func warmGrown(t *testing.T, l *Listener, link *netsim.Link, want int64) (sender, receiver *Conn) {
	t.Helper()
	sender = dial(t, link.Addr(), nil)
	receiver = accept(t, l)
	if !receiver.AllowWindowGrowth() {
		t.Fatal("AllowWindowGrowth refused")
	}
	pumpGrown(t, l.grow, sender, receiver, want)
	return sender, receiver
}

// warm connects a new sender through the link, optionally marks the accepted conn, pumps size bytes and returns both.
func warm(t *testing.T, l *Listener, link *netsim.Link, size int, trust bool) (sender, receiver *Conn) {
	t.Helper()
	sender = dial(t, link.Addr(), nil)
	receiver = accept(t, l)
	if trust && !receiver.AllowWindowGrowth() {
		t.Fatal("AllowWindowGrowth refused")
	}
	pump(t, sender, receiver, size)
	return sender, receiver
}

// credit is how many bytes writer can put into its peer's connection while the peer reads nothing: it writes on six
// streams at once, each with 256 KiB of credit, and the connection window blocks them.
func credit(t *testing.T, writer *Conn) int {
	t.Helper()
	ctx := testCtx(t)
	var streams []*Stream
	for range 6 {
		w, err := writer.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, w)
	}
	var sent atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(time.Second)
	for _, w := range streams {
		wg.Go(func() {
			w.SetWriteDeadline(deadline)
			n, err := w.Write(make([]byte, 1<<20))
			if err != os.ErrDeadlineExceeded {
				t.Errorf("write: %v", err)
			}
			sent.Add(int64(n))
		})
	}
	wg.Wait()
	return int(sent.Load())
}

// Without WindowGrowth the windows start at their maximum, quic-go gets no hook, a connection cannot be marked, and
// nothing grows however fast the data is read.
func TestNoGrowthByDefault(t *testing.T) {
	checkGoroutines(t)
	c, err := (*Config)(nil).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	q := c.quic()
	if q.InitialStreamReceiveWindow != 256<<10 || q.MaxStreamReceiveWindow != 256<<10 ||
		q.InitialConnectionReceiveWindow != 512<<10 || q.MaxConnectionReceiveWindow != 512<<10 {
		t.Fatalf("windows %+v", q)
	}
	l := listen(t, nil)
	if l.grow != nil || l.quicConfig().AllowConnectionWindowIncrease != nil {
		t.Fatal("a growth budget exists without WindowGrowth")
	}
	link := rttLink(t, l.Addr().String(), 10*time.Millisecond, 0)
	sender, receiver := warm(t, l, link, 6<<20, false)
	if receiver.AllowWindowGrowth() {
		t.Fatal("AllowWindowGrowth succeeded with WindowGrowth off")
	}
	if n := credit(t, sender); n > 512<<10 {
		t.Fatalf("a peer wrote %d bytes into a 512 KiB connection window", n)
	}
}

// With growth on, a connection nobody marked keeps its initial connection window and the budget stays untouched, however
// fast it reads, and so does one that ended before it was marked.
func TestUnmarkedConnectionNeverGrows(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, grownCfg(64<<20))
	link := rttLink(t, l.Addr().String(), 10*time.Millisecond, 0)
	sender, receiver := warm(t, l, link, 6<<20, false)
	if used := l.grow.usedBytes(); used != 0 || l.grow.tracked() != 0 {
		t.Fatalf("budget used %d with %d connections tracked, for a connection nobody marked", used, l.grow.tracked())
	}
	if n := credit(t, sender); n > 512<<10 {
		t.Fatalf("an unmarked peer wrote %d bytes into a 512 KiB connection window", n)
	}
	receiver.Close()
	if receiver.AllowWindowGrowth() {
		t.Fatal("a closed connection was marked")
	}
	waitFor(t, "no tracked connection", func() bool { return l.grow.tracked() == 0 })
}

// A marked connection grows, up to its ceiling and not beyond, and the budget is returned when it ends.
func TestMarkedConnectionGrowsAndReleases(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, grownCfg(64<<20))
	link := rttLink(t, l.Addr().String(), growOneWay, 0)
	sender, receiver := warmGrown(t, l, link, 3<<20-512<<10)
	grown, ok := l.grow.grownBy(receiver)
	if !ok || grown != 3<<20-512<<10 {
		t.Fatalf("grown %d (tracked %v), want %d", grown, ok, 3<<20-512<<10)
	}
	if l.grow.usedBytes() != grown {
		t.Fatalf("budget used %d, connection grown %d", l.grow.usedBytes(), grown)
	}
	if n := credit(t, sender); n <= 1<<20 || n > 3<<20 {
		t.Fatalf("a peer wrote %d bytes: want more than 1 MiB and at most the 3 MiB ceiling", n)
	}
	if !receiver.AllowWindowGrowth() {
		t.Fatal("marking twice failed")
	}
	receiver.Close()
	waitFor(t, "budget released", func() bool { return l.grow.usedBytes() == 0 && l.grow.tracked() == 0 })
}

// The budget is shared: with room for two connections to reach their ceiling, the third marked connection does not
// grow while they live, and does once one of them ends.
func TestBudgetIsSharedAndReturned(t *testing.T) {
	checkGoroutines(t)
	const perConn = 3<<20 - 512<<10
	l := listen(t, grownCfg(2*perConn))
	link := rttLink(t, l.Addr().String(), growOneWay, 0)
	_, a := warmGrown(t, l, link, perConn)
	_, b := warmGrown(t, l, link, perConn)
	if ga, _ := l.grow.grownBy(a); ga != perConn {
		t.Fatalf("first grew %d, want %d", ga, perConn)
	}
	if gb, _ := l.grow.grownBy(b); gb != perConn {
		t.Fatalf("second grew %d, want %d", gb, perConn)
	}
	sc, c := warm(t, l, link, 8<<20, true)
	if gc, ok := l.grow.grownBy(c); !ok || gc != 0 {
		t.Fatalf("third grew %d (tracked %v) with the budget used up", gc, ok)
	}
	if used := l.grow.usedBytes(); used != 2*perConn || l.grow.sumGrown() != used {
		t.Fatalf("budget used %d, sum of connections %d, limit %d", used, l.grow.sumGrown(), 2*perConn)
	}
	if n := credit(t, sc); n > 512<<10 {
		t.Fatalf("the third connection took %d bytes, over its 512 KiB window", n)
	}
	a.Close()
	waitFor(t, "first connection's budget", func() bool { return l.grow.usedBytes() == perConn })
	// A new connection makes quic-go try again, and now there is room for one more.
	_, d := warmGrown(t, l, link, perConn)
	if gd, _ := l.grow.grownBy(d); gd != perConn {
		t.Fatalf("a connection after the release grew %d, want %d", gd, perConn)
	}
	if used := l.grow.usedBytes(); used != 2*perConn || l.grow.sumGrown() != used {
		t.Fatalf("budget used %d, sum of connections %d, limit %d", used, l.grow.sumGrown(), 2*perConn)
	}
}

// An accepted connection can carry data before the application knows who it is; that never grows it, and marking it
// later does.
func TestGrowthStartsWhenMarked(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, grownCfg(64<<20))
	link := rttLink(t, l.Addr().String(), growOneWay, 0)
	sender := dial(t, link.Addr(), nil)
	receiver := accept(t, l)
	pump(t, sender, receiver, 4<<20)
	if g, ok := l.grow.grownBy(receiver); ok || g != 0 {
		t.Fatalf("grew %d before it was marked", g)
	}
	if !receiver.AllowWindowGrowth() {
		t.Fatal("not marked")
	}
	pumpGrown(t, l.grow, sender, receiver, 3<<20-512<<10)
	if g, _ := l.grow.grownBy(receiver); g != 3<<20-512<<10 {
		t.Fatalf("grew %d after it was marked, want %d", g, 3<<20-512<<10)
	}
}

// Marking and ending connections at the same time leaves the budget empty and no goroutine behind.
func TestMarkingWhileConnectionsEnd(t *testing.T) {
	checkGoroutines(t)
	cfg := grownCfg(64 << 20)
	cfg.MaxConnsPerIP = 32
	l := listen(t, cfg)
	var senders, receivers []*Conn
	for range 24 {
		senders = append(senders, dial(t, l.Addr().String(), nil))
		receivers = append(receivers, accept(t, l))
	}
	var wg sync.WaitGroup
	for i, r := range receivers {
		wg.Go(func() { r.AllowWindowGrowth() })
		wg.Go(func() { r.AllowWindowGrowth() })
		if i%2 == 0 {
			wg.Go(func() { senders[i].Close() })
		} else {
			wg.Go(func() { r.Close() })
		}
	}
	wg.Wait()
	for _, r := range receivers {
		r.Close()
	}
	waitFor(t, "empty budget", func() bool { return l.grow.usedBytes() == 0 && l.grow.tracked() == 0 })
}

// Connections a Listener dials share its budget like the ones it accepts.
func TestListenerDialSharesTheBudget(t *testing.T) {
	checkGoroutines(t)
	a := listen(t, grownCfg(64<<20))
	b := listen(t, nil)
	link := rttLink(t, b.Addr().String(), growOneWay, 0)
	conn, err := a.Dial(testCtx(t), netip.MustParseAddrPort(link.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	peer := accept(t, b)
	if !conn.AllowWindowGrowth() {
		t.Fatal("not marked")
	}
	pumpGrown(t, a.grow, peer, conn, 3<<20-512<<10)
	if g, ok := a.grow.grownBy(conn); !ok || g != 3<<20-512<<10 {
		t.Fatalf("a dialed connection grew %d (tracked %v), want %d", g, ok, 3<<20-512<<10)
	}
	if a.grow.usedBytes() != a.grow.sumGrown() {
		t.Fatal("budget out of step")
	}
	conn.Close()
	waitFor(t, "budget released", func() bool { return a.grow.usedBytes() == 0 })
}

// Window growth on Dial: the connection has its own budget, MaxGrownMemory.
func TestDialedConnGrowsWithItsOwnBudget(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, nil)
	link := rttLink(t, l.Addr().String(), 10*time.Millisecond, 0)
	cfg := grownCfg(1 << 20)
	conn := dial(t, link.Addr(), cfg)
	srv := accept(t, l)
	if !conn.AllowWindowGrowth() {
		t.Fatal("not marked")
	}
	ctx := testCtx(t)
	go func() {
		st, err := srv.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 64<<10)
		for ctx.Err() == nil {
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	}()
	st, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st.Write([]byte{1})
	var read atomic.Int64
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := st.Read(buf)
			read.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	waitFor(t, "growth", func() bool { g, _ := conn.grow.grownBy(conn); return g > 0 })
	// The bound must hold while the flood goes on: wait for 16 times the budget to pass, not for a time.
	at := read.Load()
	waitFor(t, "16 MiB more through the stream", func() bool { return read.Load()-at >= 16<<20 })
	if used := conn.grow.usedBytes(); used > 1<<20 || used == 0 {
		t.Fatalf("a dialed conn used %d of its 1 MiB budget", used)
	}
}

// A sender that floods an untrusted connection with one-byte frames cannot put more than the connection window into
// its buffers, and the heap it costs stays within the documented 176 bytes per unread byte.
func TestFloodOfSmallFramesStaysWithinTheInitialWindow(t *testing.T) {
	checkGoroutines(t)
	cfg := &Config{MaxStreamReceiveWindow: 64 << 10, MaxConnReceiveWindow: 128 << 10, WindowGrowth: true, MaxGrownMemory: 64 << 20,
		MaxGrownStreamReceiveWindow: 1 << 20, MaxGrownConnReceiveWindow: 2 << 20}
	l := listen(t, cfg)
	link := rttLink(t, l.Addr().String(), 10*time.Millisecond, 0)
	_, receiver := warm(t, l, link, 4<<20, false)
	writer := dial(t, link.Addr(), nil)
	accept(t, l)
	ctx := testCtx(t)
	before := heapAlloc()
	var sent int
	for i := range 4 {
		w, err := writer.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			// One frame per packet, the costliest way to send a byte.
			for end := time.Now().Add(1500 * time.Millisecond); time.Now().Before(end); sent++ {
				if _, err := w.Write([]byte{1}); err != nil {
					t.Fatal(err)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}
		w.SetWriteDeadline(time.Now().Add(time.Second))
		for {
			if _, err := w.Write([]byte{1}); err != nil {
				break
			}
			sent++
		}
	}
	time.Sleep(500 * time.Millisecond)
	heap := int64(heapAlloc()) - int64(before)
	// The sender may hold a few KiB it has not sent yet, so the bound has some slack.
	if sent > 128<<10+16<<10 {
		t.Fatalf("flooded %d bytes into a 128 KiB connection window", sent)
	}
	if limit := int64(128<<10) * 176 * 3 / 2; heap > limit {
		t.Fatalf("flood of %d bytes held %d bytes of heap, over %d", sent, heap, limit)
	}
	t.Logf("flood: %d bytes accepted, heap %d bytes (%.0f per byte)", sent, heap, float64(heap)/float64(max(sent, 1)))
	runtime.KeepAlive(receiver)
}

// The hook itself, without a network: only a marked connection is granted, a grant never goes past the budget, a refused
// one changes nothing, connections share what is left, and release gives back exactly what a connection took.
func TestGrowthAllowAccounting(t *testing.T) {
	g := &growth{limit: 10, conns: make(map[*quic.Conn]int64)}
	a, b, stranger := new(quic.Conn), new(quic.Conn), new(quic.Conn)
	g.conns[a], g.conns[b] = 0, 0
	for _, c := range []struct {
		conn  *quic.Conn
		delta uint64
		want  bool
		used  int64
	}{
		{stranger, 1, false, 0},
		{a, 4, true, 4},
		{a, 4, true, 8},
		{a, 3, false, 8},
		{b, 3, false, 8},
		{b, 2, true, 10},
		{b, 1, false, 10},
		{a, 0, true, 10},
	} {
		if got := g.allow(c.conn, c.delta); got != c.want || g.usedBytes() != c.used || g.sumGrown() != c.used {
			t.Fatalf("allow(%d) = %v, used %d, sum %d; want %v, %d", c.delta, got, g.usedBytes(), g.sumGrown(), c.want, c.used)
		}
	}
	if n := g.conns[a]; n != 8 {
		t.Fatalf("a grew %d, want 8", n)
	}
	if n := g.conns[b]; n != 2 {
		t.Fatalf("b grew %d, want 2", n)
	}
	g.release(a)
	if g.usedBytes() != 2 || g.tracked() != 1 {
		t.Fatalf("after releasing a: used %d, tracked %d", g.usedBytes(), g.tracked())
	}
	if g.allow(a, 1) {
		t.Fatal("a released connection was granted")
	}
	if !g.allow(b, 8) || g.usedBytes() != 10 {
		t.Fatalf("b could not take what a returned: used %d", g.usedBytes())
	}
	g.release(b)
	g.release(b)
	if g.usedBytes() != 0 || g.tracked() != 0 {
		t.Fatalf("budget used %d, tracked %d after every release", g.usedBytes(), g.tracked())
	}
}

func TestGrowthConfig(t *testing.T) {
	got, err := (&Config{WindowGrowth: true}).withDefaults()
	if err != nil || got.MaxGrownStreamReceiveWindow != 4<<20 || got.MaxGrownConnReceiveWindow != 8<<20 || got.MaxGrownMemory != 64<<20 {
		t.Fatalf("defaults %+v, %v", got, err)
	}
	q := got.quic()
	if q.InitialStreamReceiveWindow != 256<<10 || q.InitialConnectionReceiveWindow != 512<<10 ||
		q.MaxStreamReceiveWindow != 4<<20 || q.MaxConnectionReceiveWindow != 8<<20 {
		t.Fatalf("quic windows %+v", q)
	}
	// The defaults follow larger initial windows.
	got, err = (&Config{WindowGrowth: true, MaxStreamReceiveWindow: 8 << 20, MaxConnReceiveWindow: 16 << 20}).withDefaults()
	if err != nil || got.MaxGrownStreamReceiveWindow != 8<<20 || got.MaxGrownConnReceiveWindow != 16<<20 {
		t.Fatalf("defaults above the initial windows: %+v, %v", got, err)
	}
	for name, cfg := range map[string]Config{
		"grown stream below initial":  {WindowGrowth: true, MaxStreamReceiveWindow: 256 << 10, MaxGrownStreamReceiveWindow: 128 << 10},
		"grown stream above 16 MiB":   {WindowGrowth: true, MaxGrownStreamReceiveWindow: 16<<20 + 1},
		"grown conn below stream":     {WindowGrowth: true, MaxGrownStreamReceiveWindow: 4 << 20, MaxGrownConnReceiveWindow: 2 << 20},
		"grown conn below initial":    {WindowGrowth: true, MaxConnReceiveWindow: 4 << 20, MaxGrownStreamReceiveWindow: 1 << 20, MaxGrownConnReceiveWindow: 2 << 20},
		"grown conn above 64 MiB":     {WindowGrowth: true, MaxGrownStreamReceiveWindow: 16 << 20, MaxGrownConnReceiveWindow: 64<<20 + 1},
		"memory below 1 MiB":          {WindowGrowth: true, MaxGrownMemory: 1<<20 - 1},
		"negative memory":             {WindowGrowth: true, MaxGrownMemory: -1},
		"grown stream without growth": {MaxGrownStreamReceiveWindow: 1 << 20},
		"grown conn without growth":   {MaxGrownConnReceiveWindow: 1 << 20},
		"memory without growth":       {MaxGrownMemory: 1 << 20},
	} {
		if _, err := cfg.withDefaults(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: %v", name, err)
		}
	}
	// MaxMemory covers the initial windows plus what can grow.
	listenCfg := Config{MaxConns: 10, MaxDialed: 10, MaxMemory: 64 << 20, WindowGrowth: true}
	if _, err := listenCfg.listenConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("MaxMemory 64 MiB with 20 connections and 64 MiB of growth: %v", err)
	}
	listenCfg.MaxGrownMemory = 16 << 20
	if _, err := listenCfg.listenConfig(); err != nil {
		t.Fatalf("20 × 512 KiB plus 16 MiB fits 64 MiB: %v", err)
	}
	// A budget the connections cannot use up does not count.
	listenCfg = Config{MaxConns: 1, MaxDialed: 1, MaxMemory: 64 << 20, WindowGrowth: true, MaxGrownMemory: 1 << 30}
	if _, err := listenCfg.listenConfig(); err != nil {
		t.Fatalf("two connections can grow only 15 MiB: %v", err)
	}
	if !reflect.DeepEqual((&Config{}).quic().AllowConnectionWindowIncrease == nil, true) {
		t.Fatal("hook set without growth")
	}
}

// goodput is the best MB/s of three 1.5 s windows that one stream gets over a link with the given one-way delay and
// 100 Mbit/s each way, from a sender with the default Config to a receiver with cfg. The best window leaves out the
// stalls of a busy machine, and a marked receiver's windows start only once its connection window has grown past the
// path's bandwidth-delay product, so the figure is the speed the window allows, not the time it took to grow.
func goodput(t *testing.T, oneWay time.Duration, cfg *Config, trust bool) float64 {
	t.Helper()
	const warmup, measure, windows = 2 * time.Second, 1500 * time.Millisecond, 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	p := netsim.Profile{Rate: 100 * mbps, Delay: oneWay}
	link, err := netsim.New(l.Addr().String(), p, p)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	go func() {
		conn, err := l.Accept(ctx)
		if err != nil {
			return
		}
		st, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		buf := make([]byte, 64<<10)
		for ctx.Err() == nil {
			if _, err := st.Write(buf); err != nil {
				return
			}
		}
	}()
	conn, err := Dial(ctx, link.Addr(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if trust && !conn.AllowWindowGrowth() {
		t.Fatal("AllowWindowGrowth refused")
	}
	st, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st.Write([]byte{1})
	var got atomic.Int64
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := st.Read(buf)
			got.Add(int64(n))
			if err != nil {
				return
			}
		}
	}()
	time.Sleep(warmup)
	if trust {
		// 2 MiB of growth is a 2.5 MiB connection window, twice the 1.25 MB the path holds.
		deadline := time.Now().Add(growWait)
		for n, _ := conn.grow.grownBy(conn); n < 2<<20; n, _ = conn.grow.grownBy(conn) {
			if time.Now().After(deadline) {
				t.Fatalf("the marked connection grew %d bytes in %v", n, growWait)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	var best float64
	for range windows {
		start := got.Load()
		time.Sleep(measure)
		best = max(best, float64(got.Load()-start)/measure.Seconds()/1e6)
	}
	return best
}

// On a path with a 100 ms round trip and 100 Mbit/s, a trusted receiver gets clearly more than one that is not marked.
func TestTrustedPeerGetsHigherGoodput(t *testing.T) {
	checkGoroutines(t)
	eventually(t, 2, func() error {
		plain := goodput(t, 50*time.Millisecond, nil, false)
		unmarked := goodput(t, 50*time.Millisecond, &Config{WindowGrowth: true}, false)
		tight := goodput(t, 50*time.Millisecond, &Config{WindowGrowth: true, MaxConnReceiveWindow: 256 << 10}, false)
		trusted := goodput(t, 50*time.Millisecond, &Config{WindowGrowth: true}, true)
		t.Logf("RTT 100 ms, 100 Mbit/s, best of three windows: default %.2f MB/s, growth on not marked %.2f MB/s, growth on with a 256 KiB connection window not marked %.2f MB/s, marked %.2f MB/s",
			plain, unmarked, tight, trusted)
		// Marked is link-bound at 12.2 MB/s, 4.8 to 6 times the default and 2.5 to 3 times the unmarked in 20 measured runs.
		if trusted < 3*plain || trusted < 3*unmarked/2 {
			return fmt.Errorf("marked %.2f MB/s is not 3 times the default %.2f and 1.5 times the unmarked %.2f", trusted, plain, unmarked)
		}
		// A stream window that grows past its start is bounded by the connection window, which an unmarked peer cannot raise.
		// Both sit at the 2.6 MB/s a 256 KiB window gives at 100 ms (seen 2.0 to 2.5 across windows), where an unbounded one
		// would reach the 4 to 5 MB/s of the unmarked peer, so 1.5 times the default separates them.
		if tight > 1.5*plain {
			return fmt.Errorf("not marked with a 256 KiB connection window: %.2f MB/s, over the default's %.2f", tight, plain)
		}
		return nil
	})
}

// TestGrowthGoodputReport prints the table of the docs; set KOLO_TRANSPORT_GROWTH=1.
func TestGrowthGoodputReport(t *testing.T) {
	if os.Getenv("KOLO_TRANSPORT_GROWTH") == "" {
		t.Skip("set KOLO_TRANSPORT_GROWTH=1 to measure")
	}
	for _, rtt := range []time.Duration{20, 50, 100, 200, 300} {
		oneWay := rtt * time.Millisecond / 2
		plain := goodput(t, oneWay, nil, false)
		unmarked := goodput(t, oneWay, &Config{WindowGrowth: true}, false)
		tight := goodput(t, oneWay, &Config{WindowGrowth: true, MaxConnReceiveWindow: 256 << 10}, false)
		trusted := goodput(t, oneWay, &Config{WindowGrowth: true}, true)
		t.Logf("RTT %3d ms: default %6.2f | growth on, not marked %6.2f | growth on, 256 KiB conn window, not marked %6.2f | marked %6.2f MB/s",
			rtt, plain, unmarked, tight, trusted)
	}
}
