package kolotransport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/netsim"
)

const (
	kbps = 1000 / 8
	mbps = 1000 * kbps
)

type condition struct {
	name     string
	up, down netsim.Profile
	cutAt    time.Duration
	cutFor   time.Duration
	wide     bool // 8 MiB receive windows, so the window does not bind and the controller is what the run measures
}

// conditions are paths like a mobile uplink and a Wi-Fi downlink: delay, jitter, loss, a bandwidth limit and an MTU.
func conditions() []condition {
	mk := func(name string, rate int, loss float64, mtu int) condition {
		return condition{name: name,
			up:   netsim.Profile{Rate: rate, Delay: 60 * time.Millisecond, Jitter: 20 * time.Millisecond, Loss: loss, MTU: mtu, Seed: 1},
			down: netsim.Profile{Rate: 4 * rate, Delay: 60 * time.Millisecond, Jitter: 20 * time.Millisecond, Loss: loss, MTU: mtu, Seed: 2}}
	}
	return []condition{
		mk("clean", 0, 0, 0),
		mk("3 Mbps loss 1%", 3*mbps, 0.01, 0),
		mk("3 Mbps loss 3%", 3*mbps, 0.03, 0),
		mk("1 Mbps loss 1%", 1*mbps, 0.01, 0),
		mk("1 Mbps loss 3%", 1*mbps, 0.03, 0),
		mk("3 Mbps loss 1% mtu 1350", 3*mbps, 0.01, 1350),
		mk("3 Mbps loss 1% mtu 1200", 3*mbps, 0.01, 1200),
		mk("3 Mbps loss 1% mtu 1280", 3*mbps, 0.01, 1280),
		mk("3 Mbps loss 1% mtu 1300", 3*mbps, 0.01, 1300),
		mk("3 Mbps loss 1% mtu 1260", 3*mbps, 0.01, 1260),
		func() condition {
			c := mk("3 Mbps loss 1% outage 3s at 5s", 3*mbps, 0.01, 0)
			c.cutAt, c.cutFor = 5*time.Second, 3*time.Second
			return c
		}(),
		func() condition {
			c := mk("3 Mbps loss 1% rebind at 5s", 3*mbps, 0.01, 0)
			c.cutAt = -5 * time.Second
			return c
		}(),
	}
}

// sender is the end of a bulk run that sends: the connection a listener accepted, the one Dial returned, or the one a
// listener dialed with Listener.Dial.
type sender int

const (
	acceptSends sender = iota
	dialSends
	listenerDialSends
)

func (s sender) String() string {
	return [...]string{"Listener.Accept", "Dial", "Listener.Dial"}[s]
}

// controllerConditions are symmetric paths of 20 and 100 Mbit/s with 60 ms of delay each way, 20 ms of jitter and no, 1% or 3% loss,
// run with wide windows.
func controllerConditions() []condition {
	var out []condition
	for _, rate := range []int{20, 100} {
		for _, loss := range []float64{0, 0.01, 0.03} {
			p := func(seed int64) netsim.Profile {
				return netsim.Profile{Rate: rate * mbps, Delay: 60 * time.Millisecond, Jitter: 20 * time.Millisecond, Loss: loss, Seed: seed}
			}
			out = append(out, condition{name: fmt.Sprintf("%d Mbit/s loss %g%% wide", rate, loss*100), up: p(1), down: p(2), wide: true})
		}
	}
	return out
}

// bulkRun sends a stream from a server to a client through the link for dur and returns the bytes the client had at
// each 100 ms.
func bulkRun(t *testing.T, c condition, cfg *Config, dur time.Duration) (series []uint64, link *netsim.Link) {
	t.Helper()
	series, link, _ = bulkWith(t, c, cfg, cfg, acceptSends, dur)
	return series, link
}

// bulkWith is bulkRun with a Config for the listener (srvCfg) and one for the dialing end (cliCfg), and who sends:
// the data crosses the link's down direction when the accepted connection sends and the up direction otherwise. It
// returns the sending connection, which is closed once the run ends but keeps its stats.
func bulkWith(t *testing.T, c condition, srvCfg, cliCfg *Config, who sender, dur time.Duration) (series []uint64, link *netsim.Link, tx *Conn) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l, err := Listen(ctx, "127.0.0.1:0", srvCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	link, err = netsim.New(l.Addr().String(), c.up, c.down)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	var dialer *Listener
	if who == listenerDialSends {
		if dialer, err = Listen(ctx, "127.0.0.1:0", cliCfg); err != nil {
			t.Fatal(err)
		}
		defer dialer.Close()
	}
	var accepted atomic.Pointer[Conn]
	accepting := make(chan *Stream, 1)
	go func() {
		conn, err := l.Accept(ctx)
		if err != nil {
			return
		}
		accepted.Store(conn)
		st, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		if who == acceptSends {
			sendForever(ctx, st)
			return
		}
		accepting <- st
	}()
	var conn *Conn
	if who == listenerDialSends {
		ap, perr := netip.ParseAddrPort(link.Addr())
		if perr != nil {
			t.Fatal(perr)
		}
		conn, err = dialer.Dial(ctx, ap)
	} else {
		conn, err = Dial(ctx, link.Addr(), cliCfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	st, err := conn.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st.Write([]byte{1})
	rx := st
	if who != acceptSends {
		go sendForever(ctx, st)
		select {
		case rx = <-accepting:
		case <-time.After(10 * time.Second):
			t.Fatal("the listener accepted no stream")
		}
		tx = conn
	}
	var got atomic.Uint64
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 64<<10)
		for {
			n, err := rx.Read(buf)
			got.Add(uint64(n))
			if err != nil && err != io.EOF || n == 0 && err == io.EOF {
				return
			}
			select {
			case <-done:
				return
			default:
			}
		}
	}()
	start := time.Now()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	cut := false
	for now := range tick.C {
		if el := now.Sub(start); !cut && c.cutAt != 0 && el >= max(c.cutAt, -c.cutAt) {
			cut = true
			if c.cutAt > 0 {
				link.Cut(c.cutFor)
			} else {
				link.Rebind()
			}
		}
		series = append(series, got.Load())
		if now.Sub(start) >= dur {
			break
		}
	}
	close(done)
	if who == acceptSends {
		tx = accepted.Load()
	}
	return series, link, tx
}

func sendForever(ctx context.Context, st *Stream) {
	buf := make([]byte, 64<<10)
	for ctx.Err() == nil {
		if _, err := st.Write(buf); err != nil {
			return
		}
	}
}

func summary(name string, s []uint64, link *netsim.Link) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-28s", name)
	last := uint64(0)
	var stall time.Duration = -1
	var lastChange int
	for i, v := range s {
		if v != last {
			last, lastChange = v, i
		} else if i-lastChange >= 20 && stall < 0 && last > 0 {
			stall = time.Duration(lastChange) * 100 * time.Millisecond
		}
	}
	n := len(s)
	rate := func(a, z int) float64 { return float64(s[z-1]-s[a]) / (float64(z-1-a) / 10) / 1e6 * 8 }
	fmt.Fprintf(&b, " total %7.2f MB | Mbit/s: all %5.2f first5s %5.2f last5s %5.2f | stall>=2s at %v | up pk %d mtu %d loss %d q %d | down pk %d mtu %d loss %d q %d",
		float64(s[n-1])/1e6, rate(0, n), rate(0, 50), rate(n-50, n), stall,
		link.Up().Packets, link.Up().DroppedMTU, link.Up().DroppedLoss, link.Up().DroppedQ,
		link.Down().Packets, link.Down().DroppedMTU, link.Down().DroppedLoss, link.Down().DroppedQ)
	return b.String()
}

// TestImpairedBulkReport measures one stream's throughput over each condition, with Reno, Cubic and BBR (default windows, except the \"wide\" paths); set KOLO_TRANSPORT_IMPAIR=<seconds>.
func TestImpairedBulkReport(t *testing.T) {
	v := os.Getenv("KOLO_TRANSPORT_IMPAIR")
	if v == "" {
		t.Skip("set KOLO_TRANSPORT_IMPAIR=<seconds> to measure")
	}
	dur, _ := time.ParseDuration(v + "s")
	for _, c := range append(conditions(), controllerConditions()...) {
		for _, cc := range []string{"reno", "cubic", "bbr"} {
			t.Run(c.name+"/"+cc, func(t *testing.T) {
				cfg := &Config{CongestionControl: cc}
				if c.wide {
					cfg = ccConfig(cc)
				}
				s, link := bulkRun(t, c, cfg, dur)
				t.Log("\n" + summary(c.name+" "+cc, s, link))
			})
		}
	}
}

// transfer sends size bytes from a listener to a dialer through a link and returns how long the dialer took to read them
// all, or the error. setup runs on the link once the connection is open, and during is called while the bytes flow.
func transfer(t *testing.T, size int, cfg *Config, up, down netsim.Profile, during func(*netsim.Link)) (time.Duration, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	l, err := Listen(ctx, "127.0.0.1:0", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	link, err := netsim.New(l.Addr().String(), up, down)
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
		st.Write(make([]byte, size))
		st.Close()
		<-ctx.Done()
	}()
	dctx, dcancel := context.WithTimeout(ctx, 8*time.Second)
	defer dcancel()
	conn, err := Dial(dctx, link.Addr(), cfg)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	st, err := conn.OpenStream(ctx)
	if err != nil {
		return 0, err
	}
	st.Write([]byte{1})
	start := time.Now()
	if during != nil {
		go during(link)
	}
	n, err := io.Copy(io.Discard, st)
	if err == nil && int(n) != size {
		err = fmt.Errorf("read %d of %d bytes", n, size)
	}
	return time.Since(start), err
}

func path(rate int, loss float64, mtu int) (netsim.Profile, netsim.Profile) {
	return netsim.Profile{Rate: rate, Delay: 30 * time.Millisecond, Jitter: 10 * time.Millisecond, Loss: loss, MTU: mtu, Seed: 1},
		netsim.Profile{Rate: rate, Delay: 30 * time.Millisecond, Jitter: 10 * time.Millisecond, Loss: loss, MTU: mtu, Seed: 2}
}

// A path whose MTU is 1280, the smallest an IPv6 link has, or 1260, as an IPv4 packet has on a carrier that translates it,
// still connects and carries data: the first packets are the 1200 bytes QUIC requires every path to carry.
func TestConnectsOverASmallMTU(t *testing.T) {
	checkGoroutines(t)
	for _, mtu := range []int{1280, 1260, 1240} {
		t.Run(fmt.Sprint(mtu), func(t *testing.T) {
			up, down := path(10*mbps, 0, mtu)
			took, err := transfer(t, 2<<20, nil, up, down, nil)
			if err != nil {
				t.Fatalf("mtu %d: %v", mtu, err)
			}
			t.Logf("2 MiB over an MTU of %d in %v", mtu, took.Round(time.Millisecond))
		})
	}
}

// A path that carries nothing for 3 s, and one whose NAT gives the client a new port mid-transfer, are survived: the
// transfer finishes with every byte.
func TestSurvivesAnOutageAndARebind(t *testing.T) {
	checkGoroutines(t)
	up, down := path(8*mbps, 0.005, 0)
	t.Run("outage", func(t *testing.T) {
		took, err := transfer(t, 3<<20, nil, up, down, func(l *netsim.Link) {
			time.Sleep(500 * time.Millisecond)
			l.Cut(3 * time.Second)
		})
		if err != nil {
			t.Fatal(err)
		}
		if took < 3*time.Second {
			t.Fatalf("finished in %v, before the outage ended", took)
		}
	})
	// The target's NAT changes its port: its datagrams reach the client from a new address and the client's to the old
	// one are lost. The dialer does not follow the target, so the connection ends with ErrTimeout and the dialer has
	// to dial again (unlike the case above, where the dialer's NAT changes and the connection follows).
	t.Run("rebind of the target", func(t *testing.T) {
		cfg := &Config{IdleTimeout: 3 * time.Second, KeepAlivePeriod: time.Second}
		_, err := transfer(t, 3<<20, cfg, up, down, func(link *netsim.Link) {
			time.Sleep(500 * time.Millisecond)
			link.RebindFront()
		})
		if !errors.Is(err, ErrTimeout) {
			t.Fatalf("after the target's port changed: %T %v, want ErrTimeout", err, err)
		}
	})
	t.Run("rebind", func(t *testing.T) {
		var l *netsim.Link
		_, err := transfer(t, 3<<20, nil, up, down, func(link *netsim.Link) {
			l = link
			time.Sleep(500 * time.Millisecond)
			link.Rebind()
		})
		if err != nil {
			t.Fatal(err)
		}
		if l.Rebinds() != 1 {
			t.Fatalf("%d rebinds", l.Rebinds())
		}
	})
}

// A NAT that forgets a mapping after 3 s without an outgoing packet does not break an idle connection. With a keepalive
// faster than the NAT's timeout the mapping holds, and the server keeps seeing the same address; with a slower one the
// NAT gives the client a new port at the next keepalive, and the connection moves to it (path validation) without loss.
func TestKeepAliveHoldsANATMapping(t *testing.T) {
	checkGoroutines(t)
	idle := func(t *testing.T, keepAlive time.Duration) int {
		cfg := &Config{KeepAlivePeriod: keepAlive, IdleTimeout: 10 * time.Second}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		l, err := Listen(ctx, "127.0.0.1:0", cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		p, _ := path(0, 0, 0)
		link, err := netsim.New(l.Addr().String(), p, p)
		if err != nil {
			t.Fatal(err)
		}
		defer link.Close()
		link.SetNATIdle(3 * time.Second)
		go func() {
			c, err := l.Accept(ctx)
			if err != nil {
				return
			}
			st, err := c.AcceptStream(ctx)
			if err != nil {
				return
			}
			time.Sleep(8 * time.Second) // idle
			st.Write([]byte("still here"))
			st.Close()
			<-ctx.Done()
		}()
		conn, err := Dial(ctx, link.Addr(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		st, err := conn.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		st.Write([]byte{1})
		b, err := io.ReadAll(st)
		if err != nil || string(b) != "still here" {
			t.Fatalf("keepalive %v: read %q, %v", keepAlive, b, err)
		}
		return link.Rebinds()
	}
	if n := idle(t, time.Second); n != 0 {
		t.Fatalf("a 1s keepalive under a 3s NAT let the mapping go %d times", n)
	}
	if n := idle(t, 5*time.Second); n == 0 {
		t.Fatal("a 5s keepalive under a 3s NAT never lost the mapping: the test does not exercise it")
	}
}

// A path that stops carrying anything for good ends the connection with ErrTimeout after the idle timeout, so an app
// learns of it within seconds and can dial again. A path that drops only the large datagrams does not end it: the
// packet size falls back to 1200 bytes (TestRecoversWhenThePathMTUDrops).
func TestABlackholeEndsTheConnectionWithErrTimeout(t *testing.T) {
	checkGoroutines(t)
	up, down := path(8*mbps, 0, 0)
	cfg := &Config{IdleTimeout: 3 * time.Second, KeepAlivePeriod: time.Second}
	start := time.Now()
	_, err := transfer(t, 8<<20, cfg, up, down, func(l *netsim.Link) {
		time.Sleep(500 * time.Millisecond)
		l.Cut(time.Hour)
	})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("%v, want ErrTimeout", err)
	}
	if d := time.Since(start); d > 20*time.Second {
		t.Fatalf("the connection ended after %v, the idle timeout is 3s", d)
	}
}

// A path whose MTU drops to 1280 after the connection found a larger packet size, as after a route change or on a
// relayed or mobile path, still carries the data: the loss of the large packets brings the size back to 1200 bytes and
// the search starts again below the new MTU. The drop comes while the connection is idle, mid-transfer, or between
// two answers to requests.
func TestRecoversWhenThePathMTUDrops(t *testing.T) {
	checkGoroutines(t)
	const size = 6 << 20
	for _, tc := range []struct {
		name    string
		answers int   // 0 sends size bytes on one stream; otherwise the client asks answers times for size/answers bytes
		dropAt  int64 // bytes of the first answer read before the MTU drops; 0 drops it before the first request
	}{
		{"while idle", 0, 0},
		{"mid-transfer", 0, 1 << 20},
		{"between answers", 24, size / 24},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			l, err := Listen(ctx, "127.0.0.1:0", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			p, _ := path(20*mbps, 0, 0)
			link, err := netsim.New(l.Addr().String(), p, p)
			if err != nil {
				t.Fatal(err)
			}
			defer link.Close()
			server := make(chan *Conn, 1)
			go func() {
				conn, err := l.Accept(ctx)
				if err != nil {
					return
				}
				server <- conn
				for {
					st, err := conn.AcceptStream(ctx)
					if err != nil {
						return
					}
					var n [4]byte
					if _, err := io.ReadFull(st, n[:]); err != nil {
						return
					}
					st.Write(make([]byte, binary.BigEndian.Uint32(n[:])))
					st.Close()
				}
			}()
			conn, err := Dial(ctx, link.Addr(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ask := func(n int) (*Stream, error) {
				st, err := conn.OpenStream(ctx)
				if err != nil {
					return nil, err
				}
				if _, err := st.Write(binary.BigEndian.AppendUint32(nil, uint32(n))); err != nil {
					return nil, err
				}
				return st, st.Close()
			}
			if st, err := ask(1); err != nil {
				t.Fatal(err)
			} else if _, err := io.Copy(io.Discard, st); err != nil {
				t.Fatal(err)
			}
			srv := <-server
			// Probes go out with the traffic: keep the connection busy until discovery raised the size.
			for deadline := time.Now().Add(15 * time.Second); srv.Stats().MaxDatagramSize <= 1300; {
				if time.Now().After(deadline) {
					t.Fatal("path MTU discovery did not raise the packet size in 15 s")
				}
				st, err := ask(1)
				if err != nil {
					t.Fatal(err)
				}
				io.Copy(io.Discard, st)
				time.Sleep(50 * time.Millisecond)
			}
			if tc.dropAt == 0 {
				link.SetMTU(1280)
			}
			dropped := time.Now()
			var read atomic.Int64
			done := make(chan error, 1)
			go func() {
				count, each := 1, size
				if tc.answers > 0 {
					count, each = tc.answers, size/tc.answers
				}
				for i := range count {
					st, err := ask(each)
					if err != nil {
						done <- err
						return
					}
					if i == 0 && tc.dropAt > 0 {
						n, err := io.CopyN(io.Discard, st, tc.dropAt)
						read.Add(n)
						if err != nil {
							done <- err
							return
						}
						link.SetMTU(1280)
						dropped = time.Now()
					}
					n, err := io.Copy(io.Discard, st)
					read.Add(n)
					if err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()
			select {
			case err := <-done:
				if n := read.Load(); err != nil || n != size {
					t.Fatalf("read %d of %d bytes: %v", n, size, err)
				}
			case <-time.After(20 * time.Second):
				conn.Close()
				<-done
				t.Fatalf("read %d of %d bytes in 20 s after the MTU dropped to 1280; server stats %+v", read.Load(), size, srv.Stats())
			}
			if link.Down().DroppedMTU == 0 {
				t.Fatal("no packet was over the cap: the test does not test it")
			}
			if got := srv.Stats().MaxDatagramSize; got > 1280-28 {
				t.Fatalf("the sender still sends %d-byte packets over a 1280-byte MTU", got)
			}
			t.Logf("6 MiB in %v after the drop; packets of %d bytes at the end", time.Since(dropped).Round(time.Millisecond), srv.Stats().MaxDatagramSize)
		})
	}
}
