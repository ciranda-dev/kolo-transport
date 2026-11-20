package kolotransport

import (
	"errors"
	"fmt"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/netsim"
	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/congestion"
	"github.com/ciranda-dev/kolo-transport/internal/quicgo/monotime"
)

func TestCongestionControlConfig(t *testing.T) {
	for _, v := range []string{"", "reno", "cubic", "bbr"} {
		c := &Config{CongestionControl: v}
		got, err := c.withDefaults()
		if err != nil {
			t.Fatalf("%q: %v", v, err)
		}
		want := v
		if v == "" {
			want = defaultCongestionControl
		}
		if got.CongestionControl != want {
			t.Fatalf("%q became %q, want %q", v, got.CongestionControl, want)
		}
		if l, err := c.listenConfig(); err != nil || l.CongestionControl != want {
			t.Fatalf("%q: listen config %q, %v", v, l.CongestionControl, err)
		}
	}
	for _, v := range []string{"BBR", "Reno", "Cubic", "bbr ", " bbr", "bbr2", "bbr\x00", "-"} {
		c := &Config{CongestionControl: v}
		if _, err := c.withDefaults(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%q: withDefaults %v, want ErrInvalidConfig", v, err)
		}
		if _, err := c.listenConfig(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("%q: listenConfig %v, want ErrInvalidConfig", v, err)
		}
		ctx := testCtx(t)
		if l, err := Listen(ctx, "127.0.0.1:0", c); !errors.Is(err, ErrInvalidConfig) {
			if l != nil {
				l.Close()
			}
			t.Fatalf("%q: Listen %v, want ErrInvalidConfig", v, err)
		}
		if conn, err := Dial(ctx, "127.0.0.1:9", c); !errors.Is(err, ErrInvalidConfig) {
			if conn != nil {
				conn.Close()
			}
			t.Fatalf("%q: Dial %v, want ErrInvalidConfig", v, err)
		}
	}
}

func TestCongestionControlDefaultIsReno(t *testing.T) {
	if forcedCC != "" {
		t.Skip("the suite runs with a forced controller")
	}
	if defaultCongestionControl != "reno" {
		t.Fatalf("default %q", defaultCongestionControl)
	}
	if c, err := (*Config)(nil).withDefaults(); err != nil || c.CongestionControl != "reno" {
		t.Fatalf("nil config: %q, %v", c.CongestionControl, err)
	}
}

// mbit is the Mbit/s the receiver read between the ticks from and to of a bulk series (100 ms each).
func mbit(s []uint64, from, to int) float64 {
	return float64(s[to-1]-s[from]) / (float64(to-1-from) / 10) / 1e6 * 8
}

// lossy is a 20 Mbit/s path with 60 ms of delay each way, 20 ms of jitter and 1 percent random loss: Reno cannot fill it.
func lossy() condition {
	p := func(seed int64) netsim.Profile {
		return netsim.Profile{Rate: 20 * mbps, Delay: 60 * time.Millisecond, Jitter: 20 * time.Millisecond, Loss: 0.01, Seed: seed}
	}
	return condition{name: "20 Mbit/s loss 1%", up: p(1), down: p(2)}
}

// ccConfig has receive windows large enough that they do not bind on the lossy path.
func ccConfig(cc string) *Config {
	return &Config{CongestionControl: cc, MaxConns: 2, MaxDialed: 1, MaxStreamReceiveWindow: 8 << 20, MaxConnReceiveWindow: 16 << 20}
}

// ccRun runs one bulk transfer over the lossy path for 8 s and returns the Mbit/s the receiver read from the 3rd second on
// and the congestion window the sender reports at the end.
func ccRun(t *testing.T, c condition, who sender, senderCC, receiverCC string) (rate float64, window uint64) {
	t.Helper()
	srv, cli := ccConfig(senderCC), ccConfig(receiverCC)
	if who != acceptSends {
		srv, cli = cli, srv
	}
	series, _, tx := bulkWith(t, c, srv, cli, who, 8*time.Second)
	return mbit(series, 30, len(series)), tx.qc.ConnectionStats().CongestionWindow
}

// On a path with random loss Reno sends at a fraction of the link and BBR fills it, whichever connection sends:
// the one Listener.Accept returned, the one Dial returned or the one Listener.Dial returned. Each end sets its own
// sending, so BBR on the sender is enough, and BBR on the receiver alone changes nothing.
func TestBBRFillsALossyPathThatRenoDoesNot(t *testing.T) {
	checkGoroutines(t)
	reno, renoWindow := ccRun(t, lossy(), acceptSends, "reno", "bbr")
	t.Logf("sender reno, receiver bbr: %.2f Mbit/s, window %d", reno, renoWindow)
	if reno > 4 {
		t.Fatalf("Reno sent %.2f Mbit/s over a path with 1%% loss, the test does not exercise the loss", reno)
	}
	if renoWindow == 0 || renoWindow >= 100_000 {
		t.Fatalf("Reno window stat %d", renoWindow)
	}
	for _, who := range []sender{acceptSends, dialSends, listenerDialSends} {
		t.Run(who.String(), func(t *testing.T) {
			eventually(t, 3, func() error {
				rate, window := ccRun(t, lossy(), who, "bbr", "reno")
				t.Logf("sender bbr, receiver reno: %.2f Mbit/s, window %d", rate, window)
				if rate < 10 || rate < 4*reno {
					return fmt.Errorf("BBR sent %.2f Mbit/s over a 20 Mbit/s path (Reno %.2f), want at least 10 and 4 times Reno", rate, reno)
				}
				if window < 100_000 {
					return fmt.Errorf("window stat %d: the stats do not follow the BBR window", window)
				}
				return nil
			})
		})
	}
}

// A NAT that gives the dialer a new port makes the sender move the connection to the new path, which must not bring
// Reno back: BBR must still be sending at the link rate in the last 5 s.
func TestBBRSurvivesARebind(t *testing.T) {
	checkGoroutines(t)
	eventually(t, 2, func() error {
		c := lossy()
		c.cutAt = -4 * time.Second
		series, link, tx := bulkWith(t, c, ccConfig("bbr"), ccConfig("bbr"), acceptSends, 12*time.Second)
		if name := tx.qc.CongestionControlName(); name != "bbr" {
			t.Fatalf("the sender runs %q after the rebind, want bbr", name)
		}
		if link.Rebinds() != 1 {
			t.Fatalf("%d rebinds", link.Rebinds())
		}
		before, after := mbit(series, 20, 40), mbit(series, len(series)-50, len(series))
		t.Logf("%.2f Mbit/s before the rebind, %.2f in the last 5 s", before, after)
		if after < 10 {
			return fmt.Errorf("%.2f Mbit/s in the last 5 s after the rebind, want at least 10 (BBR before it: %.2f)", after, before)
		}
		return nil
	})
}

// The same for Cubic: the rebind rebuilds the built-in sender, which must stay Cubic.
func TestCubicSurvivesARebind(t *testing.T) {
	checkGoroutines(t)
	c := lossy()
	c.cutAt = -3 * time.Second
	_, link, tx := bulkWith(t, c, ccConfig("cubic"), ccConfig("cubic"), acceptSends, 7*time.Second)
	if link.Rebinds() != 1 {
		t.Fatalf("%d rebinds", link.Rebinds())
	}
	if name := tx.qc.CongestionControlName(); name != "cubic" {
		t.Fatalf("the sender runs %q after the rebind, want cubic", name)
	}
}

// Every connection a Config makes, on both ends, runs the sender the Config names: quic-go's Reno, its Cubic branch, or BBR.
func TestEveryConnectionRunsTheNamedController(t *testing.T) {
	checkGoroutines(t)
	for cc, want := range map[string]string{"": defaultName(), "reno": "reno", "cubic": "cubic", "bbr": "bbr"} {
		t.Run("cc "+cc, func(t *testing.T) {
			cfg := &Config{CongestionControl: cc}
			client, server := pair(t, cfg, cfg)
			a, b := listen(t, cfg), listen(t, cfg)
			dialed := func() *Conn {
				ap := netip.MustParseAddrPort(b.Addr().String())
				c, err := a.Dial(testCtx(t), ap)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { c.Close() })
				return c
			}()
			accepted := accept(t, b)
			for name, conn := range map[string]*Conn{"Dial": client, "Listener.Accept": server, "Listener.Dial": dialed, "its Accept": accepted} {
				if got := conn.qc.CongestionControlName(); got != want {
					t.Fatalf("%s runs %q, want %q", name, got, want)
				}
			}
		})
	}
}

func defaultName() string {
	if defaultCongestionControl == "bbr" {
		return "bbr"
	}
	return defaultCongestionControl
}

// probeCC is a BBR that records which packet numbers the connection tells it about.
type probeCC struct {
	congestion.CongestionControlEx
	mu    sync.Mutex
	sent  map[congestion.PacketNumber]bool
	first congestion.PacketNumber
	last  congestion.PacketNumber
	wrong []string
}

func newProbeCC(size congestion.ByteCount) *probeCC {
	return &probeCC{CongestionControlEx: newBBR(size).(congestion.CongestionControlEx), sent: map[congestion.PacketNumber]bool{}, first: -1, last: -1}
}

func (p *probeCC) OnPacketSent(t monotime.Time, inFlight congestion.ByteCount, pn congestion.PacketNumber, n congestion.ByteCount, ack bool) {
	p.mu.Lock()
	if pn <= p.last {
		p.wrong = append(p.wrong, fmt.Sprintf("sent packet %d after %d", pn, p.last))
	}
	if p.first < 0 {
		p.first = pn
	}
	p.last = max(p.last, pn)
	p.sent[pn] = true
	p.mu.Unlock()
	p.CongestionControlEx.OnPacketSent(t, inFlight, pn, n, ack)
}

func (p *probeCC) OnCongestionEventEx(inFlight congestion.ByteCount, t monotime.Time, acked []congestion.AckedPacketInfo, lost []congestion.LostPacketInfo) {
	p.mu.Lock()
	for _, a := range acked {
		if p.first >= 0 && a.PacketNumber >= p.first && !p.sent[a.PacketNumber] {
			p.wrong = append(p.wrong, fmt.Sprintf("acknowledged packet %d that it was never told was sent", a.PacketNumber))
		}
	}
	for _, l := range lost {
		if p.first >= 0 && l.PacketNumber >= p.first && !p.sent[l.PacketNumber] {
			p.wrong = append(p.wrong, fmt.Sprintf("lost packet %d that it was never told was sent", l.PacketNumber))
		}
	}
	p.mu.Unlock()
	p.CongestionControlEx.OnCongestionEventEx(inFlight, t, acked, lost)
}

// The Initial and Handshake spaces number their packets from 0 apart from the 1-RTT space, so a controller indexing packets
// by number must only hear about the 1-RTT ones (controllerSees in internal/quicgo/internal/ackhandler, generated from
// PATCH). A connection from DialAddrEarly lets the controller in right after the handshake, when the Handshake space may
// still have an acknowledgment to deliver: it must never see a packet number go down or acknowledge one it was not told of.
func TestAControllerOnlyHearsAboutApplicationDataPackets(t *testing.T) {
	checkGoroutines(t)
	l := listen(t, &Config{MaxConns: 64, MaxDialed: 64})
	cfg, err := (&Config{}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		ctx := testCtx(t)
		qc, err := quic.DialAddrEarly(ctx, l.Addr().String(), clientTLS(), cfg.quic())
		if err != nil {
			t.Fatal(err)
		}
		var probe *probeCC
		qc.SetCongestionControlFactory(func(size congestion.ByteCount) congestion.CongestionControl {
			probe = newProbeCC(size)
			return probe
		})
		server := accept(t, l)
		st, err := qc.OpenStreamSync(ctx)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			st.Write(make([]byte, 100<<10))
			st.Close()
		}()
		rx, err := server.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if n, err := io.Copy(io.Discard, rx); err != nil || n != 100<<10 {
			t.Fatalf("read %d bytes: %v", n, err)
		}
		<-qc.HandshakeComplete()
		probe.mu.Lock()
		wrong := probe.wrong
		probe.mu.Unlock()
		if len(wrong) != 0 {
			t.Fatalf("round %d: %v", i, wrong)
		}
		qc.CloseWithError(0, "")
		server.Close()
	}
}
