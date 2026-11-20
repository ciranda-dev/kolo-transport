package kolotransport

import (
	"context"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciranda-dev/kolo-transport/internal/netsim"
)

// statsLink dials a listener through a netsim link and returns both ends of the connection and the link.
func statsLink(t *testing.T, cfg *Config, p netsim.Profile) (client, server *Conn, link *netsim.Link) {
	t.Helper()
	l := listen(t, cfg)
	up, down := p, p
	up.Seed, down.Seed = 1, 2
	link, err := netsim.New(l.Addr().String(), up, down)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { link.Close() })
	client = dial(t, link.Addr(), cfg)
	server = accept(t, l)
	return client, server, link
}

// send writes n bytes on a new stream of from and reads them on to, or fails the test.
func send(t *testing.T, from, to *Conn, n int) {
	t.Helper()
	ctx := testCtx(t)
	st, err := from.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() {
		buf := make([]byte, 32<<10)
		for sent := 0; sent < n; {
			m, err := st.Write(buf[:min(len(buf), n-sent)])
			if err != nil {
				errc <- err
				return
			}
			sent += m
		}
		errc <- st.Close()
	}()
	rx, err := to.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.Copy(io.Discard, rx); err != nil || got != int64(n) {
		t.Fatalf("received %d of %d bytes: %v", got, n, err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
}

func TestStatsOnADelayedLink(t *testing.T) {
	checkGoroutines(t)
	const size = 2 << 20
	delay := 30 * time.Millisecond
	client, server, _ := statsLink(t, &Config{CongestionControl: "reno"}, netsim.Profile{Delay: delay})
	send(t, client, server, size)

	for name, c := range map[string]*Conn{"client": client, "server": server} {
		s := c.Stats()
		t.Logf("%s: %+v", name, s)
		// The lower bound is the link's; the upper ones leave room for a busy machine that stalls a sample.
		if s.MinRTT < 2*delay || s.MinRTT > 2*delay+150*time.Millisecond {
			t.Errorf("%s: MinRTT %v, want about %v", name, s.MinRTT, 2*delay)
		}
		if s.SmoothedRTT < s.MinRTT || s.SmoothedRTT > 2*delay+500*time.Millisecond {
			t.Errorf("%s: SmoothedRTT %v with MinRTT %v", name, s.SmoothedRTT, s.MinRTT)
		}
		if s.LatestRTT < s.MinRTT {
			t.Errorf("%s: LatestRTT %v is below MinRTT %v", name, s.LatestRTT, s.MinRTT)
		}
		if s.RTTVariation < 0 {
			t.Errorf("%s: RTTVariation %v", name, s.RTTVariation)
		}
		if s.CongestionWindow == 0 {
			t.Errorf("%s: no congestion window", name)
		}
		if s.CongestionController != "reno" {
			t.Errorf("%s: controller %q", name, s.CongestionController)
		}
		if s.MaxDatagramSize < 1200 || s.MaxDatagramSize > 1452 {
			t.Errorf("%s: MaxDatagramSize %d", name, s.MaxDatagramSize)
		}
		if s.PathChanges != 0 || s.BandwidthEstimate != 0 {
			t.Errorf("%s: PathChanges %d, BandwidthEstimate %d", name, s.PathChanges, s.BandwidthEstimate)
		}
		// A late acknowledgment makes quic-go declare a packet lost, so a stalled machine can count a few on a link
		// without loss; counting a tenth of the packets would be a bug.
		if s.PacketsLost*10 > s.PacketsSent {
			t.Errorf("%s: lost %d of %d packets on a link without loss", name, s.PacketsLost, s.PacketsSent)
		}
	}
	c, s := client.Stats(), server.Stats()
	if c.BytesSent < size || s.BytesReceived < size {
		t.Errorf("client sent %d bytes and the server received %d, want at least %d", c.BytesSent, s.BytesReceived, size)
	}
	if c.PacketsSent < size/1452 || s.PacketsReceived < size/1452 {
		t.Errorf("client sent %d packets and the server received %d", c.PacketsSent, s.PacketsReceived)
	}
	if c.BytesReceived == 0 || s.BytesSent == 0 {
		t.Errorf("the acknowledgments are missing: client received %d bytes, server sent %d", c.BytesReceived, s.BytesSent)
	}
	waitFor(t, "no bytes in flight", func() bool { return client.Stats().BytesInFlight == 0 })
}

func TestStatsCountLoss(t *testing.T) {
	checkGoroutines(t)
	client, server, _ := statsLink(t, &Config{}, netsim.Profile{Delay: 10 * time.Millisecond, Loss: 0.03})
	send(t, client, server, 2<<20)
	for name, c := range map[string]*Conn{"client": client, "server": server} {
		s := c.Stats()
		t.Logf("%s: sent %d packets, lost %d (%d bytes)", name, s.PacketsSent, s.PacketsLost, s.BytesLost)
	}
	s := client.Stats()
	if s.PacketsLost == 0 || s.BytesLost == 0 {
		t.Fatalf("the sender counted no loss on a link that drops 3%% of its packets: %+v", s)
	}
	if s.PacketsLost >= s.PacketsSent || s.BytesLost >= s.BytesSent {
		t.Fatalf("lost %d of %d packets, %d of %d bytes", s.PacketsLost, s.PacketsSent, s.BytesLost, s.BytesSent)
	}
}

func TestStatsNameTheController(t *testing.T) {
	checkGoroutines(t)
	for _, name := range []string{"reno", "cubic", "bbr"} {
		t.Run(name, func(t *testing.T) {
			rate := 20 * mbps
			client, server, _ := statsLink(t, &Config{CongestionControl: name}, netsim.Profile{Rate: rate, Delay: 20 * time.Millisecond})
			send(t, client, server, 4<<20)
			s := client.Stats()
			t.Logf("%s: window %d, estimate %d bytes/s", name, s.CongestionWindow, s.BandwidthEstimate)
			if s.CongestionController != name || server.Stats().CongestionController != name {
				t.Fatalf("controllers %q and %q, want %q", s.CongestionController, server.Stats().CongestionController, name)
			}
			if s.CongestionWindow == 0 {
				t.Fatal("no congestion window")
			}
			if name != "bbr" {
				if s.BandwidthEstimate != 0 {
					t.Fatalf("%s reports a bandwidth estimate of %d", name, s.BandwidthEstimate)
				}
				return
			}
			// The estimate follows what the link carried: more data until it is in range, as a slow run needs.
			inRange := func() bool {
				e := client.Stats().BandwidthEstimate
				return e >= uint64(rate)/4 && e <= 2*uint64(rate)
			}
			for i := 0; i < 5 && !inRange(); i++ {
				send(t, client, server, 4<<20)
			}
			if e := client.Stats().BandwidthEstimate; !inRange() {
				t.Fatalf("bbr estimate %d bytes/s on a %d bytes/s link", e, rate)
			}
		})
	}
}

func TestStatsShowBytesInFlightWhileSending(t *testing.T) {
	checkGoroutines(t)
	client, server, _ := statsLink(t, &Config{}, netsim.Profile{Rate: 5 * mbps, Delay: 20 * time.Millisecond})
	ctx, cancel := context.WithCancel(testCtx(t))
	defer cancel()
	st, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go sendForever(ctx, st)
	go func() {
		rx, err := server.AcceptStream(ctx)
		if err == nil {
			io.Copy(io.Discard, rx)
		}
	}()
	var peak uint64
	waitFor(t, "bytes in flight", func() bool {
		peak = max(peak, client.Stats().BytesInFlight)
		return peak > 0
	})
	client.Close()
	if s := client.Stats(); s != (ConnStats{}) {
		t.Fatalf("stats after Close: %+v", s)
	}
}

func TestStatsFollowAPathChange(t *testing.T) {
	checkGoroutines(t)
	client, server, link := statsLink(t, &Config{}, netsim.Profile{Delay: 10 * time.Millisecond})
	send(t, server, client, 256<<10)
	if n := server.Stats().PathChanges; n != 0 {
		t.Fatalf("%d path changes before the rebind", n)
	}
	link.Rebind()
	send(t, client, server, 256<<10)
	waitFor(t, "the server to see the new path", func() bool { return server.Stats().PathChanges == 1 })
	send(t, server, client, 256<<10)
	s := server.Stats()
	if s.PathChanges != 1 || s.SmoothedRTT == 0 || s.CongestionWindow == 0 {
		t.Fatalf("after one rebind: %+v", s)
	}
	if n := client.Stats().PathChanges; n != 0 {
		t.Fatalf("the client moved %d times, only its peer saw a new address", n)
	}
}

func TestStatsAreZeroAfterClose(t *testing.T) {
	checkGoroutines(t)
	client, server, _ := statsLink(t, &Config{}, netsim.Profile{Delay: 5 * time.Millisecond})
	send(t, client, server, 64<<10)
	if client.Stats() == (ConnStats{}) || server.Stats() == (ConnStats{}) {
		t.Fatal("no stats on a live connection")
	}
	client.Close()
	if s := client.Stats(); s != (ConnStats{}) {
		t.Fatalf("closed end: %+v", s)
	}
	<-server.Context().Done()
	if s := server.Stats(); s != (ConnStats{}) {
		t.Fatalf("end the peer closed: %+v", s)
	}
}

// Stats is read from many goroutines while the connection sends, receives and migrates: run with -race.
func TestStatsAreSafeFromAnyGoroutine(t *testing.T) {
	checkGoroutines(t)
	for _, cc := range []string{"reno", "bbr"} {
		t.Run(cc, func(t *testing.T) {
			client, server, link := statsLink(t, &Config{CongestionControl: cc}, netsim.Profile{Rate: 10 * mbps, Delay: 10 * time.Millisecond, Loss: 0.01})
			var stop atomic.Bool
			var wg sync.WaitGroup
			for _, c := range []*Conn{client, server, client, server} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for !stop.Load() {
						c.Stats()
						runtime.Gosched()
					}
				}()
			}
			go func() {
				time.Sleep(200 * time.Millisecond)
				link.Rebind()
			}()
			send(t, client, server, 1<<20)
			stop.Store(true)
			wg.Wait()
		})
	}
}
