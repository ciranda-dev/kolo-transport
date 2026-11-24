package kolotransport

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

func TestConfigDefaults(t *testing.T) {
	c, err := (*Config)(nil).listenConfig()
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		MaxConns:               100,
		MaxConnsPerIP:          4,
		MaxDialed:              32,
		MaxIncomingStreams:     16,
		HandshakeTimeout:       10 * time.Second,
		IdleTimeout:            30 * time.Second,
		KeepAlivePeriod:        5 * time.Second,
		MaxMemory:              1 << 30,
		MaxStreamReceiveWindow: 256 << 10,
		MaxConnReceiveWindow:   512 << 10,
		Network:                "udp4",
		CongestionControl:      defaultCongestionControl,
	}
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("got %+v, want %+v", c, want)
	}
	q := c.quic()
	if q.Allow0RTT || q.EnableDatagrams {
		t.Fatal("0-RTT or datagrams enabled")
	}
	if len(q.Versions) != 1 || q.Versions[0] != quic.Version1 {
		t.Fatalf("versions %v", q.Versions)
	}
	if q.HandshakeIdleTimeout != 5*time.Second || q.MaxIdleTimeout != 30*time.Second || q.KeepAlivePeriod != 5*time.Second {
		t.Fatalf("timeouts %v %v %v", q.HandshakeIdleTimeout, q.MaxIdleTimeout, q.KeepAlivePeriod)
	}
	if q.MaxIncomingStreams != 16 || q.MaxIncomingUniStreams != 16 {
		t.Fatalf("streams %d %d", q.MaxIncomingStreams, q.MaxIncomingUniStreams)
	}
	if q.InitialStreamReceiveWindow != 256<<10 || q.MaxStreamReceiveWindow != 256<<10 ||
		q.InitialConnectionReceiveWindow != 512<<10 || q.MaxConnectionReceiveWindow != 512<<10 {
		t.Fatalf("windows %+v", q)
	}
	if DefaultMaxStreamReceiveWindow != 256<<10 || DefaultMaxConnReceiveWindow != 512<<10 || DefaultMaxMemory != 1<<30 ||
		DefaultMaxConns != 100 || DefaultMaxDialed != 32 || DatagramQueueBytes != 240<<10 {
		t.Fatal("default constants changed")
	}
	d, err := (*Config)(nil).withDefaults()
	if err != nil || d.MaxStreamReceiveWindow != 256<<10 || d.MaxConnReceiveWindow != 512<<10 ||
		d.KeepAlivePeriod != 5*time.Second {
		t.Fatalf("dial defaults %+v, %v", d, err)
	}
}

func TestConfigKeepsValidValues(t *testing.T) {
	for _, in := range []Config{
		{
			MaxConns:           20000,
			MaxConnsPerIP:      20000,
			MaxDialed:          20000,
			MaxMemory:          64 << 30,
			MaxIncomingStreams: 1,
			HandshakeTimeout:   time.Second,
			IdleTimeout:        10 * time.Minute,
			KeepAlivePeriod:    10*time.Minute - 1,

			MaxStreamReceiveWindow: 64 << 10,
			MaxConnReceiveWindow:   64 << 10,
			Network:                "udp4",
			CongestionControl:      "bbr",
			Datagrams:              true,
		},
		{
			MaxConns:           1,
			MaxConnsPerIP:      1,
			MaxDialed:          1,
			MaxMemory:          128 << 20,
			MaxIncomingStreams: 1000,
			HandshakeTimeout:   time.Minute,
			IdleTimeout:        2 * time.Second,
			KeepAlivePeriod:    time.Second,

			MaxStreamReceiveWindow: 16 << 20,
			MaxConnReceiveWindow:   64 << 20,
			Network:                "udp4",
			CongestionControl:      "reno",
		},
		{IdleTimeout: time.Second},
	} {
		got, err := in.listenConfig()
		if err != nil {
			t.Fatalf("%+v: %v", in, err)
		}
		if in.MaxConns != 0 && !reflect.DeepEqual(got, in) {
			t.Fatalf("got %+v, want %+v", got, in)
		}
		q := got.quic()
		if q.KeepAlivePeriod != got.KeepAlivePeriod || q.MaxIncomingUniStreams != int64(got.MaxIncomingStreams) ||
			q.InitialStreamReceiveWindow != uint64(got.MaxStreamReceiveWindow) || q.MaxStreamReceiveWindow != uint64(got.MaxStreamReceiveWindow) ||
			q.InitialConnectionReceiveWindow != uint64(got.MaxConnReceiveWindow) || q.MaxConnectionReceiveWindow != uint64(got.MaxConnReceiveWindow) ||
			q.EnableDatagrams != in.Datagrams {
			t.Fatalf("quic config %+v", got.quic())
		}
	}
	if got, _ := (&Config{MaxConns: 4}).listenConfig(); got.MaxConnsPerIP != 4 {
		t.Fatalf("per-IP default with MaxConns 4: %d", got.MaxConnsPerIP)
	}
}

func TestConfigRejectsOutOfRange(t *testing.T) {
	for name, c := range map[string]Config{
		"negative conns":        {MaxConns: -1},
		"conns over memory":     {MaxConns: 2049},
		"conn window memory":    {MaxConns: 100, MaxConnReceiveWindow: 16 << 20},
		"tiny stream window":    {MaxStreamReceiveWindow: 64<<10 - 1},
		"huge stream window":    {MaxStreamReceiveWindow: 16<<20 + 1, MaxConnReceiveWindow: 64 << 20},
		"negative stream win":   {MaxStreamReceiveWindow: -1},
		"tiny conn window":      {MaxConnReceiveWindow: 64<<10 - 1},
		"huge conn window":      {MaxConns: 1, MaxConnReceiveWindow: 64<<20 + 1},
		"negative conn window":  {MaxConnReceiveWindow: -1},
		"stream above conn":     {MaxStreamReceiveWindow: 1 << 20, MaxConnReceiveWindow: 512 << 10},
		"stream above default":  {MaxStreamReceiveWindow: 1 << 20},
		"small memory":          {MaxMemory: 64<<20 - 1},
		"negative memory":       {MaxMemory: -1},
		"conns over set memory": {MaxConns: 129, MaxMemory: 64 << 20},
		"dialed over memory":    {MaxConns: 97, MaxMemory: 64 << 20},
		"negative dialed":       {MaxDialed: -1},
		"too many dialed":       {MaxDialed: 10001},
		"negative streams":      {MaxIncomingStreams: -1},
		"too many streams":      {MaxIncomingStreams: 1001},
		"negative handshake":    {HandshakeTimeout: -time.Second},
		"tiny handshake":        {HandshakeTimeout: time.Second - 1},
		"long handshake":        {HandshakeTimeout: time.Minute + 1},
		"negative per IP":       {MaxConnsPerIP: -1},
		"per IP above total":    {MaxConns: 4, MaxConnsPerIP: 5},
		"long idle":             {IdleTimeout: 11 * time.Minute},
		"negative idle":         {IdleTimeout: -1},
		"tiny idle":             {IdleTimeout: time.Second - 1},
		"negative keep-alive":   {KeepAlivePeriod: -time.Second},
		"short keep-alive":      {KeepAlivePeriod: 500*time.Millisecond - 1},
		"keep-alive at idle":    {IdleTimeout: 2 * time.Second, KeepAlivePeriod: 2 * time.Second},
		"keep-alive above idle": {KeepAlivePeriod: time.Minute},
		"keep-alive, min idle":  {IdleTimeout: time.Second, KeepAlivePeriod: time.Second},
	} {
		if _, err := c.listenConfig(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestStreamWindowFollowsSmallConnWindow(t *testing.T) {
	got, err := (&Config{MaxConnReceiveWindow: 64 << 10}).withDefaults()
	if err != nil || got.MaxStreamReceiveWindow != 64<<10 || got.MaxConnReceiveWindow != 64<<10 {
		t.Fatalf("got %+v, %v", got, err)
	}
	got, err = (&Config{MaxConnReceiveWindow: 128 << 10}).withDefaults()
	if err != nil || got.MaxStreamReceiveWindow != 128<<10 {
		t.Fatalf("got %+v, %v", got, err)
	}
	got, err = (&Config{MaxConnReceiveWindow: 1 << 20}).withDefaults()
	if err != nil || got.MaxStreamReceiveWindow != DefaultMaxStreamReceiveWindow {
		t.Fatalf("large conn window: %+v, %v", got, err)
	}
	if _, err := (&Config{MaxStreamReceiveWindow: 1 << 20}).withDefaults(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("conn window raised implicitly: %v", err)
	}
}

func TestMaxMemory(t *testing.T) {
	for _, tc := range []struct {
		cfg  Config
		want string
	}{
		{Config{MaxConns: 1000, MaxMemory: 256 << 20},
			"kolotransport: invalid config: (MaxConns 1000 + MaxDialed 32) × MaxConnReceiveWindow 512 KiB needs 516 MiB of receive window, above MaxMemory 256 MiB; raise MaxMemory or lower MaxConns, MaxDialed or MaxConnReceiveWindow"},
		{Config{MaxConns: 100, MaxConnReceiveWindow: 16 << 20},
			"kolotransport: invalid config: (MaxConns 100 + MaxDialed 32) × MaxConnReceiveWindow 16 MiB needs 2112 MiB of receive window, above MaxMemory 1024 MiB; raise MaxMemory or lower MaxConns, MaxDialed or MaxConnReceiveWindow"},
		{Config{MaxConns: 3, MaxDialed: 1, MaxConnReceiveWindow: 30<<20 + 1, MaxMemory: 64 << 20},
			"kolotransport: invalid config: (MaxConns 3 + MaxDialed 1) × MaxConnReceiveWindow 31457281 bytes needs 125829124 bytes of receive window, above MaxMemory 64 MiB; raise MaxMemory or lower MaxConns, MaxDialed or MaxConnReceiveWindow"},
	} {
		_, err := tc.cfg.listenConfig()
		if !errors.Is(err, ErrInvalidConfig) || err.Error() != tc.want {
			t.Errorf("got %v, want %q", err, tc.want)
		}
	}
	got, err := (&Config{MaxConns: 100, MaxConnReceiveWindow: 16 << 20, MaxMemory: 3 << 30}).listenConfig()
	if err != nil || got.MaxMemory != 3<<30 {
		t.Fatalf("got %+v, %v", got, err)
	}
	if got, err := (&Config{MaxConns: 2016}).listenConfig(); err != nil || got.MaxMemory != DefaultMaxMemory {
		t.Fatalf("2016 conns and 32 dialed in the default budget: %+v, %v", got, err)
	}
	if _, err := (&Config{MaxConns: 96, MaxMemory: 64 << 20}).listenConfig(); err != nil {
		t.Fatalf("96 + 32 conns in 64 MiB: %v", err)
	}
}

func TestMaxMemoryCountsDatagramQueues(t *testing.T) {
	// 96 + 32 conns of 512 KiB fill 64 MiB, so the datagram queues of 240 KiB each need 30 MiB more.
	base := Config{MaxConns: 96, MaxMemory: 64 << 20}
	if _, err := base.listenConfig(); err != nil {
		t.Fatalf("without datagrams: %v", err)
	}
	base.Datagrams = true
	_, err := base.listenConfig()
	const want = "kolotransport: invalid config: (MaxConns 96 + MaxDialed 32) × MaxConnReceiveWindow 512 KiB plus datagram queues of 30 MiB needs 94 MiB of memory, above MaxMemory 64 MiB; raise MaxMemory, lower MaxConns, MaxDialed or MaxConnReceiveWindow, or turn off Datagrams"
	if !errors.Is(err, ErrInvalidConfig) || err.Error() != want {
		t.Fatalf("got %v", err)
	}
	base.MaxMemory = 94<<20 - 1
	if _, err := base.listenConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("one byte short: %v", err)
	}
	base.MaxMemory = 94 << 20
	if _, err := base.listenConfig(); err != nil {
		t.Fatalf("exact fit: %v", err)
	}
	grown := Config{MaxConns: 96, MaxMemory: 94 << 20, Datagrams: true, WindowGrowth: true}
	if _, err := grown.listenConfig(); !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "plus window growth of up to 64 MiB plus datagram queues of 30 MiB") {
		t.Fatalf("with growth: %v", err)
	}
}

func TestCountsHaveNoRangeMaximum(t *testing.T) {
	got, err := (&Config{MaxConns: 50000, MaxDialed: 20000, MaxConnReceiveWindow: 64 << 10, MaxMemory: 8 << 30}).listenConfig()
	if err != nil || got.MaxConns != 50000 || got.MaxDialed != 20000 {
		t.Fatalf("got %+v, %v", got, err)
	}
	// The sum of two counts near math.MaxInt must not wrap around the memory check.
	_, err = (&Config{MaxConns: math.MaxInt, MaxDialed: math.MaxInt}).listenConfig()
	if !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "above MaxMemory 1024 MiB") {
		t.Fatalf("got %v", err)
	}
	for _, c := range []Config{{MaxConns: -1}, {MaxDialed: -1}} {
		if _, err := c.listenConfig(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%+v: got %v", c, err)
		}
	}
}

func TestMemoryHasNoRangeMaximum(t *testing.T) {
	for _, c := range []Config{
		{MaxMemory: 64<<30 + 1},
		{MaxMemory: math.MaxInt64},
		{MaxMemory: 2 << 30, WindowGrowth: true, MaxGrownMemory: 64<<30 + 1},
		{MaxMemory: math.MaxInt64, WindowGrowth: true, MaxGrownMemory: math.MaxInt64, Datagrams: true},
	} {
		got, err := c.listenConfig()
		if err != nil || got.MaxMemory != c.MaxMemory || got.MaxGrownMemory != c.MaxGrownMemory {
			t.Errorf("%+v: %+v, %v", c, got, err)
		}
	}
	if _, err := (&Config{MaxMemory: 64<<20 - 1}).listenConfig(); err == nil || err.Error() != "kolotransport: invalid config: MaxMemory must be at least 64 MiB" {
		t.Fatalf("below 64 MiB: %v", err)
	}
}

func TestMemoryCheckIsExactAtAnySize(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("needs a 64-bit int")
	}
	shift := 33
	n := 1 << shift
	c := Config{MaxConns: n, MaxConnsPerIP: 1, MaxDialed: 1, MaxConnReceiveWindow: 64 << 10, MaxMemory: int64(n+1) * (64 << 10)}
	if _, err := c.listenConfig(); err != nil {
		t.Fatalf("exact fit of 2^33 + 1 connections: %v", err)
	}
	c.MaxMemory--
	if _, err := c.listenConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("one byte short: %v", err)
	}
	win := int64(DefaultMaxConnReceiveWindow)
	near := int(math.MaxInt64/win) - DefaultMaxDialed
	for name, c := range map[string]Config{
		"count sum":   {MaxConns: math.MaxInt, MaxDialed: math.MaxInt, MaxMemory: math.MaxInt64},
		"windows":     {MaxConns: near + 1, MaxMemory: math.MaxInt64},
		"queues":      {MaxConns: near, MaxMemory: math.MaxInt64, Datagrams: true},
		"growth term": {MaxConns: near, MaxMemory: math.MaxInt64, WindowGrowth: true, MaxGrownMemory: math.MaxInt64},
	} {
		if _, err := c.listenConfig(); !errors.Is(err, ErrInvalidConfig) || !strings.Contains(err.Error(), "int64") {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := (&Config{MaxConns: near, MaxMemory: math.MaxInt64}).listenConfig(); err != nil {
		t.Fatalf("the most connections an int64 of windows counts: %v", err)
	}
}

func TestMaxMemoryCountsDialed(t *testing.T) {
	_, err := (&Config{MaxConns: 100, MaxConnReceiveWindow: 8 << 20}).listenConfig()
	const want = "kolotransport: invalid config: (MaxConns 100 + MaxDialed 32) × MaxConnReceiveWindow 8 MiB needs 1056 MiB of receive window, above MaxMemory 1024 MiB; raise MaxMemory or lower MaxConns, MaxDialed or MaxConnReceiveWindow"
	if !errors.Is(err, ErrInvalidConfig) || err.Error() != want {
		t.Fatalf("got %v", err)
	}
	if _, err := (&Config{MaxConns: 100, MaxDialed: 1, MaxConnReceiveWindow: 8 << 20}).listenConfig(); err != nil {
		t.Fatalf("101 conns of 8 MiB: %v", err)
	}
}

func TestDialConfigSkipsListenerLimits(t *testing.T) {
	c := Config{MaxConns: 10001, MaxConnsPerIP: -1, MaxDialed: -1, MaxMemory: -1, MaxConnReceiveWindow: 64 << 20}
	if _, err := c.withDefaults(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.listenConfig(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("listener: %v", err)
	}
	for name, d := range map[string]Config{
		"keep-alive":        {KeepAlivePeriod: 500*time.Millisecond - 1},
		"stream window":     {MaxStreamReceiveWindow: 64<<10 - 1},
		"conn window":       {MaxConnReceiveWindow: 64<<20 + 1},
		"stream above conn": {MaxStreamReceiveWindow: 1 << 20},
	} {
		if _, err := d.withDefaults(); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("dial %s: %v", name, err)
		}
	}
}

func TestListenDialRejectInvalidConfig(t *testing.T) {
	checkGoroutines(t)
	bad := &Config{MaxIncomingStreams: -1}
	if _, err := Listen(context.Background(), "127.0.0.1:0", bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Listen: %v", err)
	}
	if _, err := Dial(context.Background(), "127.0.0.1:1", bad); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Dial: %v", err)
	}
}

func TestConfigNetwork(t *testing.T) {
	checkGoroutines(t)
	for _, n := range []string{"", "udp4"} {
		if c, err := (&Config{Network: n}).listenConfig(); err != nil || c.Network != "udp4" {
			t.Errorf("%q: got %q, %v", n, c.Network, err)
		}
	}
	for _, n := range []string{"udp", "udp6", "UDP4", "udp4 ", "ip4", "tcp4"} {
		cfg := &Config{Network: n}
		want := `kolotransport: invalid config: Network must be "udp4", not "` + n + `"; IPv6 is refused`
		if _, err := cfg.listenConfig(); !errors.Is(err, ErrInvalidConfig) || err.Error() != want {
			t.Errorf("%q: %v", n, err)
		}
		if l, err := Listen(testCtx(t), "127.0.0.1:0", cfg); err == nil {
			l.Close()
			t.Errorf("Listen accepted %q", n)
		} else if !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Listen %q: %v", n, err)
		}
		pc, _ := memPair()
		if l, err := ListenConn(testCtx(t), pc, cfg); err == nil {
			l.Close()
			t.Errorf("ListenConn accepted %q", n)
		} else if !errors.Is(err, ErrInvalidConfig) || !pc.closed() {
			t.Errorf("ListenConn %q: %v, pc closed %v", n, err, pc.closed())
		}
	}
	if _, err := (&Config{Network: "udp6"}).withDefaults(); err != nil {
		t.Errorf("Dial checks Network: %v", err)
	}
}

func TestKeepAliveDefault(t *testing.T) {
	for idle, want := range map[time.Duration]time.Duration{
		time.Second:      500 * time.Millisecond,
		2 * time.Second:  time.Second,
		10 * time.Second: 5 * time.Second,
		10 * time.Minute: 5 * time.Second,
	} {
		if got, err := (&Config{IdleTimeout: idle}).withDefaults(); err != nil || got.KeepAlivePeriod != want {
			t.Errorf("IdleTimeout %v: KeepAlivePeriod %v, %v; want %v", idle, got.KeepAlivePeriod, err, want)
		}
	}
	c := Config{IdleTimeout: time.Second, KeepAlivePeriod: 500 * time.Millisecond}
	if got, err := c.withDefaults(); err != nil || got.KeepAlivePeriod != 500*time.Millisecond {
		t.Errorf("500ms below a 1s IdleTimeout: %v, %v", got.KeepAlivePeriod, err)
	}
}
