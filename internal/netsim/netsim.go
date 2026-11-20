// Package netsim is a test helper: a UDP forwarder on 127.0.0.1 that makes the path between two peers behave like a
// slow, lossy mobile link. Each direction has a bandwidth limit with a bounded queue, a one-way delay with jitter,
// random loss and an MTU cap, and the whole link can be cut for a while. It never touches a non-loopback address.
package netsim

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ipUDPHeader is the IPv4 and UDP header the MTU counts besides the payload.
const ipUDPHeader = 28

// Profile is how one direction of the link behaves.
type Profile struct {
	// Rate is the bandwidth in bytes per second; 0 is unlimited.
	Rate int
	// Queue is the bytes the bottleneck holds before it drops the newest packet; default 100 ms of Rate, at least 16 KiB.
	Queue int
	// Delay is the one-way delay; Jitter is added up to this much, never reordering packets.
	Delay, Jitter time.Duration
	// Loss is the chance, 0 to 1, that a packet is dropped.
	Loss float64
	// MTU drops every datagram whose IP packet (payload plus 28 bytes) is longer; 0 is no cap.
	MTU int
	// Seed seeds the loss and the jitter, so a run drops the same packets of the same sequence.
	Seed int64
}

// Stats counts one direction.
type Stats struct {
	Packets, Bytes                    uint64 // delivered
	DroppedMTU, DroppedLoss, DroppedQ uint64
	DroppedOutage, DroppedNAT         uint64
	MaxQueueDelay                     time.Duration
}

// Link forwards the datagrams of one client to a target and back.
type Link struct {
	front  *net.UDPConn // the address the client dials; RebindFront replaces it
	target *net.UDPAddr
	up     *dir // client to target
	down   *dir // target to client
	closed atomic.Bool
	done   chan struct{}
	wg     sync.WaitGroup

	mu      sync.Mutex
	dead    []*net.UDPConn // old front sockets, closed with the link
	back    *net.UDPConn   // the socket that talks to the target: its port is the client's address as the target sees it
	client  *net.UDPAddr
	cutTil  time.Time
	natIdle time.Duration
	lastOut time.Time
	rebinds int
}

type dir struct {
	l          *Link
	p          Profile
	mu         sync.Mutex // guards rng, freeAt and lastAt: after a rebind two sockets feed the same direction
	rng        *rand.Rand
	out        func([]byte) // writes to the destination
	queue      chan item
	freeAt     time.Time
	lastAt     time.Time
	pk, by     atomic.Uint64
	dMTU       atomic.Uint64
	dLoss      atomic.Uint64
	dQ         atomic.Uint64
	dOutage    atomic.Uint64
	dNAT       atomic.Uint64
	mtu        atomic.Int64 // Profile.MTU, changed by SetMTU
	fromClient bool
	maxQns     atomic.Int64
}

type item struct {
	at   time.Time
	data []byte
}

// New starts a link whose client side listens on a free 127.0.0.1 port and forwards to target, a 127.0.0.1 address.
// up shapes what the client sends, down what the target sends.
func New(target string, up, down Profile) (*Link, error) {
	ta, err := net.ResolveUDPAddr("udp4", target)
	if err != nil {
		return nil, err
	}
	if !ta.IP.IsLoopback() {
		return nil, fmt.Errorf("netsim: %v is not a loopback address", target)
	}
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	back, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		front.Close()
		return nil, err
	}
	for _, c := range []*net.UDPConn{front, back} {
		c.SetReadBuffer(4 << 20)
		c.SetWriteBuffer(4 << 20)
	}
	l := &Link{front: front, back: back, target: ta, lastOut: time.Now(), done: make(chan struct{})}
	l.up = l.newDir(up, func(b []byte) {
		l.mu.Lock()
		c := l.back
		l.mu.Unlock()
		c.WriteToUDP(b, ta)
	})
	l.up.fromClient = true
	l.down = l.newDir(down, func(b []byte) {
		l.mu.Lock()
		c := l.client
		l.mu.Unlock()
		l.mu.Lock()
		f := l.front
		l.mu.Unlock()
		if c != nil {
			f.WriteToUDP(b, c)
		}
	})
	l.wg.Add(4)
	go l.read(front, l.up, true)
	go l.read(back, l.down, false)
	go l.up.deliver()
	go l.down.deliver()
	return l, nil
}

func (l *Link) newDir(p Profile, out func([]byte)) *dir {
	if p.Rate > 0 && p.Queue == 0 {
		p.Queue = max(16<<10, p.Rate/10)
	}
	d := &dir{l: l, p: p, rng: rand.New(rand.NewSource(p.Seed)), out: out, queue: make(chan item, 1<<16)}
	d.mtu.Store(int64(p.MTU))
	return d
}

// SetMTU changes the MTU of both directions from now on; 0 removes the cap. A cap below 1228 bytes drops every QUIC packet.
func (l *Link) SetMTU(n int) {
	l.up.mtu.Store(int64(n))
	l.down.mtu.Store(int64(n))
}

// Addr is the address the client dials.
func (l *Link) Addr() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.front.LocalAddr().String()
}

// Cut drops everything in both directions for d.
func (l *Link) Cut(d time.Duration) {
	l.mu.Lock()
	l.cutTil = time.Now().Add(d)
	l.mu.Unlock()
}

func (l *Link) cut(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return now.Before(l.cutTil)
}

// SetNATIdle makes the link a NAT whose mapping lasts idle after the client's last outgoing datagram: later datagrams
// from the target are dropped, and the next one from the client gets a new mapping, so the target sees a new source port.
func (l *Link) SetNATIdle(idle time.Duration) {
	l.mu.Lock()
	l.natIdle = idle
	l.lastOut = time.Now()
	l.mu.Unlock()
}

// Rebind gives the client a new mapping now, as a carrier that changes the source port does: the target sees the
// client's datagrams come from a new port, and what it sends to the old one is lost.
func (l *Link) Rebind() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rebindLocked()
}

func (l *Link) rebindLocked() {
	if l.closed.Load() {
		return
	}
	nb, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return
	}
	nb.SetReadBuffer(4 << 20)
	nb.SetWriteBuffer(4 << 20)
	l.back.Close()
	l.back = nb
	l.rebinds++
	l.wg.Add(1)
	go l.read(nb, l.down, false)
}

// RebindFront moves the target's side of the link to a new address, as a NAT in front of the target does when it
// changes the port: the client's datagrams to the old address are lost, and what the target sends reaches the client
// from the new one.
func (l *Link) RebindFront() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed.Load() {
		return
	}
	nf, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return
	}
	nf.SetReadBuffer(4 << 20)
	nf.SetWriteBuffer(4 << 20)
	// The old socket stays open and its datagrams are not forwarded: the old mapping is dead, not refused.
	l.dead = append(l.dead, l.front)
	l.front = nf
	l.wg.Add(1)
	go l.read(nf, l.up, true)
}

// Rebinds is how many times the client's mapping changed.
func (l *Link) Rebinds() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rebinds
}

// nat applies the mapping timeout to a datagram at now: it reports false when the datagram is lost to an expired mapping.
func (l *Link) nat(now time.Time, fromClient bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.natIdle <= 0 {
		return true
	}
	expired := now.Sub(l.lastOut) > l.natIdle
	if fromClient {
		if expired {
			l.rebindLocked()
		}
		l.lastOut = now
		return true
	}
	return !expired
}

// Up returns the counters of the client-to-target direction.
func (l *Link) Up() Stats { return l.up.stats() }

// Down returns the counters of the target-to-client direction.
func (l *Link) Down() Stats { return l.down.stats() }

func (d *dir) stats() Stats {
	return Stats{Packets: d.pk.Load(), Bytes: d.by.Load(), DroppedMTU: d.dMTU.Load(), DroppedLoss: d.dLoss.Load(), DroppedQ: d.dQ.Load(),
		DroppedOutage: d.dOutage.Load(), DroppedNAT: d.dNAT.Load(), MaxQueueDelay: time.Duration(d.maxQns.Load())}
}

// Close stops the link.
func (l *Link) Close() error {
	if l.closed.Swap(true) {
		return nil
	}
	l.mu.Lock()
	l.front.Close()
	l.back.Close()
	for _, c := range l.dead {
		c.Close()
	}
	l.mu.Unlock()
	close(l.done)
	l.wg.Wait()
	return nil
}

func (l *Link) read(c *net.UDPConn, d *dir, fromClient bool) {
	defer l.wg.Done()
	buf := make([]byte, 65536)
	for {
		n, from, err := c.ReadFromUDP(buf)
		if err != nil {
			if l.closed.Load() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		if fromClient {
			l.mu.Lock()
			current := c == l.front
			if current {
				l.client = from
			}
			l.mu.Unlock()
			if !current {
				continue
			}
		}
		d.accept(append([]byte(nil), buf[:n]...))
	}
}

// accept applies the impairments of the direction to one datagram and queues it for delivery.
func (d *dir) accept(b []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	p := d.p
	switch {
	case d.mtu.Load() > 0 && int64(len(b)+ipUDPHeader) > d.mtu.Load():
		d.dMTU.Add(1)
		return
	case !d.l.nat(now, d.fromClient):
		d.dNAT.Add(1)
		return
	case d.l.cut(now):
		d.dOutage.Add(1)
		return
	case p.Loss > 0 && d.rng.Float64() < p.Loss:
		d.dLoss.Add(1)
		return
	}
	depart := now
	if p.Rate > 0 {
		if d.freeAt.After(now) {
			depart = d.freeAt
		}
		if int(depart.Sub(now).Seconds()*float64(p.Rate)) > p.Queue {
			d.dQ.Add(1)
			return
		}
		depart = depart.Add(time.Duration(float64(len(b)) / float64(p.Rate) * float64(time.Second)))
		d.freeAt = depart
		if q := depart.Sub(now); q > time.Duration(d.maxQns.Load()) {
			d.maxQns.Store(int64(q))
		}
	}
	at := depart.Add(p.Delay)
	if p.Jitter > 0 {
		at = at.Add(time.Duration(d.rng.Int63n(int64(p.Jitter) + 1)))
	}
	if at.Before(d.lastAt) {
		at = d.lastAt
	}
	d.lastAt = at
	select {
	case d.queue <- item{at, b}:
	default:
	}
}

func (d *dir) deliver() {
	defer d.l.wg.Done()
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for {
		var it item
		select {
		case it = <-d.queue:
		case <-d.l.done:
			return
		}
		if w := time.Until(it.at); w > 0 {
			t.Reset(w)
			select {
			case <-t.C:
			case <-d.l.done:
				return
			}
		}
		d.pk.Add(1)
		d.by.Add(uint64(len(it.data)))
		d.out(it.data)
	}
}
