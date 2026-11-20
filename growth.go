package kolotransport

import (
	"context"
	"sync"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

// growth is the budget of window memory that trusted connections share; a nil *growth never grows anything.
type growth struct {
	mu    sync.Mutex
	limit int64
	used  int64
	conns map[*quic.Conn]int64
}

func newGrowth(c Config) *growth {
	if !c.WindowGrowth {
		return nil
	}
	return &growth{limit: c.MaxGrownMemory, conns: make(map[*quic.Conn]int64)}
}

// allow is quic-go's AllowConnectionWindowIncrease: it runs under the connection's flow controller lock and calls nothing back.
func (g *growth) allow(qc *quic.Conn, delta uint64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	grown, trusted := g.conns[qc]
	if !trusted || delta > uint64(g.limit-g.used) {
		return false
	}
	g.conns[qc] = grown + int64(delta)
	g.used += int64(delta)
	return true
}

// trust lets qc grow until it ends, and reports whether it may.
func (g *growth) trust(qc *quic.Conn) bool {
	if g == nil || qc.Context().Err() != nil {
		return false
	}
	g.mu.Lock()
	if _, ok := g.conns[qc]; ok {
		g.mu.Unlock()
		return true
	}
	g.conns[qc] = 0
	g.mu.Unlock()
	// Registered before AfterFunc, so a connection that ended meanwhile releases at once and allow never sees it again.
	context.AfterFunc(qc.Context(), func() { g.release(qc) })
	return true
}

func (g *growth) release(qc *quic.Conn) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.used -= g.conns[qc]
	delete(g.conns, qc)
}
