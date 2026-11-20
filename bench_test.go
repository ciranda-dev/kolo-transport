package kolotransport

import (
	"context"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// unreadHeapPerByte sends 64 KiB in chunk-byte writes, pausing after each, on streams the server never reads.
func unreadHeapPerByte(b *testing.B, streams, chunk int, pause time.Duration) float64 {
	b.Helper()
	ctx := context.Background()
	l, err := Listen(ctx, "127.0.0.1:0", &Config{MaxIncomingStreams: streams})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	c, err := Dial(ctx, l.Addr().String(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	if _, err := l.Accept(ctx); err != nil {
		b.Fatal(err)
	}
	const total = 64 << 10
	var opened []*Stream
	for range streams {
		s, err := c.OpenStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := s.Write([]byte{1}); err != nil {
			b.Fatal(err)
		}
		opened = append(opened, s)
	}
	time.Sleep(200 * time.Millisecond)
	before := heapAlloc()
	var wg sync.WaitGroup
	errs := make(chan error, streams)
	for _, s := range opened {
		wg.Go(func() {
			buf := make([]byte, chunk)
			for left := total/streams - 1; left > 0; left -= len(buf) {
				buf = buf[:min(chunk, left)]
				if _, err := s.Write(buf); err != nil {
					errs <- err
					return
				}
				time.Sleep(pause)
			}
		})
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		b.Fatal(err)
	}
	time.Sleep(time.Second)
	return float64(int64(heapAlloc())-int64(before)) / float64(total-streams)
}

func BenchmarkUnreadHeapPerByte(b *testing.B) {
	for _, bc := range []struct {
		name           string
		streams, chunk int
		pause          time.Duration
	}{
		{"one-1-byte-frame-per-packet", 1, 1, 200 * time.Microsecond},
		{"many-1-byte-frames-per-packet", 64, 1, 200 * time.Microsecond},
		{"64KiB-write", 1, 64 << 10, 0},
	} {
		b.Run(bc.name, func(b *testing.B) {
			var ratio float64
			for range b.N {
				ratio = unreadHeapPerByte(b, bc.streams, bc.chunk, bc.pause)
			}
			b.ReportMetric(ratio, "heap-B/unread-B")
		})
	}
}

// retainedHeap fills streams one after another with size bytes of 1-byte frames, reading each back fully.
func retainedHeap(b *testing.B, streams, size int) (fill, firstRetained, retained, closed float64) {
	b.Helper()
	ctx := context.Background()
	l, err := Listen(ctx, "127.0.0.1:0", &Config{MaxIncomingStreams: streams})
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	c, err := Dial(ctx, l.Addr().String(), nil)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	server, err := l.Accept(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, size)
	var open []*Stream
	before := heapAlloc()
	for i := range streams {
		cs, err := c.OpenStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for range size {
			if _, err := cs.Write([]byte{1}); err != nil {
				b.Fatal(err)
			}
			time.Sleep(200 * time.Microsecond)
		}
		time.Sleep(time.Second)
		if i == 0 {
			fill = float64(int64(heapAlloc())-int64(before)) / float64(size)
		}
		ss, err := server.AcceptStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(ss, buf); err != nil {
			b.Fatal(err)
		}
		open = append(open, cs, ss)
		time.Sleep(time.Second)
		retained = float64(int64(heapAlloc())-int64(before)) / float64(size)
		if i == 0 {
			firstRetained = retained
		}
	}
	for _, s := range open {
		s.Close()
	}
	for _, s := range open {
		if _, err := io.ReadAll(s); err != nil {
			b.Fatal(err)
		}
	}
	time.Sleep(time.Second)
	closed = float64(int64(heapAlloc())-int64(before)) / float64(size)
	return fill, firstRetained, retained, closed
}

func BenchmarkRetainedHeapAfterRead(b *testing.B) {
	for _, bc := range []struct {
		name          string
		streams, size int
	}{
		{"64KiB", 1, 64 << 10},
		{"256KiB", 1, 256 << 10},
		{"8-streams-16KiB", 8, 16 << 10},
	} {
		b.Run(bc.name, func(b *testing.B) {
			var fill, first, all, closed float64
			for range b.N {
				fill, first, all, closed = retainedHeap(b, bc.streams, bc.size)
			}
			b.ReportMetric(fill, "fill-heap-B/B")
			b.ReportMetric(first, "retained-B/B-1st")
			b.ReportMetric(all, "retained-B/B-all")
			b.ReportMetric(closed, "closed-B/B-all")
		})
	}
}
