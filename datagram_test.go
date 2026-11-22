package kolotransport

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// exchange sends p from one end until the other receives it: a datagram may be lost, even on loopback.
func exchange(t *testing.T, from, to *Conn, p []byte) {
	t.Helper()
	for range 50 {
		if err := from.SendDatagram(p); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(testCtx(t), 200*time.Millisecond)
		got, err := to.ReceiveDatagram(ctx)
		cancel()
		if err == nil {
			if !bytes.Equal(got, p) {
				t.Fatalf("received %d bytes, want %d", len(got), len(p))
			}
			return
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
	t.Fatal("datagram never arrived")
}

func TestDatagrams(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, &Config{Datagrams: true}, &Config{Datagrams: true})
	if !client.SupportsDatagrams() || !server.SupportsDatagrams() {
		t.Fatal("datagrams not supported with both ends on")
	}
	exchange(t, client, server, []byte("ping"))
	exchange(t, server, client, []byte("pong"))
	exchange(t, client, server, bytes.Repeat([]byte{7}, 1000))
}

func TestDatagramsNeedBothEnds(t *testing.T) {
	for _, tc := range []struct {
		name           string
		server, client bool
	}{{"server only", true, false}, {"client only", false, true}, {"neither", false, false}} {
		t.Run(tc.name, func(t *testing.T) {
			checkGoroutines(t)
			client, server := pair(t, &Config{Datagrams: tc.server}, &Config{Datagrams: tc.client})
			for _, c := range []*Conn{client, server} {
				if c.SupportsDatagrams() {
					t.Fatal("SupportsDatagrams is true")
				}
				if err := c.SendDatagram([]byte("x")); !errors.Is(err, ErrDatagramsUnsupported) {
					t.Fatalf("SendDatagram: %v", err)
				}
				if _, err := c.ReceiveDatagram(testCtx(t)); !errors.Is(err, ErrDatagramsUnsupported) {
					t.Fatalf("ReceiveDatagram: %v", err)
				}
			}
		})
	}
}

func TestDatagramTooLarge(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, &Config{Datagrams: true}, &Config{Datagrams: true})
	for _, c := range []*Conn{client, server} {
		// No payload fits in a packet of maxDatagram bytes, the largest quic-go sends.
		for _, n := range []int{maxDatagram, 1 << 16} {
			if err := c.SendDatagram(make([]byte, n)); !errors.Is(err, ErrDatagramTooLarge) {
				t.Fatalf("%d bytes: %v", n, err)
			}
		}
		if err := c.SendDatagram(make([]byte, 1000)); err != nil {
			t.Fatalf("1000 bytes: %v", err)
		}
	}
}

func TestSendDatagramNeverBlocks(t *testing.T) {
	checkGoroutines(t)
	client, _ := pair(t, &Config{Datagrams: true}, &Config{Datagrams: true})
	const sends = 5000
	p := make([]byte, 1000)
	type result struct {
		dropped int
		err     error
	}
	done := make(chan result, 1)
	start := time.Now()
	go func() {
		var r result
		for range sends {
			switch err := client.SendDatagram(p); {
			case errors.Is(err, ErrDatagramDropped):
				r.dropped++
			case err != nil:
				r.err = err
			}
		}
		done <- r
	}()
	var r result
	select {
	case r = <-done:
	case <-time.After(5 * time.Second):
		client.Close()
		<-done
		t.Fatalf("%d sends did not return within 5s", sends)
	}
	t.Logf("%d sends in %v, %d dropped", sends, time.Since(start), r.dropped)
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.dropped == 0 {
		t.Fatalf("none of %d sends was dropped", sends)
	}
}

func TestReceiveDatagramEnds(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, &Config{Datagrams: true}, &Config{Datagrams: true})

	ctx, cancel := context.WithCancel(testCtx(t))
	cancel()
	if _, err := server.ReceiveDatagram(ctx); err != context.Canceled {
		t.Fatalf("canceled context: %v", err)
	}
	ctx, cancel = context.WithTimeout(testCtx(t), 20*time.Millisecond)
	defer cancel()
	if _, err := server.ReceiveDatagram(ctx); err != context.DeadlineExceeded {
		t.Fatalf("expired context: %v", err)
	}

	got := make(chan error, 2)
	for _, c := range []*Conn{client, server} {
		go func() {
			_, err := c.ReceiveDatagram(context.Background())
			got <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	if err := client.CloseWithError(7, ""); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case err := <-got:
			var e *Error
			if !errors.As(err, &e) || e.Code != 7 || !errors.Is(err, ErrClosed) && !errors.Is(err, ErrPeerClosed) {
				t.Fatalf("after close: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("ReceiveDatagram did not return after close")
		}
	}
	wantCode(t, client.SendDatagram([]byte("x")), ErrClosed, 7)
	if _, err := client.ReceiveDatagram(testCtx(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("receive after close: %v", err)
	}
	waitFor(t, "the server to see the close", func() bool { return server.Context().Err() != nil })
	wantCode(t, server.SendDatagram([]byte("x")), ErrPeerClosed, 7)
}

func TestStreamID(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, nil, nil)
	ctx := testCtx(t)
	// RFC 9000 section 2.1: client-initiated bidirectional streams are 0, 4, 8..., server-initiated ones 1, 5, 9...
	for _, want := range []uint64{0, 4, 1, 5} {
		opener, acceptor := client, server
		if want%4 == 1 {
			opener, acceptor = server, client
		}
		s, err := opener.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		a, err := acceptor.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if s.ID() != want || a.ID() != want {
			t.Fatalf("stream IDs %d and %d, want %d", s.ID(), a.ID(), want)
		}
	}
}
