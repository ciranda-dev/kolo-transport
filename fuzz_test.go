package kolotransport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strconv"
	"testing"
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

var sentinels = []error{
	ErrInvalidConfig, ErrInvalidArgument, ErrClosed, ErrPeerClosed, ErrRefused, ErrHandshake,
	ErrTimeout, ErrTransport, ErrStreamReset, ErrStreamCanceled,
}

func FuzzVerifyPeerCertificate(f *testing.F) {
	cert, err := newCertificate()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(cert.Certificate[0], uint8(0))
	f.Add(cert.Certificate[0], uint8(1))
	f.Add([]byte{0x30, 0x82, 0xff, 0xff}, uint8(0))
	f.Fuzz(func(t *testing.T, data []byte, split uint8) {
		var raw [][]byte
		if split == 0 {
			raw = [][]byte{data}
		} else {
			for chunk := bytes.Clone(data); len(chunk) > 0; {
				n := min(int(split), len(chunk))
				raw = append(raw, chunk[:n])
				chunk = chunk[n:]
			}
		}
		if verifyPeerCertificate(raw, nil) == nil && len(raw) != 1 {
			t.Fatalf("accepted %d certificates", len(raw))
		}
	})
}

func FuzzMapError(f *testing.F) {
	f.Add(uint8(0), uint64(7), true, "peer text")
	f.Add(uint8(1), uint64(0x128), true, "kolotransport: closed")
	f.Add(uint8(2), uint64(1<<62), false, "")
	f.Fuzz(func(t *testing.T, kind uint8, code uint64, remote bool, msg string) {
		var in error
		switch kind % 6 {
		case 0:
			in = &quic.ApplicationError{ErrorCode: quic.ApplicationErrorCode(code), ErrorMessage: msg, Remote: remote}
		case 1:
			in = &quic.TransportError{ErrorCode: quic.TransportErrorCode(code), ErrorMessage: msg, Remote: remote}
		case 2:
			in = &quic.StreamError{ErrorCode: quic.StreamErrorCode(code), Remote: remote}
		case 3:
			in = &quic.StatelessResetError{}
		case 4:
			in = errors.New(msg)
		default:
			in = &quic.VersionNegotiationError{}
		}
		out := mapErr(in)
		var e *Error
		if errors.As(out, &e) {
			if e.Code != code || e.Error() != e.Err.Error()+" (code "+strconv.FormatUint(code, 10)+")" {
				t.Fatalf("got %q for code %d", e.Error(), code)
			}
			out = e.Err
		}
		for _, s := range sentinels {
			if out == s {
				return
			}
		}
		t.Fatalf("mapErr returned %q, not a sentinel", out)
	})
}

func FuzzConfig(f *testing.F) {
	f.Add(0, 0, 0, 0, int64(0), int64(0), int64(0), int64(0), int64(0), int64(0), false, int64(0), int64(0), int64(0), "")
	f.Add(1000, 9, 33, 1001, int64(time.Minute), int64(-1), int64(time.Second), int64(2<<30), int64(1<<20), int64(512<<10), true, int64(4<<20), int64(8<<20), int64(64<<20), "bbr")
	f.Add(1, 1, 1, 1, int64(time.Second), int64(2*time.Second), int64(time.Second), int64(128<<20), int64(64<<10), int64(64<<20), true, int64(0), int64(0), int64(1<<20), "BBR")
	f.Add(1, 1, 1, 1, int64(time.Second), int64(2*time.Second), int64(time.Second), int64(128<<20), int64(64<<10), int64(64<<20), false, int64(0), int64(0), int64(0), "cubic")
	f.Fuzz(func(t *testing.T, conns, perIP, dialed, streams int, handshake, idle, keepAlive, memory, streamWin, connWin int64, grow bool, grownStream, grownConn, grownMem int64, cc string) {
		c := Config{
			MaxConns:               conns,
			MaxDialed:              dialed,
			MaxConnsPerIP:          perIP,
			MaxIncomingStreams:     streams,
			HandshakeTimeout:       time.Duration(handshake),
			IdleTimeout:            time.Duration(idle),
			KeepAlivePeriod:        time.Duration(keepAlive),
			MaxMemory:              memory,
			MaxStreamReceiveWindow: streamWin,
			MaxConnReceiveWindow:   connWin,

			WindowGrowth:                grow,
			MaxGrownStreamReceiveWindow: grownStream,
			MaxGrownConnReceiveWindow:   grownConn,
			MaxGrownMemory:              grownMem,

			CongestionControl: cc,
		}
		dialCfg, dialErr := c.withDefaults()
		got, err := c.listenConfig()
		if err == nil && (dialErr != nil || !reflect.DeepEqual(dialCfg.quic(), got.quic())) {
			t.Fatalf("dial config %+v, %v", dialCfg, dialErr)
		}
		if err != nil {
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatal(err)
			}
			return
		}
		if got.MaxConns < 1 || got.MaxConns > 10000 ||
			got.MaxConnsPerIP < 1 || got.MaxConnsPerIP > got.MaxConns ||
			got.MaxMemory < 64<<20 || got.MaxMemory > 64<<30 ||
			got.MaxStreamReceiveWindow < 64<<10 || got.MaxStreamReceiveWindow > 16<<20 ||
			got.MaxConnReceiveWindow < got.MaxStreamReceiveWindow || got.MaxConnReceiveWindow > 64<<20 ||
			got.MaxDialed < 1 || got.MaxDialed > 10000 ||
			int64(got.MaxConns+got.MaxDialed)*got.MaxConnReceiveWindow > got.MaxMemory ||
			got.MaxIncomingStreams < 1 || got.MaxIncomingStreams > 1000 ||
			got.HandshakeTimeout < time.Second || got.HandshakeTimeout > time.Minute ||
			got.IdleTimeout < time.Second || got.IdleTimeout > 10*time.Minute ||
			got.KeepAlivePeriod < 500*time.Millisecond || got.KeepAlivePeriod >= got.IdleTimeout {
			t.Fatalf("out of range: %+v", got)
		}
		if got.CongestionControl != congestionReno && got.CongestionControl != congestionCubic && got.CongestionControl != congestionBBR || cc != "" && cc != got.CongestionControl {
			t.Fatalf("congestion control %q became %q", cc, got.CongestionControl)
		}
		if got.WindowGrowth && (got.MaxGrownStreamReceiveWindow < got.MaxStreamReceiveWindow || got.MaxGrownStreamReceiveWindow > 16<<20 ||
			got.MaxGrownConnReceiveWindow < got.MaxConnReceiveWindow || got.MaxGrownConnReceiveWindow < got.MaxGrownStreamReceiveWindow ||
			got.MaxGrownConnReceiveWindow > 64<<20 || got.MaxGrownMemory < 1<<20 || got.MaxGrownMemory > 64<<30) {
			t.Fatalf("growth out of range: %+v", got)
		}
		q := got.quic()
		wantStream, wantConn := got.MaxStreamReceiveWindow, got.MaxConnReceiveWindow
		if got.WindowGrowth {
			wantStream, wantConn = got.MaxGrownStreamReceiveWindow, got.MaxGrownConnReceiveWindow
		}
		if q.Allow0RTT || q.HandshakeIdleTimeout <= 0 || q.KeepAlivePeriod != got.KeepAlivePeriod ||
			q.MaxConnectionReceiveWindow != uint64(wantConn) || q.MaxStreamReceiveWindow != uint64(wantStream) ||
			q.InitialConnectionReceiveWindow != uint64(got.MaxConnReceiveWindow) || q.InitialStreamReceiveWindow != uint64(got.MaxStreamReceiveWindow) {
			t.Fatalf("bad quic config %+v", q)
		}
	})
}

// FuzzStream sends peer writes and resets through the stream wrappers over loopback.
func FuzzStream(f *testing.F) {
	ctx := context.Background()
	l, err := Listen(ctx, "127.0.0.1:0", nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { l.Close() })
	client, err := Dial(ctx, l.Addr().String(), nil)
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { client.Close() })
	server, err := l.Accept(ctx)
	if err != nil {
		f.Fatal(err)
	}

	f.Add([]byte("hello"), uint8(2), false, uint64(0))
	f.Add([]byte{}, uint8(0), true, uint64(MaxCode))
	f.Add(bytes.Repeat([]byte{1}, 3000), uint8(200), true, uint64(1<<63))
	f.Fuzz(func(t *testing.T, data []byte, chunk uint8, reset bool, code uint64) {
		if len(data) > 4<<10 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ss, err := client.OpenUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ss.SetWriteDeadline(time.Now().Add(10 * time.Second))
		for rest := data; len(rest) > 0; {
			n := min(int(chunk)+1, len(rest))
			if _, err := ss.Write(rest[:n]); err != nil {
				t.Fatal(err)
			}
			rest = rest[n:]
		}
		wantCodeSent := code
		if reset {
			err := ss.CancelWrite(code)
			if code > MaxCode {
				wantCodeSent = 0
				if !errors.Is(err, ErrInvalidArgument) {
					t.Fatalf("CancelWrite(%d): %v", code, err)
				}
			}
		} else if err := ss.Close(); err != nil {
			t.Fatal(err)
		}

		rs, err := server.AcceptUniStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rs.SetReadDeadline(time.Now().Add(10 * time.Second))
		got, err := io.ReadAll(rs)
		if !bytes.HasPrefix(data, got) {
			t.Fatalf("read %d bytes that are not a prefix of the %d sent", len(got), len(data))
		}
		if reset {
			var e *Error
			if !errors.Is(err, ErrStreamReset) || !errors.As(err, &e) || e.Code != wantCodeSent {
				t.Fatalf("got %v, want reset with code %d", err, wantCodeSent)
			}
			return
		}
		if err != nil || len(got) != len(data) {
			t.Fatalf("read %d of %d bytes: %v", len(got), len(data), err)
		}
	})
}
