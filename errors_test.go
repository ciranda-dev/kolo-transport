package kolotransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

const peerText = "peer-supplied-text"

func TestMapErr(t *testing.T) {
	for _, tc := range []struct {
		in   error
		want error
		code uint64
	}{
		{&quic.ApplicationError{ErrorCode: 7, ErrorMessage: peerText, Remote: true}, ErrPeerClosed, 7},
		{&quic.ApplicationError{ErrorCode: 3, ErrorMessage: peerText}, ErrClosed, 3},
		{&quic.TransportError{ErrorCode: quic.ConnectionRefused, ErrorMessage: peerText, Remote: true}, ErrRefused, 2},
		{&quic.TransportError{ErrorCode: 0x128, ErrorMessage: peerText, Remote: true}, ErrHandshake, 0x128},
		{&quic.TransportError{ErrorCode: quic.NoError, ErrorMessage: peerText, Remote: true}, ErrPeerClosed, 0},
		{&quic.TransportError{ErrorCode: quic.NoError}, ErrClosed, 0},
		{&quic.TransportError{ErrorCode: quic.FlowControlError, ErrorMessage: peerText, Remote: true}, ErrTransport, 3},
		{&quic.StreamError{ErrorCode: 9, Remote: true}, ErrStreamReset, 9},
		{&quic.StreamError{ErrorCode: 4}, ErrStreamCanceled, 4},
		{fmt.Errorf("wrapped: %w", &quic.ApplicationError{ErrorCode: 1, ErrorMessage: peerText, Remote: true}), ErrPeerClosed, 1},
	} {
		got := mapErr(tc.in)
		wantCode(t, got, tc.want, tc.code)
		if strings.Contains(got.Error(), peerText) {
			t.Errorf("%v leaks peer text", got)
		}
	}
	for _, tc := range []struct{ in, want error }{
		{nil, nil},
		{io.EOF, io.EOF},
		{fmt.Errorf("x: %w", os.ErrDeadlineExceeded), os.ErrDeadlineExceeded},
		{context.Canceled, context.Canceled},
		{context.DeadlineExceeded, context.DeadlineExceeded},
		{&quic.IdleTimeoutError{}, ErrTimeout},
		{&quic.HandshakeTimeoutError{}, ErrTimeout},
		{&quic.StatelessResetError{}, ErrPeerClosed},
		{&quic.VersionNegotiationError{}, ErrHandshake},
		{quic.ErrServerClosed, ErrClosed},
		{quic.ErrStreamClosed, ErrClosed},
		{net.ErrClosed, ErrClosed},
		{errors.New(peerText), ErrTransport},
	} {
		if got := mapErr(tc.in); got != tc.want {
			t.Errorf("mapErr(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestPeerCloseGivesCodeOnly(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, nil, nil)
	if err := client.CloseWithError(42, peerText); err != nil {
		t.Fatal(err)
	}
	_, err := server.AcceptStream(testCtx(t))
	wantCode(t, err, ErrPeerClosed, 42)
	if strings.Contains(err.Error(), peerText) {
		t.Fatalf("%v leaks peer text", err)
	}
	_, err = client.OpenStream(testCtx(t))
	wantCode(t, err, ErrClosed, 42)
}

func TestCloseWithInvalidArguments(t *testing.T) {
	checkGoroutines(t)
	for name, tc := range map[string]struct {
		code uint64
		msg  string
	}{
		"code":    {MaxCode + 1, ""},
		"message": {1, strings.Repeat("x", maxCloseMessage+1)},
	} {
		client, server := pair(t, nil, nil)
		if err := client.CloseWithError(tc.code, tc.msg); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("%s: got %v", name, err)
		}
		_, err := server.AcceptStream(testCtx(t))
		wantCode(t, err, ErrPeerClosed, 0)
	}
}

func TestStreamCodes(t *testing.T) {
	checkGoroutines(t)
	client, server := pair(t, nil, nil)
	ctx := testCtx(t)

	cs, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := cs.CancelWrite(9); err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(ss)
	wantCode(t, err, ErrStreamReset, 9)
	_, err = cs.Write([]byte("x"))
	wantCode(t, err, ErrStreamCanceled, 9)

	if err := ss.CancelRead(MaxCode + 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v", err)
	}
	if err := cs.CancelRead(5); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err = ss.Write(make([]byte, 1024)); err != nil {
			break
		}
	}
	wantCode(t, err, ErrStreamReset, 5)

	us, err := client.OpenUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := us.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	rs, err := server.AcceptUniStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := us.CancelWrite(MaxCode + 1); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("got %v", err)
	}
	_, err = io.ReadAll(rs)
	wantCode(t, err, ErrStreamReset, 0)
	if err := rs.CancelRead(1); err != nil {
		t.Fatal(err)
	}
}
