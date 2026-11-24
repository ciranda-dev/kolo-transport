package kolotransport

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"strconv"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

// MaxCode is the largest error code QUIC can carry.
const MaxCode uint64 = 1<<62 - 1

const maxCloseMessage = 256

var (
	// ErrInvalidConfig means a Config field is out of range.
	ErrInvalidConfig = errors.New("kolotransport: invalid config")
	// ErrInvalidArgument means a bad code, close message, ExportKey label, PacketConn, listen or dial address, packet or destination (see the README).
	ErrInvalidArgument = errors.New("kolotransport: invalid argument")
	// ErrClosed means this side closed the listener, connection or stream.
	ErrClosed = errors.New("kolotransport: closed")
	// ErrPeerClosed means the peer closed the connection or no longer has state for it.
	ErrPeerClosed = errors.New("kolotransport: peer closed the connection")
	// ErrRefused means the peer refused the connection, for example because its listener is full.
	ErrRefused = errors.New("kolotransport: connection refused")
	// ErrHandshake means the TLS or QUIC handshake failed.
	ErrHandshake = errors.New("kolotransport: handshake failed")
	// ErrTimeout means the handshake or idle timeout expired.
	ErrTimeout = errors.New("kolotransport: timeout")
	// ErrTransport means a QUIC transport error or another failure of the connection.
	ErrTransport = errors.New("kolotransport: transport error")
	// ErrStreamReset means the peer reset the stream or asked this side to stop sending.
	ErrStreamReset = errors.New("kolotransport: stream reset by peer")
	// ErrMaxDialed means Listener.Dial already has MaxDialed connections open.
	ErrMaxDialed = errors.New("kolotransport: too many dialed connections")
	// ErrStreamCanceled means this side canceled the stream.
	ErrStreamCanceled = errors.New("kolotransport: stream canceled")
	// ErrDatagramsUnsupported means a datagram call on a connection whose ends did not both set Config.Datagrams.
	ErrDatagramsUnsupported = errors.New("kolotransport: datagrams not supported by both ends")
	// ErrDatagramTooLarge means the datagram does not fit in one packet on the current path.
	ErrDatagramTooLarge = errors.New("kolotransport: datagram too large")
	// ErrDatagramDropped means the datagram send queue was full and the datagram was dropped, as a full UDP socket drops.
	ErrDatagramDropped = errors.New("kolotransport: datagram dropped, send queue full")
)

// Error is a connection or stream error with its numeric code; Unwrap returns one of the sentinels.
type Error struct {
	Err  error
	Code uint64
}

// Error returns the sentinel text and the code.
func (e *Error) Error() string {
	return e.Err.Error() + " (code " + strconv.FormatUint(e.Code, 10) + ")"
}

// Unwrap returns the sentinel.
func (e *Error) Unwrap() error { return e.Err }

func ctxErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	return mapErr(err)
}

// mapErr keeps only codes from quic-go errors: their messages may come from the peer.
func mapErr(err error) error {
	var (
		appErr       *quic.ApplicationError
		transportErr *quic.TransportError
		streamErr    *quic.StreamError
		idleErr      *quic.IdleTimeoutError
		handshakeErr *quic.HandshakeTimeoutError
		resetErr     *quic.StatelessResetError
		versionErr   *quic.VersionNegotiationError
		tooLargeErr  *quic.DatagramTooLargeError
	)
	switch {
	case err == nil:
		return nil
	case err == io.EOF:
		return io.EOF
	case errors.Is(err, os.ErrDeadlineExceeded):
		return os.ErrDeadlineExceeded
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.As(err, &streamErr):
		if streamErr.Remote {
			return &Error{Err: ErrStreamReset, Code: uint64(streamErr.ErrorCode)}
		}
		return &Error{Err: ErrStreamCanceled, Code: uint64(streamErr.ErrorCode)}
	case errors.As(err, &appErr):
		if appErr.Remote {
			return &Error{Err: ErrPeerClosed, Code: uint64(appErr.ErrorCode)}
		}
		return &Error{Err: ErrClosed, Code: uint64(appErr.ErrorCode)}
	case errors.As(err, &transportErr):
		code := uint64(transportErr.ErrorCode)
		switch {
		case transportErr.ErrorCode == quic.ConnectionRefused:
			return &Error{Err: ErrRefused, Code: code}
		case transportErr.ErrorCode.IsCryptoError():
			return &Error{Err: ErrHandshake, Code: code}
		case transportErr.ErrorCode == quic.NoError && transportErr.Remote:
			return &Error{Err: ErrPeerClosed, Code: code}
		case transportErr.ErrorCode == quic.NoError:
			return &Error{Err: ErrClosed, Code: code}
		}
		return &Error{Err: ErrTransport, Code: code}
	case errors.As(err, &idleErr), errors.As(err, &handshakeErr):
		return ErrTimeout
	case errors.As(err, &resetErr):
		return ErrPeerClosed
	case errors.As(err, &versionErr):
		return ErrHandshake
	case errors.As(err, &tooLargeErr):
		return ErrDatagramTooLarge
	case errors.Is(err, quic.ErrServerClosed), errors.Is(err, quic.ErrStreamClosed), errors.Is(err, net.ErrClosed):
		return ErrClosed
	}
	return ErrTransport
}
