package kolotransport

import (
	"time"

	quic "github.com/ciranda-dev/kolo-transport/internal/quicgo"
)

// Stream is a bidirectional stream. Read and Write may be used from different goroutines.
type Stream struct {
	s *quic.Stream
}

// ID returns the QUIC stream ID (RFC 9000 section 2.1), which both ends see alike.
func (s *Stream) ID() uint64 { return uint64(s.s.StreamID()) }

// Read reads data from the peer; it returns io.EOF after the peer closed its send side.
func (s *Stream) Read(p []byte) (int, error) {
	n, err := s.s.Read(p)
	return n, mapErr(err)
}

// Write writes data to the peer, blocking while flow control is exhausted.
func (s *Stream) Write(p []byte) (int, error) {
	n, err := s.s.Write(p)
	return n, mapErr(err)
}

// Close ends the send side; the receive side stays open.
func (s *Stream) Close() error { return mapErr(s.s.Close()) }

// CancelRead asks the peer to stop sending, with code (0 and ErrInvalidArgument if above MaxCode).
func (s *Stream) CancelRead(code uint64) error {
	code, err := checkCode(code)
	s.s.CancelRead(quic.StreamErrorCode(code))
	return err
}

// CancelWrite abandons the send side with code (0 and ErrInvalidArgument if above MaxCode).
func (s *Stream) CancelWrite(code uint64) error {
	code, err := checkCode(code)
	s.s.CancelWrite(quic.StreamErrorCode(code))
	return err
}

// SetDeadline sets the read and write deadlines; an expired deadline returns os.ErrDeadlineExceeded.
func (s *Stream) SetDeadline(t time.Time) error { return s.s.SetDeadline(t) }

// SetReadDeadline sets the read deadline.
func (s *Stream) SetReadDeadline(t time.Time) error { return s.s.SetReadDeadline(t) }

// SetWriteDeadline sets the write deadline.
func (s *Stream) SetWriteDeadline(t time.Time) error { return s.s.SetWriteDeadline(t) }

// SendStream is the sending end of a unidirectional stream.
type SendStream struct {
	s *quic.SendStream
}

// Write writes data to the peer, blocking while flow control is exhausted.
func (s *SendStream) Write(p []byte) (int, error) {
	n, err := s.s.Write(p)
	return n, mapErr(err)
}

// Close ends the stream.
func (s *SendStream) Close() error { return mapErr(s.s.Close()) }

// CancelWrite abandons the stream with code (0 and ErrInvalidArgument if above MaxCode).
func (s *SendStream) CancelWrite(code uint64) error {
	code, err := checkCode(code)
	s.s.CancelWrite(quic.StreamErrorCode(code))
	return err
}

// SetWriteDeadline sets the write deadline; an expired deadline returns os.ErrDeadlineExceeded.
func (s *SendStream) SetWriteDeadline(t time.Time) error { return s.s.SetWriteDeadline(t) }

// ReceiveStream is the receiving end of a unidirectional stream.
type ReceiveStream struct {
	s *quic.ReceiveStream
}

// Read reads data from the peer; it returns io.EOF after the peer closed the stream.
func (s *ReceiveStream) Read(p []byte) (int, error) {
	n, err := s.s.Read(p)
	return n, mapErr(err)
}

// CancelRead asks the peer to stop sending, with code (0 and ErrInvalidArgument if above MaxCode).
func (s *ReceiveStream) CancelRead(code uint64) error {
	code, err := checkCode(code)
	s.s.CancelRead(quic.StreamErrorCode(code))
	return err
}

// SetReadDeadline sets the read deadline; an expired deadline returns os.ErrDeadlineExceeded.
func (s *ReceiveStream) SetReadDeadline(t time.Time) error { return s.s.SetReadDeadline(t) }

func checkCode(code uint64) (uint64, error) {
	if code > MaxCode {
		return 0, ErrInvalidArgument
	}
	return code, nil
}
