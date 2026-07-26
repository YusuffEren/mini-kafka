// Package protocol implements the wire codec for the mini-kafka binary protocol.
//
// This file implements request and response framing (docs/PROTOCOL.md Section
// 1). A frame is a length-prefixed sequence of fields encoded with the
// primitive codec functions defined in codec.go. All multi-byte integers are
// big-endian.
package protocol

import (
	"bytes"
	"errors"
	"io"
)

// MaxFrameSize is the maximum number of bytes a single frame body may occupy
// (excluding the leading int32 size field). Frames larger than this are
// rejected to protect the server from unbounded allocations.
const MaxFrameSize = 100 * 1024 * 1024 // 100 MB

// ErrFrameTooLarge is returned when the declared size of a frame is negative
// or exceeds MaxFrameSize.
var ErrFrameTooLarge = errors.New("protocol: frame too large")

// ErrFrameSizeMismatch is returned when the number of bytes consumed while
// decoding a frame body does not match the declared size field.
var ErrFrameSizeMismatch = errors.New("protocol: frame size mismatch")

// RequestFrame is a decoded request received from a client.
type RequestFrame struct {
	// Size is the number of bytes following the size field on the wire. It is
	// populated by ReadRequestFrame and computed by Write.
	Size int32
	// ApiKey identifies the request type.
	ApiKey int16
	// ApiVersion is the API version requested by the client. Currently always
	// 1.
	ApiVersion int16
	// CorrelationID is used to match a response to its request.
	CorrelationID int32
	// ClientID is the len-prefixed (int16 + UTF-8 bytes) client identifier.
	ClientID string
	// Payload is the apiKey-specific request body, encoded as int32 length
	// prefix + raw bytes.
	Payload []byte
}

// ResponseFrame is a response to be sent back to a client.
type ResponseFrame struct {
	// Size is the number of bytes following the size field on the wire. It is
	// populated by ReadResponseFrame and computed by Write.
	Size int32
	// CorrelationID mirrors the CorrelationID of the request this response
	// answers.
	CorrelationID int32
	// ErrorCode is 0 on success and a non-zero protocol error code otherwise.
	ErrorCode int16
	// Payload is the response body, encoded as int32 length prefix + raw
	// bytes.
	Payload []byte
}

// ReadRequestFrame reads a single request frame from r. It first reads the
// int32 size field, validates it against MaxFrameSize, and then decodes the
// remaining fields from an io.LimitedReader bounded by size. It returns
// ErrFrameTooLarge when size is negative or exceeds MaxFrameSize, and
// ErrFrameSizeMismatch when the body actually consumed does not match the
// declared size.
//
// It is a convenience wrapper around ReadRequestFrameWithLimit that applies
// the package-level MaxFrameSize cap and no additional server-level limit.
func ReadRequestFrame(r io.Reader) (*RequestFrame, error) {
	return ReadRequestFrameWithLimit(r, 0)
}

// ReadRequestFrameWithLimit reads a single request frame from r, enforcing an
// additional server-level size cap on top of the protocol MaxFrameSize. It
// first reads the int32 size field, validates it against MaxFrameSize and the
// provided maxRequestBytes, and then decodes the remaining fields from an
// io.LimitedReader bounded by size. It returns ErrFrameTooLarge when size is
// negative, exceeds MaxFrameSize, or — when maxRequestBytes > 0 — exceeds
// maxRequestBytes. The frame body is NOT read when the size is rejected, so
// the caller can close the connection without draining a large payload.
//
// A maxRequestBytes value of 0 disables the server-level limit; only the
// protocol MaxFrameSize cap is enforced in that case.
func ReadRequestFrameWithLimit(r io.Reader, maxRequestBytes int32) (*RequestFrame, error) {
	size, err := Int32(r)
	if err != nil {
		return nil, err
	}
	if size < 0 || size > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}
	if maxRequestBytes > 0 && size > maxRequestBytes {
		// Reject the frame without decoding its fields, but still drain the
		// declared body off the wire so the peer can finish sending its
		// bytes and observe a clean EOF rather than a connection-reset write
		// error. io.CopyN streams through a small internal buffer, so this
		// does not allocate the full payload — the OOM protection the
		// server-level limit is meant to provide is preserved. Any read
		// deadline set by the caller (e.g. an idle timeout) still applies
		// and bounds how long this drain may block.
		if _, derr := io.CopyN(io.Discard, r, int64(size)); derr != nil {
			return nil, derr
		}
		return nil, ErrFrameTooLarge
	}

	lr := &io.LimitedReader{R: r, N: int64(size)}
	f := &RequestFrame{Size: size}

	if f.ApiKey, err = Int16(lr); err != nil {
		return nil, err
	}
	if f.ApiVersion, err = Int16(lr); err != nil {
		return nil, err
	}
	if f.CorrelationID, err = Int32(lr); err != nil {
		return nil, err
	}
	if f.ClientID, err = String(lr); err != nil {
		return nil, err
	}
	if f.Payload, err = Bytes(lr); err != nil {
		return nil, err
	}

	// The body must consume exactly `size` bytes. A non-EOF read here means
	// the body was shorter than declared (the LimitedReader still has budget).
	var probe [1]byte
	n, _ := lr.Read(probe[:])
	if n > 0 {
		return nil, ErrFrameSizeMismatch
	}
	return f, nil
}

// Write encodes the request frame to w. The Size field is recomputed from the
// encoded body (apiKey through payload) and written first, followed by the
// body. It returns the total number of bytes written (4 + Size) along with
// any error from the underlying writer.
func (f *RequestFrame) Write(w io.Writer) (int, error) {
	var body bytes.Buffer
	if _, err := PutInt16(&body, f.ApiKey); err != nil {
		return 0, err
	}
	if _, err := PutInt16(&body, f.ApiVersion); err != nil {
		return 0, err
	}
	if _, err := PutInt32(&body, f.CorrelationID); err != nil {
		return 0, err
	}
	if _, err := PutString(&body, f.ClientID); err != nil {
		return 0, err
	}
	if _, err := PutBytes(&body, f.Payload); err != nil {
		return 0, err
	}

	f.Size = int32(body.Len())
	total := 0
	n, err := PutInt32(w, f.Size)
	total += n
	if err != nil {
		return total, err
	}
	m, err := w.Write(body.Bytes())
	total += m
	if err != nil {
		return total, err
	}
	return total, nil
}

// ReadResponseFrame reads a single response frame from r. It first reads the
// int32 size field, validates it against MaxFrameSize, and then decodes the
// remaining fields from an io.LimitedReader bounded by size. It returns
// ErrFrameTooLarge when size is negative or exceeds MaxFrameSize, and
// ErrFrameSizeMismatch when the body actually consumed does not match the
// declared size.
func ReadResponseFrame(r io.Reader) (*ResponseFrame, error) {
	size, err := Int32(r)
	if err != nil {
		return nil, err
	}
	if size < 0 || size > MaxFrameSize {
		return nil, ErrFrameTooLarge
	}

	lr := &io.LimitedReader{R: r, N: int64(size)}
	f := &ResponseFrame{Size: size}

	if f.CorrelationID, err = Int32(lr); err != nil {
		return nil, err
	}
	if f.ErrorCode, err = Int16(lr); err != nil {
		return nil, err
	}
	if f.Payload, err = Bytes(lr); err != nil {
		return nil, err
	}

	// The body must consume exactly `size` bytes.
	var probe [1]byte
	n, _ := lr.Read(probe[:])
	if n > 0 {
		return nil, ErrFrameSizeMismatch
	}
	return f, nil
}

// Write encodes the response frame to w. The Size field is recomputed from the
// encoded body (correlationID through payload) and written first, followed by
// the body. It returns the total number of bytes written (4 + Size) along with
// any error from the underlying writer.
func (f *ResponseFrame) Write(w io.Writer) (int, error) {
	var body bytes.Buffer
	if _, err := PutInt32(&body, f.CorrelationID); err != nil {
		return 0, err
	}
	if _, err := PutInt16(&body, f.ErrorCode); err != nil {
		return 0, err
	}
	if _, err := PutBytes(&body, f.Payload); err != nil {
		return 0, err
	}

	f.Size = int32(body.Len())
	total := 0
	n, err := PutInt32(w, f.Size)
	total += n
	if err != nil {
		return total, err
	}
	m, err := w.Write(body.Bytes())
	total += m
	if err != nil {
		return total, err
	}
	return total, nil
}
