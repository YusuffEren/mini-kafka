// Package protocol implements the wire codec for the mini-kafka binary protocol.
//
// This file provides encode (Put*) and decode functions for the primitive wire
// types defined in MINI_KAFKA_SPEC.md Section 5.2:
//
//   - int8 / int16 / int32 / int64: big-endian, fixed size
//   - string: int16 length + UTF-8 bytes; length -1 means null
//   - bytes: int32 length + raw bytes; length -1 means null
//   - array<T>: int32 element count; count -1 means null array
//   - bool: int8, 0 or 1
//
// All multi-byte integers are encoded in big-endian (network) byte order using
// the encoding/binary package from the standard library.
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// ErrNullValue is returned by decode functions when a null sentinel is read
// from a non-nullable context (e.g. String reading a -1 length).
var ErrNullValue = errors.New("protocol: null value in non-nullable context")

// ---------------------------------------------------------------------------
// Encode functions
// ---------------------------------------------------------------------------

// PutInt8 writes v as a single signed byte to w and returns the number of
// bytes written (1) along with any error from the underlying writer.
func PutInt8(w io.Writer, v int8) (int, error) {
	err := binary.Write(w, binary.BigEndian, v)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

// PutInt16 writes v as a big-endian int16 to w and returns the number of bytes
// written (2) along with any error from the underlying writer.
func PutInt16(w io.Writer, v int16) (int, error) {
	err := binary.Write(w, binary.BigEndian, v)
	if err != nil {
		return 0, err
	}
	return 2, nil
}

// PutInt32 writes v as a big-endian int32 to w and returns the number of bytes
// written (4) along with any error from the underlying writer.
func PutInt32(w io.Writer, v int32) (int, error) {
	err := binary.Write(w, binary.BigEndian, v)
	if err != nil {
		return 0, err
	}
	return 4, nil
}

// PutInt64 writes v as a big-endian int64 to w and returns the number of bytes
// written (8) along with any error from the underlying writer.
func PutInt64(w io.Writer, v int64) (int, error) {
	err := binary.Write(w, binary.BigEndian, v)
	if err != nil {
		return 0, err
	}
	return 8, nil
}

// PutBool writes v as an int8 (0 for false, 1 for true) to w and returns the
// number of bytes written (1) along with any error from the underlying writer.
func PutBool(w io.Writer, v bool) (int, error) {
	var b int8
	if v {
		b = 1
	}
	return PutInt8(w, b)
}

// PutString writes v as a non-nullable string: an int16 length prefix followed
// by the UTF-8 bytes of v. Empty strings are encoded with length 0. The length
// is always non-negative; null strings are not representable here (use
// PutNullableString for that). It returns the total number of bytes written
// (2 + len(v)) along with any error from the underlying writer.
func PutString(w io.Writer, v string) (int, error) {
	n := len(v)
	if n > math.MaxInt16 {
		return 0, errors.New("protocol: string length exceeds int16 range")
	}
	written, err := PutInt16(w, int16(n))
	if err != nil {
		return written, err
	}
	if n == 0 {
		return written, nil
	}
	m, err := w.Write([]byte(v))
	if err != nil {
		return written + m, err
	}
	return written + m, nil
}

// PutNullableString writes v as a nullable string. A nil pointer is encoded as
// int16(-1) with no following bytes. A non-nil pointer is encoded as
// PutString(*v). It returns the total number of bytes written along with any
// error from the underlying writer.
func PutNullableString(w io.Writer, v *string) (int, error) {
	if v == nil {
		return PutInt16(w, -1)
	}
	return PutString(w, *v)
}

// PutBytes writes v as a non-nullable byte sequence: an int32 length prefix
// followed by the raw bytes. An empty (but non-nil) slice is encoded with
// length 0. It returns the total number of bytes written (4 + len(v)) along
// with any error from the underlying writer.
func PutBytes(w io.Writer, v []byte) (int, error) {
	n := len(v)
	if int64(n) > math.MaxInt32 {
		return 0, errors.New("protocol: bytes length exceeds int32 range")
	}
	written, err := PutInt32(w, int32(n))
	if err != nil {
		return written, err
	}
	if n == 0 {
		return written, nil
	}
	m, err := w.Write(v)
	if err != nil {
		return written + m, err
	}
	return written + m, nil
}

// PutNullableBytes writes v as a nullable byte sequence. A nil slice is encoded
// as int32(-1) with no following bytes. A non-nil slice is encoded as
// PutBytes(v). It returns the total number of bytes written along with any
// error from the underlying writer.
func PutNullableBytes(w io.Writer, v []byte) (int, error) {
	if v == nil {
		return PutInt32(w, -1)
	}
	return PutBytes(w, v)
}

// PutArrayHeader writes the element count of an array as an int32. A count of
// -1 denotes a null array. It returns the number of bytes written (4) along
// with any error from the underlying writer.
func PutArrayHeader(w io.Writer, length int) (int, error) {
	if int64(length) > math.MaxInt32 {
		return 0, errors.New("protocol: array length exceeds int32 range")
	}
	return PutInt32(w, int32(length))
}

// ---------------------------------------------------------------------------
// Decode functions
// ---------------------------------------------------------------------------

// Int8 reads a single signed byte from r and returns it along with any error
// from the underlying reader.
func Int8(r io.Reader) (int8, error) {
	var v int8
	err := binary.Read(r, binary.BigEndian, &v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Int16 reads a big-endian int16 from r and returns it along with any error
// from the underlying reader.
func Int16(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Int32 reads a big-endian int32 from r and returns it along with any error
// from the underlying reader.
func Int32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Int64 reads a big-endian int64 from r and returns it along with any error
// from the underlying reader.
func Int64(r io.Reader) (int64, error) {
	var v int64
	err := binary.Read(r, binary.BigEndian, &v)
	if err != nil {
		return 0, err
	}
	return v, nil
}

// Bool reads an int8 from r and returns true when it is non-zero. It returns
// the decoded value along with any error from the underlying reader.
func Bool(r io.Reader) (bool, error) {
	v, err := Int8(r)
	if err != nil {
		return false, err
	}
	return v != 0, nil
}

// String reads a non-nullable string from r: an int16 length prefix followed
// by that many UTF-8 bytes. A length of -1 (null sentinel) is treated as an
// empty string for symmetry with the non-nullable contract. It returns the
// decoded string along with any error from the underlying reader.
func String(r io.Reader) (string, error) {
	length, err := Int16(r)
	if err != nil {
		return "", err
	}
	if length < 0 {
		// null sentinel in a non-nullable context: treat as empty string.
		return "", nil
	}
	if length == 0 {
		return "", nil
	}
	if int(length) > MaxStringLength {
		return "", errors.New("protocol: string length exceeds maximum allowed limit")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// NullableString reads a nullable string from r. A length prefix of -1 yields a
// nil pointer. Otherwise the decoded string is returned as a non-nil pointer.
// It returns the value along with any error from the underlying reader.
func NullableString(r io.Reader) (*string, error) {
	length, err := Int16(r)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		empty := ""
		return &empty, nil
	}
	if int(length) > MaxStringLength {
		return nil, errors.New("protocol: string length exceeds maximum allowed limit")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	s := string(buf)
	return &s, nil
}

const (
	// MaxStringLength is the maximum allowed byte length for a string (10 MB).
	MaxStringLength = 10 * 1024 * 1024
	// MaxBytesLength is the maximum allowed byte length for raw byte payloads (100 MB).
	MaxBytesLength = 100 * 1024 * 1024
	// MaxArrayElements is the maximum allowed element count for decoded arrays.
	MaxArrayElements = math.MaxInt32
)

// Bytes reads a non-nullable byte sequence from r: an int32 length prefix
// followed by that many raw bytes. A length of -1 (null sentinel) yields nil
// for symmetry with PutNullableBytes. It returns the decoded slice along with
// any error from the underlying reader.
func Bytes(r io.Reader) ([]byte, error) {
	length, err := Int32(r)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		return []byte{}, nil
	}
	if length > MaxBytesLength {
		return nil, errors.New("protocol: bytes length exceeds maximum allowed limit")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// NullableBytes reads a nullable byte sequence from r. A length prefix of -1
// yields nil. Otherwise the decoded slice is returned (possibly empty but
// non-nil). It returns the value along with any error from the underlying
// reader.
func NullableBytes(r io.Reader) ([]byte, error) {
	length, err := Int32(r)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		return []byte{}, nil
	}
	if length > MaxBytesLength {
		return nil, errors.New("protocol: bytes length exceeds maximum allowed limit")
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

// ArrayHeader reads the element count of an array as an int32. A count of -1
// denotes a null array. It returns the count along with any error from the
// underlying reader.
func ArrayHeader(r io.Reader) (int, error) {
	v, err := Int32(r)
	if err != nil {
		return 0, err
	}
	if v > MaxArrayElements {
		return 0, errors.New("protocol: array element count exceeds maximum allowed limit")
	}
	return int(v), nil
}
