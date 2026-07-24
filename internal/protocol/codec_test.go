package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"
)

// ---------------------------------------------------------------------------
// Int8
// ---------------------------------------------------------------------------

func TestPutInt8_roundTrip(t *testing.T) {
	cases := []int8{0, -1, 1, math.MaxInt8, math.MinInt8, 42, -42}
	for _, want := range cases {
		var buf bytes.Buffer
		n, err := PutInt8(&buf, want)
		if err != nil {
			t.Fatalf("PutInt8(%d) error: %v", want, err)
		}
		if n != 1 {
			t.Fatalf("PutInt8(%d) wrote %d bytes, want 1", want, n)
		}
		if buf.Len() != 1 {
			t.Fatalf("PutInt8(%d) buffer length = %d, want 1", want, buf.Len())
		}
		got, err := Int8(&buf)
		if err != nil {
			t.Fatalf("Int8() error: %v", err)
		}
		if got != want {
			t.Fatalf("Int8() = %d, want %d", got, want)
		}
	}
}

func TestInt8_truncatedReturnsEOF(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	_, err := Int8(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Int8() from empty buffer error = %v, want io.EOF", err)
	}
}

func TestPutInt8_bigEndianByteOrder(t *testing.T) {
	var buf bytes.Buffer
	_, _ = PutInt8(&buf, -1)
	want := []byte{0xff}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutInt8(-1) = %x, want %x", buf.Bytes(), want)
	}
}

// ---------------------------------------------------------------------------
// Int16
// ---------------------------------------------------------------------------

func TestPutInt16_roundTrip(t *testing.T) {
	cases := []int16{0, -1, 1, math.MaxInt16, math.MinInt16, 12345, -12345}
	for _, want := range cases {
		var buf bytes.Buffer
		n, err := PutInt16(&buf, want)
		if err != nil {
			t.Fatalf("PutInt16(%d) error: %v", want, err)
		}
		if n != 2 {
			t.Fatalf("PutInt16(%d) wrote %d bytes, want 2", want, n)
		}
		got, err := Int16(&buf)
		if err != nil {
			t.Fatalf("Int16() error: %v", err)
		}
		if got != want {
			t.Fatalf("Int16() = %d, want %d", got, want)
		}
	}
}

func TestInt16_truncatedReturnsUnexpectedEOF(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00})
	_, err := Int16(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Int16() from 1-byte buffer error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPutInt16_bigEndianByteOrder(t *testing.T) {
	var buf bytes.Buffer
	_, _ = PutInt16(&buf, 0x1234)
	want := []byte{0x12, 0x34}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutInt16(0x1234) = %x, want %x", buf.Bytes(), want)
	}
}

// ---------------------------------------------------------------------------
// Int32
// ---------------------------------------------------------------------------

func TestPutInt32_roundTrip(t *testing.T) {
	cases := []int32{0, -1, 1, math.MaxInt32, math.MinInt32, 123456789, -123456789}
	for _, want := range cases {
		var buf bytes.Buffer
		n, err := PutInt32(&buf, want)
		if err != nil {
			t.Fatalf("PutInt32(%d) error: %v", want, err)
		}
		if n != 4 {
			t.Fatalf("PutInt32(%d) wrote %d bytes, want 4", want, n)
		}
		got, err := Int32(&buf)
		if err != nil {
			t.Fatalf("Int32() error: %v", err)
		}
		if got != want {
			t.Fatalf("Int32() = %d, want %d", got, want)
		}
	}
}

func TestInt32_truncatedReturnsUnexpectedEOF(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00, 0x11, 0x22})
	_, err := Int32(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Int32() from 3-byte buffer error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPutInt32_bigEndianByteOrder(t *testing.T) {
	var buf bytes.Buffer
	_, _ = PutInt32(&buf, 0x12345678)
	want := []byte{0x12, 0x34, 0x56, 0x78}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutInt32(0x12345678) = %x, want %x", buf.Bytes(), want)
	}
}

// ---------------------------------------------------------------------------
// Int64
// ---------------------------------------------------------------------------

func TestPutInt64_roundTrip(t *testing.T) {
	cases := []int64{0, -1, 1, math.MaxInt64, math.MinInt64, 123456789012345, -123456789012345}
	for _, want := range cases {
		var buf bytes.Buffer
		n, err := PutInt64(&buf, want)
		if err != nil {
			t.Fatalf("PutInt64(%d) error: %v", want, err)
		}
		if n != 8 {
			t.Fatalf("PutInt64(%d) wrote %d bytes, want 8", want, n)
		}
		got, err := Int64(&buf)
		if err != nil {
			t.Fatalf("Int64() error: %v", err)
		}
		if got != want {
			t.Fatalf("Int64() = %d, want %d", got, want)
		}
	}
}

func TestInt64_truncatedReturnsUnexpectedEOF(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66})
	_, err := Int64(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Int64() from 7-byte buffer error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPutInt64_bigEndianByteOrder(t *testing.T) {
	var buf bytes.Buffer
	_, _ = PutInt64(&buf, 0x123456789abcdef0)
	want := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutInt64(0x123456789abcdef0) = %x, want %x", buf.Bytes(), want)
	}
}

// ---------------------------------------------------------------------------
// Bool
// ---------------------------------------------------------------------------

func TestPutBool_roundTrip(t *testing.T) {
	cases := []bool{false, true}
	for _, want := range cases {
		var buf bytes.Buffer
		n, err := PutBool(&buf, want)
		if err != nil {
			t.Fatalf("PutBool(%t) error: %v", want, err)
		}
		if n != 1 {
			t.Fatalf("PutBool(%t) wrote %d bytes, want 1", want, n)
		}
		got, err := Bool(&buf)
		if err != nil {
			t.Fatalf("Bool() error: %v", err)
		}
		if got != want {
			t.Fatalf("Bool() = %t, want %t", got, want)
		}
	}
}

func TestBool_nonZeroDecodesAsTrue(t *testing.T) {
	buf := bytes.NewBuffer([]byte{2})
	got, err := Bool(buf)
	if err != nil {
		t.Fatalf("Bool() error: %v", err)
	}
	if !got {
		t.Fatalf("Bool() with input 2 = %t, want true", got)
	}
}

func TestBool_negativeDecodesAsTrue(t *testing.T) {
	// -1 encoded as int8 is 0xff.
	buf := bytes.NewBuffer([]byte{0xff})
	got, err := Bool(buf)
	if err != nil {
		t.Fatalf("Bool() error: %v", err)
	}
	if !got {
		t.Fatalf("Bool() with input -1 = %t, want true", got)
	}
}

func TestBool_truncatedReturnsEOF(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	_, err := Bool(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Bool() from empty buffer error = %v, want io.EOF", err)
	}
}

// ---------------------------------------------------------------------------
// String
// ---------------------------------------------------------------------------

func TestPutString_roundTrip(t *testing.T) {
	cases := []string{
		"",
		"hello",
		"UTF-8: こんにちは 世界 🌍",
		string(make([]byte, math.MaxInt16)),
	}
	for _, want := range cases {
		var buf bytes.Buffer
		n, err := PutString(&buf, want)
		if err != nil {
			t.Fatalf("PutString(len=%d) error: %v", len(want), err)
		}
		if n != 2+len(want) {
			t.Fatalf("PutString(len=%d) wrote %d bytes, want %d", len(want), n, 2+len(want))
		}
		got, err := String(&buf)
		if err != nil {
			t.Fatalf("String() error: %v", err)
		}
		if got != want {
			t.Fatalf("String() round-trip mismatch for length %d", len(want))
		}
	}
}

func TestPutString_rejectsLengthExceedingInt16(t *testing.T) {
	var buf bytes.Buffer
	v := string(make([]byte, math.MaxInt16+1))
	_, err := PutString(&buf, v)
	if err == nil {
		t.Fatal("PutString with length > MaxInt16 expected error, got nil")
	}
}

func TestString_nullSentinelReturnsEmpty(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int16(-1))
	got, err := String(&buf)
	if err != nil {
		t.Fatalf("String() error: %v", err)
	}
	if got != "" {
		t.Fatalf("String() with null sentinel = %q, want empty string", got)
	}
}

func TestString_truncatedPayloadReturnsUnexpectedEOF(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int16(5))
	buf.Write([]byte("ab")) // only 2 of 5 bytes
	_, err := String(&buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("String() truncated payload error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPutString_bigEndianByteOrder(t *testing.T) {
	var buf bytes.Buffer
	_, _ = PutString(&buf, "AB")
	want := []byte{0x00, 0x02, 'A', 'B'}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutString(\"AB\") = %x, want %x", buf.Bytes(), want)
	}
}

// ---------------------------------------------------------------------------
// NullableString
// ---------------------------------------------------------------------------

func TestPutNullableString_nilEncodesAsNull(t *testing.T) {
	var buf bytes.Buffer
	n, err := PutNullableString(&buf, nil)
	if err != nil {
		t.Fatalf("PutNullableString(nil) error: %v", err)
	}
	if n != 2 {
		t.Fatalf("PutNullableString(nil) wrote %d bytes, want 2", n)
	}
	want := []byte{0xff, 0xff}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutNullableString(nil) = %x, want %x", buf.Bytes(), want)
	}
	got, err := NullableString(&buf)
	if err != nil {
		t.Fatalf("NullableString() error: %v", err)
	}
	if got != nil {
		t.Fatalf("NullableString() = %q, want nil", *got)
	}
}

func TestPutNullableString_emptyStringRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	empty := ""
	n, err := PutNullableString(&buf, &empty)
	if err != nil {
		t.Fatalf("PutNullableString(\"\") error: %v", err)
	}
	if n != 2 {
		t.Fatalf("PutNullableString(\"\") wrote %d bytes, want 2", n)
	}
	got, err := NullableString(&buf)
	if err != nil {
		t.Fatalf("NullableString() error: %v", err)
	}
	if got == nil {
		t.Fatal("NullableString() = nil, want non-nil empty pointer")
	}
	if *got != "" {
		t.Fatalf("NullableString() = %q, want \"\"", *got)
	}
}

func TestPutNullableString_nonEmptyRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := "kafka"
	n, err := PutNullableString(&buf, &want)
	if err != nil {
		t.Fatalf("PutNullableString(\"%s\") error: %v", want, err)
	}
	if n != 2+len(want) {
		t.Fatalf("PutNullableString(\"%s\") wrote %d bytes, want %d", want, n, 2+len(want))
	}
	got, err := NullableString(&buf)
	if err != nil {
		t.Fatalf("NullableString() error: %v", err)
	}
	if got == nil {
		t.Fatal("NullableString() = nil, want non-nil pointer")
	}
	if *got != want {
		t.Fatalf("NullableString() = %q, want %q", *got, want)
	}
}

func TestNullableString_truncatedPayloadReturnsUnexpectedEOF(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int16(4))
	buf.Write([]byte("ab"))
	_, err := NullableString(&buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("NullableString() truncated payload error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// ---------------------------------------------------------------------------
// Bytes
// ---------------------------------------------------------------------------

func TestPutBytes_roundTrip(t *testing.T) {
	cases := [][]byte{
		{},
		[]byte("hello"),
		[]byte{0x00, 0xff, 0x12, 0x34},
		make([]byte, 1<<20), // 1 MiB
	}
	for i, want := range cases {
		var buf bytes.Buffer
		n, err := PutBytes(&buf, want)
		if err != nil {
			t.Fatalf("case %d PutBytes(len=%d) error: %v", i, len(want), err)
		}
		if n != 4+len(want) {
			t.Fatalf("case %d PutBytes(len=%d) wrote %d bytes, want %d", i, len(want), n, 4+len(want))
		}
		got, err := Bytes(&buf)
		if err != nil {
			t.Fatalf("case %d Bytes() error: %v", i, err)
		}
		if len(want) == 0 {
			if len(got) != 0 {
				t.Fatalf("case %d Bytes() = len %d, want 0", i, len(got))
			}
			continue
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("case %d Bytes() round-trip mismatch for length %d", i, len(want))
		}
	}
}

func TestPutBytes_nilEncodesAsEmpty(t *testing.T) {
	var buf bytes.Buffer
	n, err := PutBytes(&buf, nil)
	if err != nil {
		t.Fatalf("PutBytes(nil) error: %v", err)
	}
	if n != 4 {
		t.Fatalf("PutBytes(nil) wrote %d bytes, want 4", n)
	}
	got, err := Bytes(&buf)
	if err != nil {
		t.Fatalf("Bytes() error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Bytes() = len %d, want 0", len(got))
	}
}

func TestBytes_nullSentinelReturnsNil(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(-1))
	got, err := Bytes(&buf)
	if err != nil {
		t.Fatalf("Bytes() error: %v", err)
	}
	if got != nil {
		t.Fatalf("Bytes() = %x, want nil", got)
	}
}

func TestBytes_truncatedPayloadReturnsUnexpectedEOF(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(10))
	buf.Write([]byte{0x01, 0x02})
	_, err := Bytes(&buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("Bytes() truncated payload error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPutBytes_bigEndianByteOrder(t *testing.T) {
	var buf bytes.Buffer
	_, _ = PutBytes(&buf, []byte{0xde, 0xad, 0xbe, 0xef})
	want := []byte{0x00, 0x00, 0x00, 0x04, 0xde, 0xad, 0xbe, 0xef}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutBytes() = %x, want %x", buf.Bytes(), want)
	}
}

// ---------------------------------------------------------------------------
// NullableBytes
// ---------------------------------------------------------------------------

func TestPutNullableBytes_nilEncodesAsNull(t *testing.T) {
	var buf bytes.Buffer
	n, err := PutNullableBytes(&buf, nil)
	if err != nil {
		t.Fatalf("PutNullableBytes(nil) error: %v", err)
	}
	if n != 4 {
		t.Fatalf("PutNullableBytes(nil) wrote %d bytes, want 4", n)
	}
	want := []byte{0xff, 0xff, 0xff, 0xff}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutNullableBytes(nil) = %x, want %x", buf.Bytes(), want)
	}
	got, err := NullableBytes(&buf)
	if err != nil {
		t.Fatalf("NullableBytes() error: %v", err)
	}
	if got != nil {
		t.Fatalf("NullableBytes() = %x, want nil", got)
	}
}

func TestPutNullableBytes_emptySliceRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	n, err := PutNullableBytes(&buf, []byte{})
	if err != nil {
		t.Fatalf("PutNullableBytes(empty) error: %v", err)
	}
	if n != 4 {
		t.Fatalf("PutNullableBytes(empty) wrote %d bytes, want 4", n)
	}
	got, err := NullableBytes(&buf)
	if err != nil {
		t.Fatalf("NullableBytes() error: %v", err)
	}
	if got == nil {
		t.Fatal("NullableBytes() = nil, want non-nil empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("NullableBytes() = len %d, want 0", len(got))
	}
}

func TestPutNullableBytes_nonEmptyRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	want := []byte{0x00, 0xff, 0xab, 0xcd}
	n, err := PutNullableBytes(&buf, want)
	if err != nil {
		t.Fatalf("PutNullableBytes() error: %v", err)
	}
	if n != 4+len(want) {
		t.Fatalf("PutNullableBytes() wrote %d bytes, want %d", n, 4+len(want))
	}
	got, err := NullableBytes(&buf)
	if err != nil {
		t.Fatalf("NullableBytes() error: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("NullableBytes() = %x, want %x", got, want)
	}
}

func TestNullableBytes_truncatedPayloadReturnsUnexpectedEOF(t *testing.T) {
	var buf bytes.Buffer
	_ = binary.Write(&buf, binary.BigEndian, int32(5))
	buf.Write([]byte{0x01})
	_, err := NullableBytes(&buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("NullableBytes() truncated payload error = %v, want io.ErrUnexpectedEOF", err)
	}
}

// ---------------------------------------------------------------------------
// ArrayHeader
// ---------------------------------------------------------------------------

func TestPutArrayHeader_roundTrip(t *testing.T) {
	cases := []int{0, 1, 100, math.MaxInt32}
	for _, want := range cases {
		var buf bytes.Buffer
		n, err := PutArrayHeader(&buf, want)
		if err != nil {
			t.Fatalf("PutArrayHeader(%d) error: %v", want, err)
		}
		if n != 4 {
			t.Fatalf("PutArrayHeader(%d) wrote %d bytes, want 4", want, n)
		}
		got, err := ArrayHeader(&buf)
		if err != nil {
			t.Fatalf("ArrayHeader() error: %v", err)
		}
		if got != want {
			t.Fatalf("ArrayHeader() = %d, want %d", got, want)
		}
	}
}

func TestPutArrayHeader_nullArrayRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	n, err := PutArrayHeader(&buf, -1)
	if err != nil {
		t.Fatalf("PutArrayHeader(-1) error: %v", err)
	}
	if n != 4 {
		t.Fatalf("PutArrayHeader(-1) wrote %d bytes, want 4", n)
	}
	got, err := ArrayHeader(&buf)
	if err != nil {
		t.Fatalf("ArrayHeader() error: %v", err)
	}
	if got != -1 {
		t.Fatalf("ArrayHeader() = %d, want -1", got)
	}
}

func TestPutArrayHeader_rejectsLengthExceedingInt32(t *testing.T) {
	var buf bytes.Buffer
	_, err := PutArrayHeader(&buf, math.MaxInt32+1)
	if err == nil {
		t.Fatal("PutArrayHeader with length > MaxInt32 expected error, got nil")
	}
}

func TestArrayHeader_truncatedReturnsUnexpectedEOF(t *testing.T) {
	buf := bytes.NewBuffer([]byte{0x00, 0x11, 0x22})
	_, err := ArrayHeader(buf)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("ArrayHeader() from 3-byte buffer error = %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestPutArrayHeader_bigEndianByteOrder(t *testing.T) {
	var buf bytes.Buffer
	_, _ = PutArrayHeader(&buf, 0x00010203)
	want := []byte{0x00, 0x01, 0x02, 0x03}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("PutArrayHeader(0x00010203) = %x, want %x", buf.Bytes(), want)
	}
}

// ---------------------------------------------------------------------------
// Cross-type mixed read/write sanity
// ---------------------------------------------------------------------------

func TestCodec_mixedPrimitives(t *testing.T) {
	var buf bytes.Buffer

	if _, err := PutInt32(&buf, 42); err != nil {
		t.Fatalf("PutInt32 error: %v", err)
	}
	if _, err := PutString(&buf, "topic"); err != nil {
		t.Fatalf("PutString error: %v", err)
	}
	if _, err := PutNullableBytes(&buf, []byte{0x01}); err != nil {
		t.Fatalf("PutNullableBytes error: %v", err)
	}
	if _, err := PutBool(&buf, true); err != nil {
		t.Fatalf("PutBool error: %v", err)
	}
	if _, err := PutArrayHeader(&buf, 3); err != nil {
		t.Fatalf("PutArrayHeader error: %v", err)
	}

	v32, err := Int32(&buf)
	if err != nil || v32 != 42 {
		t.Fatalf("Int32() = %d, want 42", v32)
	}
	s, err := String(&buf)
	if err != nil || s != "topic" {
		t.Fatalf("String() = %q, want \"topic\"", s)
	}
	b, err := NullableBytes(&buf)
	if err != nil || !bytes.Equal(b, []byte{0x01}) {
		t.Fatalf("NullableBytes() = %x, want [01]", b)
	}
	bl, err := Bool(&buf)
	if err != nil || !bl {
		t.Fatalf("Bool() = %t, want true", bl)
	}
	a, err := ArrayHeader(&buf)
	if err != nil || a != 3 {
		t.Fatalf("ArrayHeader() = %d, want 3", a)
	}

	if buf.Len() != 0 {
		t.Fatalf("buffer has %d leftover bytes after decode", buf.Len())
	}
}
