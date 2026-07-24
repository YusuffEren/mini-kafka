package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// recordEqual reports whether two records are semantically equal. It treats nil
// and empty byte slices as different so that round-trip tests can distinguish the
// encoding rules for null/absent payloads.
func recordEqual(a, b *Record) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.Offset != b.Offset ||
		a.Timestamp != b.Timestamp ||
		a.Attributes != b.Attributes {
		return false
	}
	if len(a.Key) != len(b.Key) || len(a.Value) != len(b.Value) {
		return false
	}
	if string(a.Key) != string(b.Key) || string(a.Value) != string(b.Value) {
		return false
	}
	return true
}

func encodeRecord(tb testing.TB, r *Record) []byte {
	tb.Helper()
	var buf bytes.Buffer
	if _, err := r.Encode(&buf); err != nil {
		tb.Fatalf("Encode failed: %v", err)
	}
	return buf.Bytes()
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		rec  Record
	}{
		{
			name: "normal key/value",
			rec: Record{
				Offset:    42,
				Timestamp: 1620000000000,
				Key:       []byte("user-1"),
				Value:     []byte("hello world"),
			},
		},
		{
			name: "null key",
			rec: Record{
				Offset:    7,
				Timestamp: 0,
				Value:     []byte("value-only"),
			},
		},
		{
			name: "null value",
			rec: Record{
				Offset:    99,
				Timestamp: 1234567890,
				Key:       []byte("key-only"),
			},
		},
		{
			name: "tombstone",
			rec: Record{
				Offset:     1001,
				Timestamp:  1620000000001,
				Attributes: 1,
				Key:        []byte("delete-me"),
				Value:      nil,
			},
		},
		{
			name: "large offset",
			rec: Record{
				Offset:    9223372036854775807,
				Timestamp: -1,
				Key:       []byte("k"),
				Value:     []byte("v"),
			},
		},
		{
			name: "binary payload",
			rec: Record{
				Offset:    0,
				Timestamp: 5,
				Key:       []byte{0x00, 0x01, 0xff},
				Value:     []byte{0x00, 0x00, 0x00},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := encodeRecord(t, &tc.rec)
			got, n, err := DecodeRecord(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("DecodeRecord failed: %v", err)
			}
			if n != len(data) {
				t.Fatalf("decoded bytes = %d, want %d", n, len(data))
			}
			if !recordEqual(got, &tc.rec) {
				t.Fatalf("round-trip mismatch: got %+v, want %+v", got, tc.rec)
			}
		})
	}
}

func TestDecodeCorruptCRC(t *testing.T) {
	rec := &Record{
		Offset:    10,
		Timestamp: 1000,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}
	data := encodeRecord(t, rec)

	// Mutate the last byte, which belongs to the value payload and therefore
	// changes the CRC but still leaves the data partially readable.
	data[len(data)-1] ^= 0xff

	_, _, err := DecodeRecord(bytes.NewReader(data))
	if !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("got error %v, want ErrCorruptRecord", err)
	}
}

func TestRecordTooLarge(t *testing.T) {
	rec := &Record{
		Offset:    1,
		Timestamp: 1,
		Value:     make([]byte, MaxRecordSize+1),
	}
	var buf bytes.Buffer
	_, err := rec.Encode(&buf)
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("got error %v, want ErrRecordTooLarge", err)
	}
}

func TestDecodeTooLargeLength(t *testing.T) {
	var buf bytes.Buffer
	// Encode a length that is exactly one byte over the limit. The length
	// field itself is the only thing needed to fail with ErrRecordTooLarge.
	if err := binary.Write(&buf, binary.BigEndian, int32(MaxRecordSize+1)); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	_, _, err := DecodeRecord(&buf)
	if !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("got error %v, want ErrRecordTooLarge", err)
	}
}

func TestEncodedSize(t *testing.T) {
	cases := []struct {
		name string
		rec  Record
	}{
		{
			name: "empty key and value",
			rec:  Record{Offset: 0, Timestamp: 0, Key: []byte{}, Value: []byte{}},
		},
		{
			name: "nil key and value",
			rec:  Record{Offset: 0, Timestamp: 0},
		},
		{
			name: "unicode payload",
			rec:  Record{Key: []byte("日本語"), Value: []byte("αβγ")},
		},
		{
			name: "1KB payload",
			rec:  Record{Value: make([]byte, 1024)},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := tc.rec.EncodedSize()
			data := encodeRecord(t, &tc.rec)
			if len(data) != want {
				t.Fatalf("encoded size = %d, EncodedSize() = %d", len(data), want)
			}
			_, got, err := DecodeRecord(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("DecodeRecord failed: %v", err)
			}
			if got != want {
				t.Fatalf("decoded byte count = %d, EncodedSize() = %d", got, want)
			}
		})
	}
}

func TestEncodedSizeMaxBoundary(t *testing.T) {
	// A record whose encoded size is exactly MaxRecordSize should encode and
	// decode successfully.
	fixed := 4 + 8 + 8 + 4 + 1 + 4 + 4 // length..value fields without payloads
	valueLen := MaxRecordSize - fixed
	rec := &Record{
		Offset:    0,
		Timestamp: 0,
		Value:     make([]byte, valueLen),
	}
	if rec.EncodedSize() != MaxRecordSize {
		t.Fatalf("EncodedSize = %d, want %d", rec.EncodedSize(), MaxRecordSize)
	}
	var buf bytes.Buffer
	if _, err := rec.Encode(&buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	if buf.Len() != MaxRecordSize {
		t.Fatalf("encoded bytes = %d, want %d", buf.Len(), MaxRecordSize)
	}
	got, n, err := DecodeRecord(&buf)
	if err != nil {
		t.Fatalf("DecodeRecord failed: %v", err)
	}
	if n != MaxRecordSize {
		t.Fatalf("decoded bytes = %d, want %d", n, MaxRecordSize)
	}
	if len(got.Value) != valueLen {
		t.Fatalf("value len = %d, want %d", len(got.Value), valueLen)
	}
}

func TestDecodeInvalidLength(t *testing.T) {
	t.Run("negative length", func(t *testing.T) {
		var buf bytes.Buffer
		if err := binary.Write(&buf, binary.BigEndian, int32(-1)); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		_, _, err := DecodeRecord(&buf)
		if !errors.Is(err, ErrCorruptRecord) {
			t.Fatalf("got error %v, want ErrCorruptRecord", err)
		}
	})

	t.Run("length too large", func(t *testing.T) {
		var buf bytes.Buffer
		if err := binary.Write(&buf, binary.BigEndian, int32(MaxRecordSize+1)); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		_, _, err := DecodeRecord(&buf)
		if !errors.Is(err, ErrRecordTooLarge) {
			t.Fatalf("got error %v, want ErrRecordTooLarge", err)
		}
	})

	t.Run("length shorter than fixed header", func(t *testing.T) {
		// offset(8) + timestamp(8) + crc(4) + attributes(1) = 21 bytes minimum
		// after the length field. Claim the payload is only 20 bytes long.
		var buf bytes.Buffer
		if err := binary.Write(&buf, binary.BigEndian, int32(20)); err != nil {
			t.Fatalf("setup failed: %v", err)
		}
		buf.Write(make([]byte, 20))
		_, _, err := DecodeRecord(&buf)
		if err == nil {
			t.Fatalf("expected error, got nil")
		}
	})
}

func TestEncodeDecodeEdgeCases(t *testing.T) {
	t.Run("empty key and empty value", func(t *testing.T) {
		rec := &Record{
			Offset:    0,
			Timestamp: 0,
			Key:       []byte{},
			Value:     []byte{},
		}
		data := encodeRecord(t, rec)
		got, _, err := DecodeRecord(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("DecodeRecord failed: %v", err)
		}
		if !recordEqual(got, rec) {
			t.Fatalf("mismatch: got %+v, want %+v", got, rec)
		}
		if got.Key == nil || got.Value == nil {
			t.Fatalf("empty slices should remain non-nil, got key=%v value=%v", got.Key, got.Value)
		}
	})

	t.Run("zero timestamp", func(t *testing.T) {
		rec := &Record{Offset: 1, Timestamp: 0, Key: []byte("x"), Value: []byte("y")}
		data := encodeRecord(t, rec)
		got, _, err := DecodeRecord(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("DecodeRecord failed: %v", err)
		}
		if got.Timestamp != 0 {
			t.Fatalf("Timestamp = %d, want 0", got.Timestamp)
		}
	})

	t.Run("large offset", func(t *testing.T) {
		rec := &Record{Offset: 1<<63 - 1, Timestamp: 0, Key: nil, Value: nil}
		data := encodeRecord(t, rec)
		got, _, err := DecodeRecord(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("DecodeRecord failed: %v", err)
		}
		if got.Offset != 1<<63-1 {
			t.Fatalf("Offset = %d, want %d", got.Offset, 1<<63-1)
		}
	})

	t.Run("tombstone bit", func(t *testing.T) {
		rec := &Record{Offset: 0, Timestamp: 0, Attributes: 1, Key: []byte("k"), Value: nil}
		data := encodeRecord(t, rec)
		got, _, err := DecodeRecord(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("DecodeRecord failed: %v", err)
		}
		if got.Attributes != 1 {
			t.Fatalf("Attributes = %d, want 1", got.Attributes)
		}
		if got.Value != nil {
			t.Fatalf("tombstone value should be nil, got %v", got.Value)
		}
	})

	t.Run("negative offset", func(t *testing.T) {
		rec := &Record{Offset: -123, Timestamp: 0, Key: nil, Value: nil}
		data := encodeRecord(t, rec)
		got, _, err := DecodeRecord(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("DecodeRecord failed: %v", err)
		}
		if got.Offset != -123 {
			t.Fatalf("Offset = %d, want -123", got.Offset)
		}
	})

	t.Run("nil record encode", func(t *testing.T) {
		var r *Record
		var buf bytes.Buffer
		_, err := r.Encode(&buf)
		if err == nil {
			t.Fatalf("expected error for nil record, got nil")
		}
	})
}

func TestDecodeTruncated(t *testing.T) {
	rec := &Record{
		Offset:    1,
		Timestamp: 1,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}
	data := encodeRecord(t, rec)

	// Drop the last byte so the payload is shorter than the length field claims.
	_, _, err := DecodeRecord(bytes.NewReader(data[:len(data)-1]))
	if err == nil {
		t.Fatalf("expected error for truncated record, got nil")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF error, got %v", err)
	}
}

func TestDecodeGarbage(t *testing.T) {
	// A stream of random bytes should either fail to read the length or fail
	// CRC, but never return a valid record.
	garbage := []byte{0x00, 0x00, 0x00, 0x10, 0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe, 0xba, 0xbe, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	_, _, err := DecodeRecord(bytes.NewReader(garbage))
	if err == nil {
		t.Fatalf("expected error for garbage data, got nil")
	}
}

func BenchmarkEncode(b *testing.B) {
	rec := &Record{
		Offset:    123456789,
		Timestamp: 1620000000000,
		Key:       []byte("benchmark-key"),
		Value:     []byte("benchmark-value payload"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if _, err := rec.Encode(&buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecode(b *testing.B) {
	rec := &Record{
		Offset:    123456789,
		Timestamp: 1620000000000,
		Key:       []byte("benchmark-key"),
		Value:     []byte("benchmark-value payload"),
	}
	data := encodeRecord(b, rec)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeRecord(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}
