package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// MaxRecordSize is the maximum allowed encoded size of a single record, in
// bytes. Records exceeding this limit are rejected during encode and decode.
const MaxRecordSize = 1048576 // 1 MiB

// Sentinel length values used to represent a null (absent) key or value in the
// binary format. A length of -1 means the corresponding payload is absent and
// no payload bytes follow.
const nullLength int32 = -1

// ErrCorruptRecord is returned when a record's stored CRC32 does not match the
// CRC32 computed over the decoded payload, indicating on-disk corruption.
var ErrCorruptRecord = errors.New("corrupt record: CRC mismatch")

// ErrRecordTooLarge is returned when a record's encoded size exceeds
// MaxRecordSize.
var ErrRecordTooLarge = errors.New("record exceeds maximum size")

// crcTable is the CRC32-Castagnoli table reused across all encode/decode
// operations.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is a single immutable entry in a partition log.
//
// The binary representation on disk is big-endian and laid out as:
//
//	length       int32   // byte count of everything after this field (offset..value)
//	offset       int64   // absolute offset within the partition
//	timestamp    int64   // unix milliseconds
//	crc32        uint32  // CRC32C over attributes..value
//	attributes   int8    // bit 0: tombstone, bits 1-7 reserved
//	keyLength    int32   // -1 means null key (no key bytes follow)
//	key          []byte  // keyLength bytes (omitted when keyLength == -1)
//	valueLength  int32   // -1 means null value (no value bytes follow)
//	value        []byte  // valueLength bytes (omitted when valueLength == -1)
type Record struct {
	Offset     int64
	Timestamp  int64
	Attributes int8
	Key        []byte
	Value      []byte
}

// EncodedSize returns the total number of bytes the record occupies when
// encoded, including the leading length field. It is constant-time and does
// not perform any I/O.
func (r *Record) EncodedSize() int {
	// length(4) + offset(8) + timestamp(8) + crc32(4) + attributes(1)
	size := 4 + 8 + 8 + 4 + 1
	// keyLength(4) + key bytes
	size += 4
	if r.Key != nil {
		size += len(r.Key)
	}
	// valueLength(4) + value bytes
	size += 4
	if r.Value != nil {
		size += len(r.Value)
	}
	return size
}

// Encode writes the binary representation of the record to w and returns the
// total number of bytes written (including the length field).
//
// The CRC32-Castagnoli checksum is computed over the bytes spanning attributes
// through the end of value. If the encoded size exceeds MaxRecordSize,
// ErrRecordTooLarge is returned without writing anything.
func (r *Record) Encode(w io.Writer) (int, error) {
	if r == nil {
		return 0, errors.New("encode: nil record")
	}

	total := r.EncodedSize()
	if total > MaxRecordSize {
		return 0, ErrRecordTooLarge
	}

	// Single allocation for the full wire image. Layout offsets:
	//   0:4   length
	//   4:12  offset
	//  12:20  timestamp
	//  20:24  crc32
	//  24:    attributes..value  (CRC-covered region)
	buf := make([]byte, total)

	binary.BigEndian.PutUint32(buf[0:4], uint32(total-4))
	binary.BigEndian.PutUint64(buf[4:12], uint64(r.Offset))
	binary.BigEndian.PutUint64(buf[12:20], uint64(r.Timestamp))
	// buf[20:24] filled after CRC is computed over the body.

	const crcStart = 24
	off := crcStart
	buf[off] = byte(r.Attributes)
	off++

	if r.Key != nil {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r.Key)))
		off += 4
		copy(buf[off:], r.Key)
		off += len(r.Key)
	} else {
		// nullLength is -1; write as two's-complement int32 bits.
		binary.BigEndian.PutUint32(buf[off:off+4], ^uint32(0))
		off += 4
	}

	if r.Value != nil {
		binary.BigEndian.PutUint32(buf[off:off+4], uint32(len(r.Value)))
		off += 4
		copy(buf[off:], r.Value)
	} else {
		binary.BigEndian.PutUint32(buf[off:off+4], ^uint32(0))
	}

	// CRC covers attributes through end of value — same region as before.
	checksum := crc32.Checksum(buf[crcStart:total], crcTable)
	binary.BigEndian.PutUint32(buf[20:24], checksum)

	return w.Write(buf)
}

// DecodeRecord reads a single record from r and returns it together with the
// total number of bytes consumed (including the length field).
//
// If the stored CRC32 does not match the computed CRC32, ErrCorruptRecord is
// returned. If the length field exceeds MaxRecordSize, ErrRecordTooLarge is
// returned.
func DecodeRecord(r io.Reader) (*Record, int, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, 0, err
	}
	read := 4

	length := int32(binary.BigEndian.Uint32(lenBuf[:]))
	if length < 0 {
		return nil, read, ErrCorruptRecord
	}
	if int(length) > MaxRecordSize {
		return nil, read, ErrRecordTooLarge
	}

	// Read the remaining `length` bytes in one shot so we can both compute the
	// CRC and parse the fields from a single buffer.
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, read, err
	}
	read += len(payload)

	// Minimum body: offset(8) + timestamp(8) + crc(4) + attributes(1) +
	// keyLength(4) + valueLength(4). Key/value payloads may be absent.
	const minPayload = 8 + 8 + 4 + 1 + 4 + 4
	if len(payload) < minPayload {
		return nil, read, io.ErrUnexpectedEOF
	}

	off := 0
	rec := &Record{}

	rec.Offset = int64(binary.BigEndian.Uint64(payload[off : off+8]))
	off += 8
	rec.Timestamp = int64(binary.BigEndian.Uint64(payload[off : off+8]))
	off += 8
	checksum := binary.BigEndian.Uint32(payload[off : off+4])
	off += 4

	// CRC-covered region starts at attributes and runs to the end of payload.
	if crc32.Checksum(payload[off:], crcTable) != checksum {
		return nil, read, ErrCorruptRecord
	}

	rec.Attributes = int8(payload[off])
	off++

	if off+4 > len(payload) {
		return nil, read, io.ErrUnexpectedEOF
	}
	keyLen := int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	switch {
	case keyLen == nullLength:
		rec.Key = nil
	case keyLen < 0:
		return nil, read, ErrCorruptRecord
	default:
		if off+int(keyLen) > len(payload) {
			return nil, read, io.ErrUnexpectedEOF
		}
		rec.Key = make([]byte, keyLen)
		copy(rec.Key, payload[off:off+int(keyLen)])
		off += int(keyLen)
	}

	if off+4 > len(payload) {
		return nil, read, io.ErrUnexpectedEOF
	}
	valueLen := int32(binary.BigEndian.Uint32(payload[off : off+4]))
	off += 4
	switch {
	case valueLen == nullLength:
		rec.Value = nil
	case valueLen < 0:
		return nil, read, ErrCorruptRecord
	default:
		if off+int(valueLen) > len(payload) {
			return nil, read, io.ErrUnexpectedEOF
		}
		rec.Value = make([]byte, valueLen)
		copy(rec.Value, payload[off:off+int(valueLen)])
	}

	return rec, read, nil
}
