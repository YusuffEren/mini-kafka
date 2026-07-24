package storage

import (
	"bytes"
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

	// Build the CRC-covered region first: attributes + keyLength + key +
	// valueLength + value. We buffer it so we can compute the CRC before
	// emitting the fixed header.
	var crcBuf bytes.Buffer
	if err := binary.Write(&crcBuf, binary.BigEndian, r.Attributes); err != nil {
		return 0, err
	}
	if r.Key != nil {
		if err := binary.Write(&crcBuf, binary.BigEndian, int32(len(r.Key))); err != nil {
			return 0, err
		}
		if _, err := crcBuf.Write(r.Key); err != nil {
			return 0, err
		}
	} else {
		if err := binary.Write(&crcBuf, binary.BigEndian, nullLength); err != nil {
			return 0, err
		}
	}
	if r.Value != nil {
		if err := binary.Write(&crcBuf, binary.BigEndian, int32(len(r.Value))); err != nil {
			return 0, err
		}
		if _, err := crcBuf.Write(r.Value); err != nil {
			return 0, err
		}
	} else {
		if err := binary.Write(&crcBuf, binary.BigEndian, nullLength); err != nil {
			return 0, err
		}
	}

	checksum := crc32.Checksum(crcBuf.Bytes(), crcTable)

	// length = bytes after the length field: offset + timestamp + crc32 +
	// attributes + keyLength + key + valueLength + value.
	length := int32(total - 4)

	written := 0
	if err := binary.Write(w, binary.BigEndian, length); err != nil {
		return written, err
	}
	written += 4
	if err := binary.Write(w, binary.BigEndian, r.Offset); err != nil {
		return written, err
	}
	written += 8
	if err := binary.Write(w, binary.BigEndian, r.Timestamp); err != nil {
		return written, err
	}
	written += 8
	if err := binary.Write(w, binary.BigEndian, checksum); err != nil {
		return written, err
	}
	written += 4

	// The CRC-covered region is written verbatim.
	n, err := w.Write(crcBuf.Bytes())
	written += n
	if err != nil {
		return written, err
	}
	return written, nil
}

// DecodeRecord reads a single record from r and returns it together with the
// total number of bytes consumed (including the length field).
//
// If the stored CRC32 does not match the computed CRC32, ErrCorruptRecord is
// returned. If the length field exceeds MaxRecordSize, ErrRecordTooLarge is
// returned.
func DecodeRecord(r io.Reader) (*Record, int, error) {
	var length int32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, 0, err
	}
	read := 4

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

	buf := bytes.NewReader(payload)

	rec := &Record{}
	if err := binary.Read(buf, binary.BigEndian, &rec.Offset); err != nil {
		return nil, read, err
	}
	if err := binary.Read(buf, binary.BigEndian, &rec.Timestamp); err != nil {
		return nil, read, err
	}

	var checksum uint32
	if err := binary.Read(buf, binary.BigEndian, &checksum); err != nil {
		return nil, read, err
	}

	// The CRC-covered region starts at the attributes byte, which is the
	// remaining unread portion of the payload.
	crcRegion := payload[len(payload)-buf.Len():]
	if crc32.Checksum(crcRegion, crcTable) != checksum {
		return nil, read, ErrCorruptRecord
	}

	if err := binary.Read(buf, binary.BigEndian, &rec.Attributes); err != nil {
		return nil, read, err
	}

	var keyLen int32
	if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
		return nil, read, err
	}
	if keyLen == nullLength {
		rec.Key = nil
	} else if keyLen < 0 {
		return nil, read, ErrCorruptRecord
	} else {
		rec.Key = make([]byte, keyLen)
		if _, err := io.ReadFull(buf, rec.Key); err != nil {
			return nil, read, err
		}
	}

	var valueLen int32
	if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
		return nil, read, err
	}
	if valueLen == nullLength {
		rec.Value = nil
	} else if valueLen < 0 {
		return nil, read, ErrCorruptRecord
	} else {
		rec.Value = make([]byte, valueLen)
		if _, err := io.ReadFull(buf, rec.Value); err != nil {
			return nil, read, err
		}
	}

	return rec, read, nil
}
