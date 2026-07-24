package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// newTestSegment creates a segment in a fresh temporary directory.
func newTestSegment(t *testing.T, baseOffset int64, cfg Config) *Segment {
	t.Helper()
	return newTestSegmentWithDir(t, t.TempDir(), baseOffset, cfg)
}

// newTestSegmentWithDir creates a segment in the given directory.
func newTestSegmentWithDir(t *testing.T, dir string, baseOffset int64, cfg Config) *Segment {
	t.Helper()
	seg, err := NewSegment(dir, baseOffset, cfg)
	if err != nil {
		t.Fatalf("NewSegment(dir=%q, baseOffset=%d) failed: %v", dir, baseOffset, err)
	}
	return seg
}

// TestNewSegment verifies that a freshly created segment reports the correct
// base offset and zero size.
func TestNewSegment(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	defer seg.Close()

	if seg.BaseOffset != 0 {
		t.Errorf("BaseOffset = %d, want 0", seg.BaseOffset)
	}
	if seg.NextOffset != 0 {
		t.Errorf("NextOffset = %d, want 0", seg.NextOffset)
	}
	if got := seg.Size(); got != 0 {
		t.Errorf("Size() = %d, want 0", got)
	}
	if seg.IsFull() {
		t.Errorf("IsFull() = true, want false")
	}
}

// TestSegmentAppend checks that offsets are assigned sequentially and that the
// segment size grows.
func TestSegmentAppend(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	defer seg.Close()

	for i := 0; i < 10; i++ {
		rec := &Record{Value: []byte("value")}
		off, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("Append #%d returned offset = %d, want %d", i, off, i)
		}
	}
	if seg.NextOffset != 10 {
		t.Errorf("NextOffset = %d, want 10", seg.NextOffset)
	}
	if got := seg.Size(); got <= 0 {
		t.Errorf("Size() = %d, want > 0", got)
	}
}

// TestSegmentAppendBatch writes many records and reads them back to verify
// round-trip integrity.
func TestSegmentAppendBatch(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	defer seg.Close()

	const n = 100
	for i := 0; i < n; i++ {
		rec := &Record{
			Key:   []byte("key"),
			Value: []byte("value"),
		}
		off, err := seg.Append(rec)
		if err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
		if off != int64(i) {
			t.Fatalf("Append #%d returned offset = %d, want %d", i, off, i)
		}
	}
	// Flush is required because the writer is buffered; without it the records
	// are not visible to the separate read handle.
	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	for i := 0; i < n; i++ {
		got, err := seg.Read(int64(i))
		if err != nil {
			t.Fatalf("Read(%d) failed: %v", i, err)
		}
		if got.Offset != int64(i) {
			t.Errorf("Read(%d) Offset = %d, want %d", i, got.Offset, i)
		}
		if string(got.Key) != "key" {
			t.Errorf("Read(%d) Key = %q, want %q", i, got.Key, "key")
		}
		if string(got.Value) != "value" {
			t.Errorf("Read(%d) Value = %q, want %q", i, got.Value, "value")
		}
	}
}

// TestSegmentRead verifies that a single appended record can be read back
// exactly.
func TestSegmentRead(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	defer seg.Close()

	want := &Record{
		Offset:    0,
		Timestamp: 12345,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}
	if _, err := seg.Append(want); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	// The writer is buffered, so an explicit flush is required before the
	// record can be read back through a separate file handle.
	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	got, err := seg.Read(0)
	if err != nil {
		t.Fatalf("Read(0) failed: %v", err)
	}
	if !recordEqual(got, want) {
		t.Errorf("Read(0) returned %+v, want %+v", got, want)
	}
}

// TestSegmentReadInvalidOffset checks that reading below the base offset is
// rejected.
func TestSegmentReadInvalidOffset(t *testing.T) {
	seg := newTestSegment(t, 10, Config{})
	defer seg.Close()

	if _, err := seg.Append(&Record{Value: []byte("value")}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	_, err := seg.Read(9)
	if !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("Read(9) err = %v, want ErrOffsetOutOfRange", err)
	}
}

// TestSegmentReadFrom reads a slice of records starting at a given offset and
// bounded by a byte budget.
func TestSegmentReadFrom(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	defer seg.Close()

	const n = 50
	for i := 0; i < n; i++ {
		rec := &Record{Value: []byte("value")}
		if _, err := seg.Append(rec); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}
	// Buffered writer must be flushed so ReadFrom can see the data through a
	// separate read handle.
	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	records, err := seg.ReadFrom(10, 500)
	if err != nil {
		t.Fatalf("ReadFrom(10, 500) failed: %v", err)
	}
	if len(records) == 0 {
		t.Fatalf("ReadFrom(10, 500) returned no records")
	}
	if records[0].Offset != 10 {
		t.Errorf("first record Offset = %d, want 10", records[0].Offset)
	}
	for i, rec := range records {
		want := int64(10 + i)
		if rec.Offset != want {
			t.Errorf("records[%d].Offset = %d, want %d", i, rec.Offset, want)
		}
	}
}

// TestSegmentIsFull fills a small segment and checks that it reports itself as
// full.
func TestSegmentIsFull(t *testing.T) {
	seg := newTestSegment(t, 0, Config{SegmentBytes: 1000})
	defer seg.Close()

	if seg.IsFull() {
		t.Fatalf("IsFull() = true before any append")
	}

	const maxIterations = 1000
	for i := 0; i < maxIterations && !seg.IsFull(); i++ {
		rec := &Record{Value: []byte("value")}
		if _, err := seg.Append(rec); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}

	if !seg.IsFull() {
		t.Errorf("IsFull() = false after filling segment")
	}
}

// TestSegmentClose verifies that Close is idempotent and that operations after
// Close return ErrSegmentClosed.
func TestSegmentClose(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	if _, err := seg.Append(&Record{Value: []byte("value")}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Errorf("second Close failed: %v", err)
	}

	if _, err := seg.Append(&Record{Value: []byte("value")}); !errors.Is(err, ErrSegmentClosed) {
		t.Errorf("Append after Close err = %v, want ErrSegmentClosed", err)
	}
	if _, err := seg.Read(0); !errors.Is(err, ErrSegmentClosed) {
		t.Errorf("Read after Close err = %v, want ErrSegmentClosed", err)
	}
}

// TestSegmentRemove checks that Remove deletes both the .log and .index files.
func TestSegmentRemove(t *testing.T) {
	dir := t.TempDir()
	seg := newTestSegmentWithDir(t, dir, 0, Config{})
	if _, err := seg.Append(&Record{Value: []byte("value")}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	logPath := filepath.Join(dir, "00000000000000000000.log")
	indexPath := filepath.Join(dir, "00000000000000000000.index")

	if _, err := os.Stat(logPath); err != nil {
		t.Fatalf("log file was not created: %v", err)
	}
	if _, err := os.Stat(indexPath); err != nil {
		t.Fatalf("index file was not created: %v", err)
	}

	if err := seg.Remove(); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if _, err := os.Stat(logPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("log file still exists after Remove")
	}
	if _, err := os.Stat(indexPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("index file still exists after Remove")
	}
}

// TestSegmentFlush verifies that Flush persists buffered data to the .log file
// without closing the segment.
func TestSegmentFlush(t *testing.T) {
	dir := t.TempDir()
	seg := newTestSegmentWithDir(t, dir, 0, Config{})
	defer seg.Close()

	if _, err := seg.Append(&Record{Value: []byte("value")}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	sizeBefore := seg.Size()
	if sizeBefore <= 0 {
		t.Fatalf("Size() = %d, want > 0", sizeBefore)
	}
	if err := seg.Flush(); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, "00000000000000000000.log"))
	if err != nil {
		t.Fatalf("Stat log file failed: %v", err)
	}
	if info.Size() != sizeBefore {
		t.Errorf("log file size = %d, want %d", info.Size(), sizeBefore)
	}
}

// TestSegmentReopen verifies that records can be read after closing and
// reopening the segment. The current implementation does not recover the
// NextOffset, so this test asserts that NextOffset is reset to the base offset.
func TestSegmentReopen(t *testing.T) {
	dir := t.TempDir()
	seg := newTestSegmentWithDir(t, dir, 0, Config{})

	want := &Record{Value: []byte("hello")}
	if _, err := seg.Append(want); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	seg2, err := NewSegment(dir, 0, Config{})
	if err != nil {
		t.Fatalf("NewSegment(reopen) failed: %v", err)
	}
	defer seg2.Close()

	if seg2.NextOffset != 0 {
		t.Errorf("NextOffset after reopen = %d, want 0 (no recovery)", seg2.NextOffset)
	}

	got, err := seg2.Read(0)
	if err != nil {
		t.Fatalf("Read(0) after reopen failed: %v", err)
	}
	if !recordEqual(got, want) {
		t.Errorf("Read(0) after reopen returned %+v, want %+v", got, want)
	}
}

// TestSegmentAppendNil checks that appending a nil record returns an error.
func TestSegmentAppendNil(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	defer seg.Close()

	if _, err := seg.Append(nil); err == nil {
		t.Errorf("Append(nil) returned nil, want error")
	}
}

// TestSegmentReadFromMaxBytesZero checks that ReadFrom with a non-positive
// maxBytes returns an empty slice without error.
func TestSegmentReadFromMaxBytesZero(t *testing.T) {
	seg := newTestSegment(t, 0, Config{})
	defer seg.Close()

	if _, err := seg.Append(&Record{Value: []byte("value")}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	records, err := seg.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("ReadFrom(0, 0) failed: %v", err)
	}
	if len(records) != 0 {
		t.Errorf("ReadFrom(0, 0) returned %d records, want 0", len(records))
	}
}
