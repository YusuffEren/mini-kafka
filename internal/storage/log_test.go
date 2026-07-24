package storage

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// newTestLog creates a Log in a fresh temporary directory using the provided
// configuration. The caller is responsible for closing the log.
func newTestLog(t *testing.T, cfg Config) *Log {
	t.Helper()
	l, err := NewLog(t.TempDir(), cfg)
	if err != nil {
		t.Fatalf("NewLog failed: %v", err)
	}
	return l
}

// testRecord returns a Record with predictable payload for the given offset.
func testRecord(offsetHint int) *Record {
	return &Record{
		Timestamp: int64(offsetHint),
		Key:       []byte("key"),
		Value:     []byte("value"),
	}
}

// TestNewLog verifies that a freshly created log reports empty offset range.
func TestNewLog(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	if got := l.LowestOffset(); got != 0 {
		t.Errorf("LowestOffset() = %d, want 0", got)
	}
	if got := l.HighestOffset(); got != 0 {
		t.Errorf("HighestOffset() = %d, want 0", got)
	}
}

// TestLogAppend checks that sequential appends receive increasing offsets and
// advance the log end offset.
func TestLogAppend(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	for i := 0; i < 10; i++ {
		off, err := l.Append(testRecord(i))
		if err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("Append #%d offset = %d, want %d", i, off, i)
		}
	}
	if got := l.HighestOffset(); got != 10 {
		t.Errorf("HighestOffset() = %d, want 10", got)
	}
}

// TestLogAppendBatch verifies that a batch of records is assigned a contiguous
// range of offsets starting at the returned base offset.
func TestLogAppendBatch(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	const n = 100
	recs := make([]*Record, n)
	for i := range recs {
		recs[i] = testRecord(i)
	}

	base, err := l.AppendBatch(recs)
	if err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}
	if base != 0 {
		t.Errorf("AppendBatch base offset = %d, want 0", base)
	}
	if got := l.HighestOffset(); got != n {
		t.Errorf("HighestOffset() = %d, want %d", got, n)
	}

	// Durability is only guaranteed after close/reopen; verify all offsets.
	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	l2, err := NewLog(dir, Config{})
	if err != nil {
		t.Fatalf("NewLog(reopen) failed: %v", err)
	}
	defer l2.Close()

	for i := int64(0); i < n; i++ {
		rec, err := l2.Read(i)
		if err != nil {
			t.Fatalf("Read(%d) after reopen failed: %v", i, err)
		}
		if rec.Offset != i {
			t.Errorf("Read(%d) Offset = %d, want %d", i, rec.Offset, i)
		}
	}
}

// TestLogRead confirms that an appended record survives close and reopen and
// can be read back unchanged.
func TestLogRead(t *testing.T) {
	l := newTestLog(t, Config{})

	want := &Record{
		Timestamp: 1234567890,
		Key:       []byte("read-key"),
		Value:     []byte("read-value"),
	}
	if _, err := l.Append(want); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	l2, err := NewLog(dir, Config{})
	if err != nil {
		t.Fatalf("NewLog(reopen) failed: %v", err)
	}
	defer l2.Close()

	got, err := l2.Read(0)
	if err != nil {
		t.Fatalf("Read(0) failed: %v", err)
	}
	want.Offset = 0
	if !recordEqual(got, want) {
		t.Errorf("Read(0) returned %+v, want %+v", got, want)
	}
}

// TestLogReadFrom reads a range of records starting at a non-zero offset.
func TestLogReadFrom(t *testing.T) {
	l := newTestLog(t, Config{})

	const n = 50
	for i := 0; i < n; i++ {
		if _, err := l.Append(testRecord(i)); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}

	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	l2, err := NewLog(dir, Config{})
	if err != nil {
		t.Fatalf("NewLog(reopen) failed: %v", err)
	}
	defer l2.Close()

	recs, err := l2.ReadFrom(10, 1024*1024)
	if err != nil {
		t.Fatalf("ReadFrom(10, 1MB) failed: %v", err)
	}
	if len(recs) != n-10 {
		t.Fatalf("ReadFrom returned %d records, want %d", len(recs), n-10)
	}
	for i, rec := range recs {
		want := int64(10 + i)
		if rec.Offset != want {
			t.Errorf("records[%d].Offset = %d, want %d", i, rec.Offset, want)
		}
	}
}

// TestLogReadOutOfRange checks that reading below the log's lowest offset
// returns ErrOffsetOutOfRange.
func TestLogReadOutOfRange(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	if _, err := l.Read(-1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Errorf("Read(-1) err = %v, want ErrOffsetOutOfRange", err)
	}
}

// TestLogRotation forces the log to rotate by using a tiny segment size and
// asserts that more than one segment is created.
func TestLogRotation(t *testing.T) {
	l := newTestLog(t, Config{SegmentBytes: 1000})
	defer l.Close()

	const n = 100
	for i := 0; i < n; i++ {
		if _, err := l.Append(testRecord(i)); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}

	// The log must have rotated at least once.
	if l.NumSegments() <= 1 {
		t.Errorf("segments = %d, want > 1", l.NumSegments())
	}
	if got := l.HighestOffset(); got != n {
		t.Errorf("HighestOffset() = %d, want %d", got, n)
	}
}

// TestLogRecovery verifies that records survive a close and reopen and that
// the log resumes assigning the correct next offset.
func TestLogRecovery(t *testing.T) {
	l := newTestLog(t, Config{})

	const n = 25
	for i := 0; i < n; i++ {
		if _, err := l.Append(testRecord(i)); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}

	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	l2, err := NewLog(dir, Config{})
	if err != nil {
		t.Fatalf("NewLog(recovery) failed: %v", err)
	}
	defer l2.Close()

	if got := l2.HighestOffset(); got != n {
		t.Errorf("HighestOffset() after recovery = %d, want %d", got, n)
	}
	for i := int64(0); i < n; i++ {
		rec, err := l2.Read(i)
		if err != nil {
			t.Fatalf("Read(%d) after recovery failed: %v", i, err)
		}
		if rec.Offset != i {
			t.Errorf("Read(%d) Offset = %d, want %d", i, rec.Offset, i)
		}
	}

	// A new append should receive the next offset after the recovered records.
	off, err := l2.Append(testRecord(n))
	if err != nil {
		t.Fatalf("Append after recovery failed: %v", err)
	}
	if off != n {
		t.Errorf("post-recovery offset = %d, want %d", off, n)
	}
}

// TestLogTruncate discards the upper half of the log and verifies that only the
// lower half remains readable.
func TestLogTruncate(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	const n = 20
	for i := 0; i < n; i++ {
		if _, err := l.Append(testRecord(i)); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}

	if err := l.Truncate(10); err != nil {
		t.Fatalf("Truncate(10) failed: %v", err)
	}
	if got := l.HighestOffset(); got != 10 {
		t.Errorf("HighestOffset() after truncate = %d, want 10", got)
	}

	for i := int64(0); i < 10; i++ {
		if _, err := l.Read(i); err != nil {
			t.Errorf("Read(%d) after truncate failed: %v", i, err)
		}
	}
	if _, err := l.Read(10); err == nil {
		t.Errorf("Read(10) after truncate returned nil, want error")
	}
}

// TestLogClose checks that Close is idempotent and that operations after Close
// return ErrLogClosed.
func TestLogClose(t *testing.T) {
	l := newTestLog(t, Config{})

	if _, err := l.Append(testRecord(0)); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close failed: %v", err)
	}

	if _, err := l.Append(testRecord(1)); !errors.Is(err, ErrLogClosed) {
		t.Errorf("Append after Close err = %v, want ErrLogClosed", err)
	}
	if _, err := l.Read(0); !errors.Is(err, ErrLogClosed) {
		t.Errorf("Read after Close err = %v, want ErrLogClosed", err)
	}
	if err := l.Truncate(0); !errors.Is(err, ErrLogClosed) {
		t.Errorf("Truncate after Close err = %v, want ErrLogClosed", err)
	}
}

// TestLogRetentionByBytes creates a log with a small byte budget, produces many
// small segments, and checks that reopening the log and waiting for the
// retention goroutine deletes the oldest segments.
func TestLogRetentionByBytes(t *testing.T) {
	cfg := Config{
		SegmentBytes:   1000,
		RetentionBytes: 2000,
		FlushMs:        100,
	}
	l := newTestLog(t, cfg)

	// Fill enough segments so that the total size exceeds RetentionBytes.
	for i := 0; i < 300; i++ {
		if _, err := l.Append(testRecord(i)); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}

	segmentsBefore := l.NumSegments()
	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen and give the retention goroutine time to clean up.
	l2, err := NewLog(dir, cfg)
	if err != nil {
		t.Fatalf("NewLog(reopen) failed: %v", err)
	}
	defer l2.Close()
	time.Sleep(2 * time.Second)

	if segmentsBefore <= l2.NumSegments() {
		t.Errorf("segments did not shrink: before=%d, after=%d", segmentsBefore, l2.NumSegments())
	}
	if got := l2.LowestOffset(); got <= 0 {
		t.Errorf("LowestOffset() after retention = %d, want > 0", got)
	}
	// Active segment must remain intact.
	if got := l2.HighestOffset(); got <= l2.LowestOffset() {
		t.Errorf("HighestOffset()=%d <= LowestOffset()=%d after retention", got, l2.LowestOffset())
	}
}

// TestLogLowestHighestOffset verifies that the range accessors reflect the
// records currently in the log.
func TestLogLowestHighestOffset(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	if got := l.LowestOffset(); got != 0 {
		t.Errorf("empty LowestOffset() = %d, want 0", got)
	}
	if got := l.HighestOffset(); got != 0 {
		t.Errorf("empty HighestOffset() = %d, want 0", got)
	}

	const n = 15
	for i := 0; i < n; i++ {
		if _, err := l.Append(testRecord(i)); err != nil {
			t.Fatalf("Append #%d failed: %v", i, err)
		}
	}
	if got := l.LowestOffset(); got != 0 {
		t.Errorf("LowestOffset() = %d, want 0", got)
	}
	if got := l.HighestOffset(); got != n {
		t.Errorf("HighestOffset() = %d, want %d", got, n)
	}
}

// TestLogAppendNil checks that appending a nil record returns an error.
func TestLogAppendNil(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	if _, err := l.Append(nil); err == nil {
		t.Errorf("Append(nil) returned nil, want error")
	}
}

// TestLogAppendBatchEmpty checks that an empty batch returns an error.
func TestLogAppendBatchEmpty(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	if _, err := l.AppendBatch(nil); !errors.Is(err, ErrEmptyLog) {
		t.Errorf("AppendBatch(nil) err = %v, want ErrEmptyLog", err)
	}
	if _, err := l.AppendBatch([]*Record{}); !errors.Is(err, ErrEmptyLog) {
		t.Errorf("AppendBatch(empty) err = %v, want ErrEmptyLog", err)
	}
}

// TestLogReadFromMaxBytesZero checks that ReadFrom with non-positive max bytes
// returns an empty slice without error.
func TestLogReadFromMaxBytesZero(t *testing.T) {
	l := newTestLog(t, Config{})
	defer l.Close()

	if _, err := l.Append(testRecord(0)); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	recs, err := l.ReadFrom(0, 0)
	if err != nil {
		t.Fatalf("ReadFrom(0, 0) failed: %v", err)
	}
	if len(recs) != 0 {
		t.Errorf("ReadFrom(0, 0) returned %d records, want 0", len(recs))
	}
}

// TestLogReopenFiles verifies that segment files are actually written to disk.
func TestLogReopenFiles(t *testing.T) {
	l := newTestLog(t, Config{})
	if _, err := l.Append(testRecord(0)); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	dir := l.dir
	if err := l.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	matches, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatalf("Glob failed: %v", err)
	}
	if len(matches) == 0 {
		t.Errorf("no .log files found in %q", dir)
	}
	if _, err := os.Stat(filepath.Join(dir, "00000000000000000000.log")); err != nil {
		t.Errorf("expected base log file missing: %v", err)
	}
}
