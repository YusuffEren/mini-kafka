package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Default configuration values used when a Config field is zero.
const (
	defaultSegmentBytes       int64 = 128 * 1024 * 1024 // 128 MiB
	defaultIndexMaxBytes      int64 = 10 * 1024 * 1024  // 10 MiB
	defaultIndexIntervalBytes int64 = 4096              // index entry every 4 KiB of log data

	// Log-level defaults. These govern the partition Log built on top of
	// segments: retention, background flushing and time-based rotation.
	defaultRetentionMs    int64 = 7 * 24 * 60 * 60 * 1000 // 7 days in milliseconds
	defaultRetentionBytes int64 = -1                      // -1 means unlimited
	defaultFlushMs        int64 = 1000                    // flush every second
	defaultFlushMessages  int64 = 0                       // 0 means leave it to the OS
	defaultSegmentMs      int64 = 7 * 24 * 60 * 60 * 1000 // 7 days in milliseconds
)

// ErrSegmentClosed is returned when an operation is attempted on a segment
// that has already been closed or removed.
var ErrSegmentClosed = errors.New("segment: already closed")

// ErrOffsetOutOfRange is returned by Read when the requested offset is below
// the segment's base offset.
var ErrOffsetOutOfRange = errors.New("segment: offset out of range")

// Config groups the tunable parameters that govern a segment's behaviour. Zero
// values are replaced with the package defaults at NewSegment time.
type Config struct {
	// SegmentBytes is the maximum size, in bytes, of the .log file before the
	// segment is considered full and must be rotated. Defaults to 128 MiB.
	SegmentBytes int64

	// IndexMaxBytes is the maximum size, in bytes, of the .index file. The
	// number of index entries it can hold is IndexMaxBytes / 8. Defaults to
	// 10 MiB.
	IndexMaxBytes int64

	// IndexIntervalBytes is the approximate number of bytes written to the
	// .log file between two consecutive index entries. Defaults to 4096.
	IndexIntervalBytes int64

	// RetentionMs is the maximum age, in milliseconds, of a segment before it
	// is eligible for deletion by the retention goroutine. A value of -1
	// disables time-based retention. Defaults to 7 days.
	RetentionMs int64

	// RetentionBytes is the maximum total size, in bytes, of all segments in
	// a log. When the total exceeds this value the oldest segments are
	// deleted until the limit is respected. A value of -1 means unlimited.
	// Defaults to -1.
	RetentionBytes int64

	// FlushMs is the interval, in milliseconds, at which the background
	// flusher fsyncs the active segment. A value of 0 disables time-based
	// flushing. Defaults to 1000.
	FlushMs int64

	// FlushMessages is the number of records appended between automatic
	// flushes of the active segment. A value of 0 means the OS is left to
	// decide when to persist buffered data. Defaults to 0.
	FlushMessages int64

	// SegmentMs is the maximum age, in milliseconds, of the active segment
	// before it is rotated. A value of 0 disables time-based rotation.
	// Defaults to 7 days.
	SegmentMs int64
}

// withDefaults returns a copy of cfg in which any zero field has been replaced
// by the corresponding package default.
func (c Config) withDefaults() Config {
	out := c
	if out.SegmentBytes <= 0 {
		out.SegmentBytes = defaultSegmentBytes
	}
	if out.IndexMaxBytes <= 0 {
		out.IndexMaxBytes = defaultIndexMaxBytes
	}
	if out.IndexIntervalBytes <= 0 {
		out.IndexIntervalBytes = defaultIndexIntervalBytes
	}
	if out.RetentionMs == 0 {
		out.RetentionMs = defaultRetentionMs
	}
	if out.RetentionBytes == 0 {
		out.RetentionBytes = defaultRetentionBytes
	}
	if out.FlushMs == 0 {
		out.FlushMs = defaultFlushMs
	}
	if out.FlushMessages < 0 {
		out.FlushMessages = defaultFlushMessages
	}
	if out.SegmentMs == 0 {
		out.SegmentMs = defaultSegmentMs
	}
	return out
}

// Segment is a single contiguous slice of a partition log: a pair of files
// ({baseOffset}.log and {baseOffset}.index) that together store records and a
// sparse offset-to-position index.
//
// Writes go through a buffered writer for throughput; reads open a separate
// read handle on the .log file so that concurrent reads do not disturb the
// shared write cursor. The logical write position within the .log file is
// tracked by bytesWritten because the buffered writer hides the true file
// offset from os.File.Seek.
type Segment struct {
	// BaseOffset is the absolute offset of the first record stored in this
	// segment. It never changes for the lifetime of the segment.
	BaseOffset int64

	// NextOffset is the absolute offset that will be assigned to the next
	// record appended to this segment.
	NextOffset int64

	logFile       *os.File
	readFile      *os.File // shared read-only handle; ReadAt is position-independent & concurrency-safe
	writer        *bufio.Writer
	index         *Index
	maxBytes      int64
	indexInterval int64

	// bytesWritten is the number of bytes committed to the .log file for this
	// segment, i.e. the logical end-of-file position. It is used both to feed
	// the index with correct positions and to report Size/IsFull without
	// flushing the buffered writer.
	bytesWritten int64

	// bytesSinceLastIndex counts log bytes appended since the last index
	// entry was written. When it reaches indexInterval a new entry is added.
	bytesSinceLastIndex int64

	// CreatedAt is the wall-clock time at which the segment was created. It
	// drives time-based retention: a sealed segment becomes eligible for
	// deletion once its age (now.Sub(CreatedAt)) exceeds RetentionMs.
	CreatedAt time.Time

	// closed guards against double Close / use-after-close.
	closed bool

	mu sync.RWMutex
}

// NewSegment creates (or opens) a segment rooted at baseOffset inside dir. The
// directory is created if it does not already exist.
//
// The .log file is opened with O_RDWR (without O_APPEND) so that the same
// handle can be used both to append records and to truncate the file during
// recovery. After open, the write cursor is positioned once at the current
// end of file (bytesWritten) so that subsequent Append calls can stream
// through the buffered writer without a per-record Seek. rebuildSegment is
// responsible for re-seeking after a truncate. The .index file is memory-
// mapped via NewIndex. NextOffset is initialised to baseOffset. NewSegment
// does not attempt to recover NextOffset from existing on-disk contents;
// recovery is the responsibility of the upper-level Log type.
func NewSegment(dir string, baseOffset int64, cfg Config) (*Segment, error) {
	cfg = cfg.withDefaults()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("segment: create directory %q: %w", dir, err)
	}

	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", baseOffset))

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment: open log file %q: %w", logPath, err)
	}

	// If the .log file already existed with content, initialise bytesWritten
	// from its on-disk size so that Size() reports the correct value before
	// the upper-level Log has a chance to rebuild the segment. This matters
	// for sealed segments, which are not rebuilt on open: without it their
	// Size() would report 0 and byte-based retention would never fire. A
	// freshly created file reports size 0, so this is a no-op for new
	// segments. The active segment's bytesWritten is later overwritten by
	// rebuildSegment during recovery.
	//
	// CreatedAt is derived from the .log file's mtime when an existing
	// non-empty segment is reopened. This preserves the segment's true age
	// across broker restarts so that time-based retention still fires for
	// old sealed segments. A freshly created (size 0) file is assigned
	// time.Now(), matching the original behaviour for new segments.
	var bytesWritten int64
	var createdAt time.Time
	if info, statErr := logFile.Stat(); statErr == nil {
		bytesWritten = info.Size()
		if bytesWritten > 0 {
			createdAt = info.ModTime()
		} else {
			createdAt = time.Now()
		}
	} else {
		createdAt = time.Now()
	}

	// Position the write cursor at the logical end once. Append no longer
	// seeks per record; the buffered writer streams sequentially from here.
	// Without this seek, reopening a non-empty segment would overwrite from
	// offset 0 on the first flush.
	if _, err := logFile.Seek(bytesWritten, io.SeekStart); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("segment: seek log to end: %w", err)
	}

	index, err := NewIndex(indexPath, cfg.IndexMaxBytes)
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("segment: open index file %q: %w", indexPath, err)
	}

	// Open a long-lived read-only handle on the .log file. Reads go through
	// this handle via io.NewSectionReader + ReadAt, which is position-
	// independent and safe for concurrent use, so the read path no longer
	// pays an os.Open per Read/ReadFrom and avoids contention with the
	// shared write cursor on logFile.
	readFile, err := os.Open(logPath)
	if err != nil {
		_ = index.Close()
		_ = logFile.Close()
		return nil, fmt.Errorf("segment: open log for read %q: %w", logPath, err)
	}

	return &Segment{
		BaseOffset:    baseOffset,
		NextOffset:    baseOffset,
		logFile:       logFile,
		readFile:      readFile,
		writer:        bufio.NewWriterSize(logFile, 256*1024),
		index:         index,
		maxBytes:      cfg.SegmentBytes,
		indexInterval: cfg.IndexIntervalBytes,
		bytesWritten:  bytesWritten,
		CreatedAt:     createdAt,
	}, nil
}

// SetNextOffset sets the absolute offset that will be assigned to the next
// record appended to this segment. It is intended for use during Log recovery
// to restore NextOffset for segments whose .log files already contain records
// but are not scanned end-to-end by rebuildSegment (e.g. sealed segments
// reopened on startup). The upper-level Log is responsible for knowing the
// correct value; NewSegment itself only initialises NextOffset to baseOffset.
func (s *Segment) SetNextOffset(offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.NextOffset = offset
}

// Append writes rec to the segment, assigning it the next available offset.
// The assigned offset is returned. Append is safe for concurrent use, although
// in practice the upper-level Log serialises appends per partition.
//
// Append does not flush the buffered writer; callers should invoke Flush (or
// Close) when durability is required.
func (s *Segment) Append(rec *Record) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrSegmentClosed
	}
	if rec == nil {
		return 0, errors.New("segment: append nil record")
	}

	// Guard against silent uint32 overflow of the index position. The config
	// layer rejects segment_bytes > math.MaxUint32, so this should never fire
	// in a correctly configured broker. It exists as a loud failure in case the
	// guard is bypassed (e.g. a Config constructed directly in code): without
	// it, position would wrap and the index would point reads at the wrong
	// byte offset, silently corrupting the log.
	if s.bytesWritten > math.MaxUint32 {
		return 0, fmt.Errorf("segment: log position %d exceeds uint32 index range", s.bytesWritten)
	}

	// The write cursor is positioned at end-of-file once in NewSegment (and
	// again after truncate in rebuildSegment). Append streams through the
	// buffered writer without a per-record Seek: sequential writes advance
	// the underlying file offset naturally when the buffer flushes.

	// Assign the offset and the write position for this record.
	offset := s.NextOffset
	rec.Offset = offset
	position := uint32(s.bytesWritten)

	n, err := rec.Encode(s.writer)
	if err != nil {
		// The buffered writer may have partially consumed the encode; we
		// cannot trust its state, so fail the segment rather than silently
		// corrupting the log.
		return 0, fmt.Errorf("segment: encode record: %w", err)
	}

	s.bytesWritten += int64(n)
	s.bytesSinceLastIndex += int64(n)

	// Add an index entry when enough bytes have accumulated since the last
	// one. We always index the first record of a segment so that Read has a
	// starting point even when IndexIntervalBytes is large.
	if s.index.Entries() == 0 || s.bytesSinceLastIndex >= s.indexInterval {
		relativeOffset := uint32(offset - s.BaseOffset)
		if err := s.index.Append(relativeOffset, position); err != nil {
			// ErrIndexFull means the segment is full; surface it so the
			// upper layer can rotate. Other errors are unexpected.
			return 0, fmt.Errorf("segment: append to index: %w", err)
		}
		s.bytesSinceLastIndex = 0
	}

	s.NextOffset++
	return offset, nil
}

// Read returns the record stored at the given absolute offset, or an error if
// the offset is below the segment's base offset or the record cannot be
// located. Read opens a short-lived read handle on the .log file so that it
// does not interfere with the shared write cursor.
func (s *Segment) Read(offset int64) (*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}
	if offset < s.BaseOffset {
		return nil, ErrOffsetOutOfRange
	}

	relativeOffset := uint32(offset - s.BaseOffset)
	position, found, err := s.index.Lookup(relativeOffset)
	if err != nil {
		return nil, fmt.Errorf("segment: index lookup: %w", err)
	}

	// On an exact index match we can skip the record-by-record scan: the
	// position points directly at the requested record, so a single
	// DecodeRecord at that position yields it. When the index only has a
	// nearest-lower entry (found == false) we fall back to scanFrom, which
	// seeks to the nearest lower position and scans forward until the
	// target offset is reached.
	if found {
		rec, scanErr := s.readAt(int64(position), offset)
		if scanErr != nil {
			return nil, scanErr
		}
		return rec, nil
	}

	rec, err := s.scanFrom(int64(position), offset)
	if err != nil {
		return nil, err
	}
	return rec, nil
}

// readAt opens a read handle on the .log file, seeks to position and decodes a
// single record, verifying that its offset matches target. It is the fast path
// used when the index reported an exact match for the requested offset. The
// caller must hold s.mu (at least read).
func (s *Segment) readAt(position int64, target int64) (*Record, error) {
	reader, closer, err := s.readHandle()
	if err != nil {
		return nil, err
	}
	defer closer()

	if _, err := reader.Seek(position, io.SeekStart); err != nil {
		return nil, fmt.Errorf("segment: seek log: %w", err)
	}

	br := bufio.NewReaderSize(reader, 64*1024)
	rec, _, err := DecodeRecord(br)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("segment: offset %d not found", target)
		}
		return nil, fmt.Errorf("segment: decode record: %w", err)
	}
	if rec.Offset != target {
		// The index entry did not actually point at the target record
		// (e.g. the index is stale or corrupt). Fall back to a forward
		// scan from this position to locate it.
		return s.scanFrom(position, target)
	}
	return rec, nil
}

// ReadFrom returns a slice of records starting at offset, reading forward until
// at most maxBytes of encoded record data have been accumulated. The slice
// stops as soon as adding the next record would exceed maxBytes; a partial
// record is never returned.
//
// If maxBytes is zero or negative, ReadFrom returns no records and no error.
func (s *Segment) ReadFrom(offset int64, maxBytes int32) ([]*Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, ErrSegmentClosed
	}
	if maxBytes <= 0 {
		return nil, nil
	}
	if offset < s.BaseOffset {
		return nil, ErrOffsetOutOfRange
	}

	relativeOffset := uint32(offset - s.BaseOffset)
	position, _, err := s.index.Lookup(relativeOffset)
	if err != nil {
		return nil, fmt.Errorf("segment: index lookup: %w", err)
	}

	reader, closer, err := s.readHandle()
	if err != nil {
		return nil, err
	}
	defer closer()

	if _, err := reader.Seek(int64(position), io.SeekStart); err != nil {
		return nil, fmt.Errorf("segment: seek log: %w", err)
	}

	// Buffer the read handle so DecodeRecord issues a single large read per
	// chunk instead of one syscall per field. 64 KiB matches the write-side
	// order of magnitude and amortises the per-record decode cost.
	br := bufio.NewReaderSize(reader, 64*1024)

	var records []*Record
	var accumulated int32
	for {
		rec, n, err := DecodeRecord(br)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break // reached end of written data
			}
			return records, fmt.Errorf("segment: decode record: %w", err)
		}
		if rec.Offset < offset {
			// Index entries are sparse; skip records before the requested
			// offset until we reach the one we want.
			continue
		}
		if accumulated > 0 && accumulated+int32(n) > maxBytes {
			// Including this record would exceed the budget; stop here.
			break
		}
		records = append(records, rec)
		accumulated += int32(n)
	}
	return records, nil
}

// IsFull reports whether the segment can accept more records. A segment is full
// when its log file has reached maxBytes or its index has no room for another
// entry.
func (s *Segment) IsFull() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return true
	}
	if s.bytesWritten >= s.maxBytes {
		return true
	}
	// Index capacity check: each entry is entrySize bytes. maxSize is
	// immutable after NewIndex, so reading it without the index lock is safe.
	if s.index.Size()+entrySize > s.index.maxSize {
		return true
	}
	return false
}

// Size returns the logical size of the .log file in bytes, i.e. the number of
// bytes appended to this segment (including any buffered but not yet flushed
// data).
func (s *Segment) Size() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytesWritten
}

// Close flushes any buffered data, truncates and closes the index, and closes
// the log file. Close is idempotent.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var firstErr error
	if s.writer != nil {
		if err := s.writer.Flush(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: flush writer: %w", err)
		}
	}
	if s.index != nil {
		if err := s.index.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: close index: %w", err)
		}
	}
	if s.readFile != nil {
		if err := s.readFile.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: close read handle: %w", err)
		}
	}
	if s.logFile != nil {
		if err := s.logFile.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("segment: close log file: %w", err)
		}
	}
	return firstErr
}

// Remove closes the segment and deletes its .log and .index files from disk.
// It is intended for retention/compaction; callers must ensure no other code
// holds a reference to the segment afterwards.
func (s *Segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	dir := filepath.Dir(s.logFile.Name())
	logPath := filepath.Join(dir, fmt.Sprintf("%020d.log", s.BaseOffset))
	indexPath := filepath.Join(dir, fmt.Sprintf("%020d.index", s.BaseOffset))

	if err := os.Remove(logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("segment: remove log file: %w", err)
	}
	if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("segment: remove index file: %w", err)
	}
	return nil
}

// Flush flushes the buffered writer and asks the operating system to persist
// the .log file's contents to disk. It does not close the segment.
func (s *Segment) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("segment: flush writer: %w", err)
	}
	if err := s.logFile.Sync(); err != nil {
		return fmt.Errorf("segment: sync log file: %w", err)
	}
	return nil
}

// FlushWriter flushes the buffered writer only, without issuing an fsync. It
// is intended for the read path of the owning Log: because reads open a
// separate file handle, any data still sitting in the buffered writer would
// be invisible to them. FlushWriter makes that data visible without paying
// the cost of an fsync on every read.
func (s *Segment) FlushWriter() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return ErrSegmentClosed
	}
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("segment: flush writer: %w", err)
	}
	return nil
}

// scanFrom opens a read handle on the .log file, seeks to position, and scans
// forward until it finds the record whose offset matches target. The caller
// must hold s.mu.
func (s *Segment) scanFrom(position int64, target int64) (*Record, error) {
	reader, closer, err := s.readHandle()
	if err != nil {
		return nil, err
	}
	defer closer()

	if _, err := reader.Seek(position, io.SeekStart); err != nil {
		return nil, fmt.Errorf("segment: seek log: %w", err)
	}

	// Buffer the read handle so DecodeRecord issues a single large read per
	// chunk instead of one syscall per field.
	br := bufio.NewReaderSize(reader, 64*1024)

	for {
		rec, _, err := DecodeRecord(br)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, fmt.Errorf("segment: offset %d not found", target)
			}
			return nil, fmt.Errorf("segment: decode record: %w", err)
		}
		if rec.Offset == target {
			return rec, nil
		}
		if rec.Offset > target {
			// We passed the target without matching it; the requested
			// offset does not exist in this segment.
			return nil, fmt.Errorf("segment: offset %d not found", target)
		}
	}
}

// readHandle returns a fresh, position-independent reader over the segment's
// .log file backed by the long-lived shared read handle (s.readFile). Because
// io.SectionReader uses ReadAt under the hood, which is position-independent
// and safe for concurrent use, multiple readers may be created and used
// concurrently without disturbing one another or the shared write cursor on
// logFile. The returned closer is a no-op (the underlying handle is owned by
// the segment and closed in Close); it is kept so callers can `defer closer()`
// unchanged.
//
// The section is bounded by s.bytesWritten, the logical end-of-file. The caller
// holds s.mu (at least read), so bytesWritten is stable for the duration of the
// read and reflects all data flushed to the file (the Log layer FlushWriter's
// the active segment before reading).
func (s *Segment) readHandle() (io.ReadSeeker, func(), error) {
	if s.readFile == nil {
		return nil, nil, fmt.Errorf("segment: read handle not available")
	}
	sr := io.NewSectionReader(s.readFile, 0, s.bytesWritten)
	return sr, func() {}, nil
}
