package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrLogClosed is returned when an operation is attempted on a Log that has
// been closed.
var ErrLogClosed = errors.New("log: already closed")

// ErrEmptyBatch is returned by AppendBatch when the supplied record slice is
// empty. It signals a caller programming error rather than an empty log: the
// log itself may well hold records, but the batch contained none.
var ErrEmptyBatch = errors.New("log: empty batch")

// ErrEmptyLog is retained as an alias for ErrEmptyBatch for backwards
// compatibility with callers and tests that still reference it. New code
// should use ErrEmptyBatch.
//
// Deprecated: use ErrEmptyBatch.
var ErrEmptyLog = ErrEmptyBatch

// Log is a partition log: an ordered, append-only collection of segments
// backed by files inside a single directory. At any time exactly one segment
// is "active" (receiving appends); the others are sealed and serve reads only.
//
// Concurrency: a sync.RWMutex guards the segment slice and the active pointer.
// Appends take the write lock; reads take the read lock. A background
// goroutine performs retention and periodic flushing and is stopped by
// closing the Log.
type Log struct {
	dir      string
	config   Config
	segments []*Segment // sorted by BaseOffset ascending
	active   *Segment   // == segments[len-1]; the segment receiving appends

	// activeCreatedAt is the wall-clock time at which the active segment was
	// created (either at NewLog or at the most recent rotation). It drives
	// time-based rotation (SegmentMs).
	activeCreatedAt time.Time

	// messagesSinceFlush counts records appended to the active segment since
	// the last automatic flush. It drives FlushMessages.
	messagesSinceFlush int64

	mu     sync.RWMutex
	closed bool

	// done is closed to signal the background goroutines to stop.
	done chan struct{}
	// wg tracks the background goroutines so Close can wait for them.
	wg sync.WaitGroup
}

// NewLog opens (or creates) the partition log rooted at dir.
//
// On open the directory is scanned for existing segment files; each is
// reopened and the active segment (the one with the highest base offset) is
// recovered: its .log file is scanned end-to-end, the index is rebuilt from
// the valid records and NextOffset is set to one past the last valid record.
// The first corrupt or truncated record terminates the scan and the log file
// is truncated at that point so subsequent appends start from a clean
// boundary.
//
// If no segments are present a fresh segment at base offset 0 is created.
func NewLog(dir string, cfg Config) (*Log, error) {
	cfg = cfg.withDefaults()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("log: create directory %q: %w", dir, err)
	}

	baseOffsets, err := scanBaseOffsets(dir)
	if err != nil {
		return nil, err
	}

	l := &Log{
		dir:    dir,
		config: cfg,
		done:   make(chan struct{}),
	}

	if len(baseOffsets) == 0 {
		seg, err := NewSegment(dir, 0, cfg)
		if err != nil {
			return nil, err
		}
		l.segments = []*Segment{seg}
		l.active = seg
		l.activeCreatedAt = time.Now()
	} else {
		for _, base := range baseOffsets {
			seg, err := NewSegment(dir, base, cfg)
			if err != nil {
				// Clean up the segments we already opened.
				_ = l.closeSegments()
				return nil, err
			}
			l.segments = append(l.segments, seg)
		}
		sort.Slice(l.segments, func(i, j int) bool {
			return l.segments[i].BaseOffset < l.segments[j].BaseOffset
		})
		l.active = l.segments[len(l.segments)-1]
		l.activeCreatedAt = time.Now()

		// Recover the active segment: rebuild its index and NextOffset from
		// the on-disk log, truncating any trailing corrupt/partial record.
		if err := rebuildSegment(l.active, -1); err != nil {
			_ = l.closeSegments()
			return nil, fmt.Errorf("log: recover active segment: %w", err)
		}
	}

	l.startRetention()
	return l, nil
}

// Append assigns rec the next available offset and writes it to the active
// segment. If the active segment is full or too old it is rotated first.
// Append returns the offset assigned to rec.
func (l *Log) Append(rec *Record) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrLogClosed
	}
	if rec == nil {
		return 0, errors.New("log: append nil record")
	}

	if err := l.maybeRotateLocked(); err != nil {
		return 0, err
	}

	offset, err := l.active.Append(rec)
	if err != nil {
		return 0, fmt.Errorf("log: append: %w", err)
	}

	// Best-effort automatic flush based on message count. Errors are ignored
	// here because the data is still in the buffered writer and will be
	// flushed on the next read, rotation or close.
	if l.config.FlushMessages > 0 {
		l.messagesSinceFlush++
		if l.messagesSinceFlush >= l.config.FlushMessages {
			_ = l.active.Flush()
			l.messagesSinceFlush = 0
		}
	}
	return offset, nil
}

// AppendBatch appends recs one by one and returns the offset assigned to the
// first record. If any record fails the records already appended remain
// committed (at-least-once semantics); the error from the failing append is
// returned.
func (l *Log) AppendBatch(recs []*Record) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return 0, ErrLogClosed
	}
	if len(recs) == 0 {
		return 0, ErrEmptyBatch
	}

	var base int64
	for i, rec := range recs {
		if rec == nil {
			if i == 0 {
				return 0, errors.New("log: append nil record")
			}
			return base, errors.New("log: append nil record")
		}
		if err := l.maybeRotateLocked(); err != nil {
			if i == 0 {
				return 0, err
			}
			return base, err
		}
		off, err := l.active.Append(rec)
		if err != nil {
			if i == 0 {
				return 0, fmt.Errorf("log: append: %w", err)
			}
			return base, fmt.Errorf("log: append: %w", err)
		}
		if i == 0 {
			base = off
		}
		if l.config.FlushMessages > 0 {
			l.messagesSinceFlush++
			if l.messagesSinceFlush >= l.config.FlushMessages {
				_ = l.active.Flush()
				l.messagesSinceFlush = 0
			}
		}
	}
	return base, nil
}

// Read returns the record stored at offset. It returns ErrOffsetOutOfRange if
// offset is below the log's lowest offset, and an error wrapping
// "segment: offset ... not found" if the offset is within range but no such
// record exists (e.g. it was never written or has been truncated).
func (l *Log) Read(offset int64) (*Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogClosed
	}
	if len(l.segments) == 0 || offset < l.segments[0].BaseOffset {
		return nil, ErrOffsetOutOfRange
	}

	seg := l.findSegmentLocked(offset)
	if seg == nil {
		return nil, ErrOffsetOutOfRange
	}

	// The active segment may have unflushed data in its buffered writer; make
	// it visible to the separate read handle without paying for an fsync.
	if seg == l.active {
		if err := seg.FlushWriter(); err != nil {
			return nil, fmt.Errorf("log: flush active for read: %w", err)
		}
	}

	rec, err := seg.Read(offset)
	if err != nil {
		return nil, fmt.Errorf("log: read: %w", err)
	}
	return rec, nil
}

// ReadFrom returns a slice of records starting at offset, reading forward
// across segment boundaries until at most maxBytes of encoded record data
// have been accumulated. A partial record is never returned. If maxBytes is
// zero or negative no records are returned.
func (l *Log) ReadFrom(offset int64, maxBytes int32) ([]*Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrLogClosed
	}
	if maxBytes <= 0 {
		return nil, nil
	}
	if len(l.segments) == 0 || offset < l.segments[0].BaseOffset {
		return nil, ErrOffsetOutOfRange
	}

	var out []*Record
	var accumulated int32

	for i := 0; i < len(l.segments); i++ {
		seg := l.segments[i]
		next := int64(-1)
		if i+1 < len(l.segments) {
			next = l.segments[i+1].BaseOffset
		}

		// Skip segments that end before the requested offset.
		if next >= 0 && offset >= next {
			continue
		}
		// Skip segments that start after the requested offset (should not
		// happen because of the skip above, but guard anyway).
		if offset < seg.BaseOffset {
			break
		}

		if seg == l.active {
			if err := seg.FlushWriter(); err != nil {
				return out, fmt.Errorf("log: flush active for read: %w", err)
			}
		}

		// Ask the segment for up to the remaining byte budget. The segment
		// stops cleanly at the budget boundary, so we can simply append and
		// continue into the next segment if there is room left.
		remaining := maxBytes - accumulated
		recs, err := seg.ReadFrom(offset, remaining)
		if err != nil {
			// A corrupt read mid-scan is reported but we keep whatever was
			// collected so far.
			return out, fmt.Errorf("log: read from: %w", err)
		}
		for _, r := range recs {
			out = append(out, r)
			accumulated += int32(r.EncodedSize())
		}
		if accumulated >= maxBytes {
			break
		}
		// Advance the offset to the segment's NextOffset for the next
		// segment. If the segment had no records for this offset (e.g. the
		// offset is beyond what was written), stop here.
		if seg.NextOffset <= offset {
			break
		}
		offset = seg.NextOffset
	}
	return out, nil
}

// LowestOffset returns the base offset of the oldest segment, i.e. the
// smallest offset still present in the log.
func (l *Log) LowestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if len(l.segments) == 0 {
		return 0
	}
	return l.segments[0].BaseOffset
}

// HighestOffset returns the log end offset (LEO): the offset that will be
// assigned to the next record appended.
func (l *Log) HighestOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.active == nil {
		return 0
	}
	return l.active.NextOffset
}

// Truncate discards every record whose offset is >= offset. Segments whose
// base offset is >= offset are removed entirely; the segment containing
// offset is rebuilt keeping only records strictly below offset. After
// truncation the log's highest offset is offset. At least one segment
// always remains.
func (l *Log) Truncate(offset int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ErrLogClosed
	}
	if len(l.segments) == 0 {
		return nil
	}

	// Remove segments whose base offset is >= offset. They are entirely
	// beyond the truncation point.
	kept := l.segments[:0]
	for _, seg := range l.segments {
		if seg.BaseOffset >= offset {
			if err := seg.Remove(); err != nil {
				return fmt.Errorf("log: truncate: remove segment %d: %w", seg.BaseOffset, err)
			}
		} else {
			kept = append(kept, seg)
		}
	}
	l.segments = kept

	if len(l.segments) == 0 {
		// Everything was removed; start a fresh segment at the truncation
		// offset so the log is still writable.
		seg, err := NewSegment(l.dir, offset, l.config)
		if err != nil {
			return err
		}
		l.segments = []*Segment{seg}
		l.active = seg
		l.activeCreatedAt = time.Now()
		l.messagesSinceFlush = 0
		return nil
	}

	// Rebuild the last remaining segment so that records with offset >=
	// offset are discarded.
	last := l.segments[len(l.segments)-1]
	if err := rebuildSegment(last, offset); err != nil {
		return fmt.Errorf("log: truncate: rebuild segment %d: %w", last.BaseOffset, err)
	}
	l.active = last
	l.activeCreatedAt = time.Now()
	l.messagesSinceFlush = 0
	return nil
}

// NumSegments returns the number of segments currently in the log.
func (l *Log) NumSegments() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.segments)
}

// Close stops the background goroutines, flushes and closes every segment.
// Close is idempotent.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.done)
	l.mu.Unlock()

	// Wait for the background goroutines to stop before closing segments so
	// they do not race against the retention/flush logic.
	l.wg.Wait()

	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closeSegments()
}

// closeSegments closes every segment in the log. The caller must hold l.mu.
func (l *Log) closeSegments() error {
	var firstErr error
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	l.segments = nil
	l.active = nil
	return firstErr
}

// maybeRotateLocked rotates the active segment if it is full or older than
// SegmentMs. The caller must hold l.mu in write mode.
func (l *Log) maybeRotateLocked() error {
	if l.active == nil {
		return ErrLogClosed
	}
	if !l.shouldRotateLocked() {
		return nil
	}
	return l.rotateLocked()
}

// shouldRotateLocked reports whether the active segment should be rotated.
// The caller must hold l.mu.
func (l *Log) shouldRotateLocked() bool {
	if l.active.IsFull() {
		return true
	}
	// Time-based rotation only fires once the segment has at least one
	// record; an empty active segment is never rotated (it would just be
	// replaced by another empty one).
	if l.config.SegmentMs > 0 && l.active.NextOffset > l.active.BaseOffset {
		if time.Since(l.activeCreatedAt) >= time.Duration(l.config.SegmentMs)*time.Millisecond {
			return true
		}
	}
	return false
}

// rotateLocked seals the active segment and opens a new one whose base offset
// is the active segment's NextOffset. The caller must hold l.mu.
func (l *Log) rotateLocked() error {
	// Flush the outgoing segment so reads against it (via a separate handle)
	// see all previously buffered data.
	if err := l.active.Flush(); err != nil {
		return fmt.Errorf("log: rotate: flush: %w", err)
	}

	// Seal the outgoing segment's index: shrink the preallocated .index
	// file down to the number of entries actually written and remap that
	// region. Without this the sealed .index stays at its 10 MiB
	// preallocated size on disk, wasting space and confusing recovery
	// (a subsequent crash would leave a file full of trailing zero
	// entries that NewIndex must then diagnose).
	if err := l.active.index.Seal(); err != nil {
		return fmt.Errorf("log: rotate: seal index: %w", err)
	}

	base := l.active.NextOffset
	seg, err := NewSegment(l.dir, base, l.config)
	if err != nil {
		return fmt.Errorf("log: rotate: new segment: %w", err)
	}
	l.segments = append(l.segments, seg)
	l.active = seg
	l.activeCreatedAt = time.Now()
	l.messagesSinceFlush = 0
	return nil
}

// findSegmentLocked returns the segment that should contain offset, or nil if
// offset is below the lowest segment. Segments are sorted by base offset; the
// matching segment is the one with the largest base offset <= offset. The
// caller must hold l.mu (at least read).
func (l *Log) findSegmentLocked(offset int64) *Segment {
	if len(l.segments) == 0 {
		return nil
	}
	if offset < l.segments[0].BaseOffset {
		return nil
	}
	// Binary search for the largest base offset <= offset.
	idx := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].BaseOffset > offset
	})
	if idx == 0 {
		return nil
	}
	return l.segments[idx-1]
}

// startRetention launches the background retention and flushing goroutine. It
// is called once from NewLog and must not be called again.
func (l *Log) startRetention() {
	l.wg.Add(1)
	go l.retentionLoop()
}

// retentionLoop periodically applies retention and flush policies. It exits
// when the Log's done channel is closed.
func (l *Log) retentionLoop() {
	defer l.wg.Done()

	// Pick a tick interval that is responsive but not busy. One second is a
	// reasonable default for an educational implementation.
	interval := time.Second
	if l.config.FlushMs > 0 {
		flushInterval := time.Duration(l.config.FlushMs) * time.Millisecond
		if flushInterval < interval {
			interval = flushInterval
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			l.runRetention()
			l.runFlush()
		}
	}
}

// runRetention deletes segments that have exceeded the retention policy. At
// least one segment (the active one) is always kept.
func (l *Log) runRetention() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed || len(l.segments) <= 1 {
		return
	}

	now := time.Now()
	var kept []*Segment
	for _, seg := range l.segments {
		// Never delete the active segment.
		if seg == l.active {
			kept = append(kept, seg)
			continue
		}
		// Time-based retention: a sealed segment is eligible for deletion
		// once its own age (now - CreatedAt) exceeds RetentionMs. Using the
		// segment's own creation timestamp (rather than the active
		// segment's) ensures retention actually fires for old sealed
		// segments regardless of how recently the active one was rotated.
		if l.config.RetentionMs > 0 && !seg.CreatedAt.IsZero() {
			if now.Sub(seg.CreatedAt) >= time.Duration(l.config.RetentionMs)*time.Millisecond {
				// The segment has outlived the retention window; remove it
				// from disk. If removal fails we keep the segment so that a
				// transient I/O error does not silently drop it from the
				// in-memory slice (which would leak the files and lose the
				// retry opportunity on the next retention tick).
				if err := seg.Remove(); err != nil {
					kept = append(kept, seg)
				}
				continue
			}
		}
		kept = append(kept, seg)
	}
	l.segments = kept

	// Byte-based retention: delete oldest non-active segments until the total
	// size is within RetentionBytes.
	if l.config.RetentionBytes > 0 {
		total := l.totalSizeLocked()
		for total > l.config.RetentionBytes && len(l.segments) > 1 {
			oldest := l.segments[0]
			if oldest == l.active {
				break
			}
			size := oldest.Size()
			if err := oldest.Remove(); err != nil {
				return
			}
			l.segments = l.segments[1:]
			total -= size
		}
	}
}

// runFlush fsyncs the active segment when FlushMs has elapsed since the last
// automatic flush. It is best-effort.
func (l *Log) runFlush() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed || l.active == nil {
		return
	}
	if l.config.FlushMs <= 0 {
		return
	}
	_ = l.active.Flush()
	l.messagesSinceFlush = 0
}

// totalSizeLocked returns the sum of the logical sizes of all segments. The
// caller must hold l.mu.
func (l *Log) totalSizeLocked() int64 {
	var total int64
	for _, seg := range l.segments {
		total += seg.Size()
	}
	return total
}

// scanBaseOffsets returns the base offsets of every .log file inside dir,
// sorted ascending. Files whose name does not match the
// {20-digit}.log pattern are ignored.
func scanBaseOffsets(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("log: read directory %q: %w", dir, err)
	}
	var offsets []int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		stem := strings.TrimSuffix(name, ".log")
		if len(stem) != 20 {
			continue
		}
		base, err := strconv.ParseInt(stem, 10, 64)
		if err != nil || base < 0 {
			continue
		}
		offsets = append(offsets, base)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	return offsets, nil
}

// rebuildSegment scans seg's .log file from the beginning, discards the
// existing index and rebuilds it from the valid records, and truncates the
// .log file at the first corrupt record or at the first record whose offset
// is >= stopOffset (when stopOffset >= 0). NextOffset and bytesWritten are
// updated accordingly.
//
// stopOffset < 0 means "scan to end of file" (recovery mode): the scan stops
// at the first decode error or at end of file.
//
// rebuildSegment is used both for crash recovery of the active segment on
// open and for Truncate.
func rebuildSegment(seg *Segment, stopOffset int64) error {
	seg.mu.Lock()
	defer seg.mu.Unlock()

	if seg.closed {
		return ErrSegmentClosed
	}

	// Drop any buffered data: the writer is fresh from NewSegment so it
	// should be empty, but flush defensively.
	if seg.writer != nil {
		if err := seg.writer.Flush(); err != nil {
			return fmt.Errorf("rebuild: flush writer: %w", err)
		}
	}

	// Replace the index with a fresh one. We close the current index, remove
	// its file and recreate it so the rebuilt entries start from zero.
	indexMax := seg.index.maxSize
	if err := seg.index.Close(); err != nil {
		return fmt.Errorf("rebuild: close index: %w", err)
	}
	indexPath := filepath.Join(filepath.Dir(seg.logFile.Name()),
		fmt.Sprintf("%020d.index", seg.BaseOffset))
	if err := os.Remove(indexPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("rebuild: remove index: %w", err)
	}
	freshIndex, err := NewIndex(indexPath, indexMax)
	if err != nil {
		return fmt.Errorf("rebuild: recreate index: %w", err)
	}
	seg.index = freshIndex

	// Reset the write-side accounting; we will repopulate it from the scan.
	seg.bytesWritten = 0
	seg.bytesSinceLastIndex = 0
	seg.NextOffset = seg.BaseOffset

	// Open a read handle on the log file and scan it record by record. The
	// handle is closed before truncating the log file because on some
	// platforms (notably Windows) an open file handle can block truncation of
	// the same file through a different handle.
	readFile, err := os.Open(seg.logFile.Name())
	if err != nil {
		return fmt.Errorf("rebuild: open log: %w", err)
	}

	if _, err := readFile.Seek(0, io.SeekStart); err != nil {
		_ = readFile.Close()
		return fmt.Errorf("rebuild: seek log: %w", err)
	}

	// Buffer the read handle so the record-by-record scan issues a small
	// number of large reads rather than one syscall per decoded field.
	br := bufio.NewReaderSize(readFile, 64*1024)

	for {
		pos := seg.bytesWritten
		rec, n, err := DecodeRecord(br)
		if err != nil {
			// Corrupt or truncated tail: stop and truncate the log at the
			// start of this record (pos).
			break
		}
		if stopOffset >= 0 && rec.Offset >= stopOffset {
			// Truncation point reached: discard this record and everything
			// after it. The log file is truncated at pos below.
			break
		}

		// Mirror Segment.Append's accounting so the rebuilt index matches
		// what a fresh append would have produced.
		seg.bytesWritten += int64(n)
		seg.bytesSinceLastIndex += int64(n)
		if seg.index.Entries() == 0 || seg.bytesSinceLastIndex >= seg.indexInterval {
			relativeOffset := uint32(rec.Offset - seg.BaseOffset)
			if err := seg.index.Append(relativeOffset, uint32(pos)); err != nil {
				_ = readFile.Close()
				return fmt.Errorf("rebuild: append index: %w", err)
			}
			seg.bytesSinceLastIndex = 0
		}
		seg.NextOffset = rec.Offset + 1
	}

	if err := readFile.Close(); err != nil {
		return fmt.Errorf("rebuild: close read handle: %w", err)
	}

	// Truncate the log file to the last valid byte position so that any
	// corrupt/partial trailing data is removed and subsequent appends start
	// from a clean boundary.
	//
	// The segment's own log handle is opened with O_RDWR (no O_APPEND) so
	// that it can be used both for appending and for truncation here. On
	// Windows, O_APPEND maps to FILE_APPEND_DATA which forbids SetEndOfFile;
	// using plain O_RDWR avoids that restriction and lets Truncate succeed
	// on the same handle.
	if err := seg.logFile.Truncate(seg.bytesWritten); err != nil {
		return fmt.Errorf("rebuild: truncate log: %w", err)
	}
	// Reposition the write cursor at the new end of file. Append no longer
	// seeks per record, so this seek is mandatory: without it the next
	// buffered flush would write at the pre-truncate cursor (past EOF or
	// into a hole) and corrupt the log.
	if _, err := seg.logFile.Seek(seg.bytesWritten, io.SeekStart); err != nil {
		return fmt.Errorf("rebuild: seek log after truncate: %w", err)
	}

	return nil
}
