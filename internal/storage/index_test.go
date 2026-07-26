package storage

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// newTestIndex is a small helper that creates a temporary index file for a test.
func newTestIndex(t *testing.T, maxBytes int64) *Index {
	t.Helper()
	idx, err := NewIndex(filepath.Join(t.TempDir(), "index"), maxBytes)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	return idx
}

// TestNewIndex verifies that a freshly created index reports zero entries
// and zero logical size.
func TestNewIndex(t *testing.T) {
	idx := newTestIndex(t, 1024)
	defer idx.Close()

	if got := idx.Entries(); got != 0 {
		t.Errorf("Entries() = %d, want 0", got)
	}
	if got := idx.Size(); got != 0 {
		t.Errorf("Size() = %d, want 0", got)
	}
}

// TestNewIndexInvalidMaxSize checks that NewIndex rejects unusable max sizes.
func TestNewIndexInvalidMaxSize(t *testing.T) {
	cases := []struct {
		name string
		size int64
	}{
		{"negative", -1},
		{"zero", 0},
		{"smaller than one entry", 1},
		{"between one and two entries", 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "index")
			_, err := NewIndex(path, tc.size)
			if err == nil {
				t.Fatalf("NewIndex(maxBytes=%d) expected error, got nil", tc.size)
			}
		})
	}
}

// TestNewIndexExistingFile verifies that opening an existing index file picks
// up the entries that were previously written to it.
func TestNewIndexExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")

	// Create, populate, and close the first index.
	idx1, err := NewIndex(path, 1024)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}
	if err := idx1.Append(0, 0); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx1.Append(10, 100); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx1.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Reopen the same file and check the logical size.
	idx2, err := NewIndex(path, 1024)
	if err != nil {
		t.Fatalf("NewIndex(existing) failed: %v", err)
	}
	defer idx2.Close()

	if got := idx2.Entries(); got != 2 {
		t.Errorf("Entries() = %d, want 2", got)
	}
	if got := idx2.Size(); got != 2*entrySize {
		t.Errorf("Size() = %d, want %d", got, 2*entrySize)
	}
}

// TestIndexAppend checks that appending entries updates the logical size and
// entry count as expected.
func TestIndexAppend(t *testing.T) {
	idx := newTestIndex(t, 1024)
	defer idx.Close()

	entries := []struct {
		off uint32
		pos uint32
	}{
		{0, 0},
		{1, 10},
		{2, 20},
	}
	for _, e := range entries {
		if err := idx.Append(e.off, e.pos); err != nil {
			t.Fatalf("Append(%d, %d) failed: %v", e.off, e.pos, err)
		}
	}

	if got := idx.Entries(); got != len(entries) {
		t.Errorf("Entries() = %d, want %d", got, len(entries))
	}
	if got := idx.Size(); got != int64(len(entries)*entrySize) {
		t.Errorf("Size() = %d, want %d", got, len(entries)*entrySize)
	}
}

// TestIndexLookupExact ensures that an exact offset match returns the stored
// position.
func TestIndexLookupExact(t *testing.T) {
	idx := newTestIndex(t, 1024)
	defer idx.Close()

	if err := idx.Append(10, 100); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx.Append(20, 200); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx.Append(30, 300); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	pos, found, err := idx.Lookup(20)
	if err != nil {
		t.Fatalf("Lookup(20) failed: %v", err)
	}
	if !found {
		t.Fatalf("Lookup(20) found = false, want true")
	}
	if pos != 200 {
		t.Errorf("Lookup(20) position = %d, want 200", pos)
	}
}

// TestIndexLookupClosest exercises Lookup when no exact match exists. The
// implementation must return the position of the largest entry whose offset
// is strictly less than the target.
func TestIndexLookupClosest(t *testing.T) {
	idx := newTestIndex(t, 1024)
	defer idx.Close()

	if err := idx.Append(0, 0); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx.Append(100, 1000); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx.Append(200, 2000); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	cases := []struct {
		off     uint32
		wantPos uint32
	}{
		{50, 0},     // falls back to the first entry
		{150, 1000}, // falls back to the second entry
		{250, 2000}, // falls back to the last entry
	}
	for _, tc := range cases {
		pos, found, err := idx.Lookup(tc.off)
		if err != nil {
			t.Fatalf("Lookup(%d) failed: %v", tc.off, err)
		}
		if found {
			t.Errorf("Lookup(%d) found = true, want false", tc.off)
		}
		if pos != tc.wantPos {
			t.Errorf("Lookup(%d) position = %d, want %d", tc.off, pos, tc.wantPos)
		}
	}
}

// TestIndexLookupEmpty confirms that Lookup on an empty index returns
// found=false and a zero position.
func TestIndexLookupEmpty(t *testing.T) {
	idx := newTestIndex(t, 1024)
	defer idx.Close()

	pos, found, err := idx.Lookup(0)
	if err != nil {
		t.Fatalf("Lookup on empty index failed: %v", err)
	}
	if found {
		t.Errorf("Lookup on empty index found = true, want false")
	}
	if pos != 0 {
		t.Errorf("Lookup on empty index position = %d, want 0", pos)
	}
}

// TestIndexLookupBeforeFirst checks that a target smaller than the first
// stored offset returns found=false and position=0.
func TestIndexLookupBeforeFirst(t *testing.T) {
	idx := newTestIndex(t, 1024)
	defer idx.Close()

	if err := idx.Append(10, 100); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx.Append(20, 200); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	pos, found, err := idx.Lookup(5)
	if err != nil {
		t.Fatalf("Lookup(5) failed: %v", err)
	}
	if found {
		t.Errorf("Lookup(5) found = true, want false")
	}
	if pos != 0 {
		t.Errorf("Lookup(5) position = %d, want 0", pos)
	}
}

// TestIndexTruncate verifies that Truncate shrinks the backing file to the
// logical size and leaves the index in a state where Close does not error.
func TestIndexTruncate(t *testing.T) {
	idx := newTestIndex(t, 1024)
	path := idx.file.Name()

	if err := idx.Append(0, 0); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx.Append(1, 10); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	sizeBefore := idx.Size()
	if err := idx.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) failed: %v", path, err)
	}
	if info.Size() != sizeBefore {
		t.Errorf("file size after Truncate = %d, want %d", info.Size(), sizeBefore)
	}

	if err := idx.Close(); err != nil {
		t.Errorf("Close after Truncate failed: %v", err)
	}
}

// TestIndexTruncateEmpty checks that truncating an empty index shrinks the
// file to zero bytes.
func TestIndexTruncateEmpty(t *testing.T) {
	idx := newTestIndex(t, 1024)
	path := idx.file.Name()

	if err := idx.Truncate(); err != nil {
		t.Fatalf("Truncate failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(%q) failed: %v", path, err)
	}
	if info.Size() != 0 {
		t.Errorf("file size after empty Truncate = %d, want 0", info.Size())
	}
}

// TestIndexClose verifies that a normal Close succeeds and that a second
// close is harmless.
func TestIndexClose(t *testing.T) {
	idx := newTestIndex(t, 1024)

	if err := idx.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Errorf("second Close failed: %v", err)
	}
}

// TestIndexMaxSize ensures that once the index is full, further appends return
// ErrIndexFull without panicking.
func TestIndexMaxSize(t *testing.T) {
	idx := newTestIndex(t, 16) // room for exactly two entries
	defer idx.Close()

	if err := idx.Append(0, 0); err != nil {
		t.Fatalf("first Append failed: %v", err)
	}
	if err := idx.Append(1, 10); err != nil {
		t.Fatalf("second Append failed: %v", err)
	}
	if err := idx.Append(2, 20); !errors.Is(err, ErrIndexFull) {
		t.Fatalf("third Append returned err=%v, want ErrIndexFull", err)
	}
	if got := idx.Entries(); got != 2 {
		t.Errorf("Entries() = %d, want 2", got)
	}
}

// TestIndexMaxSizeRoundDown checks that a non-multiple-of-entrySize maxBytes
// value is rounded down correctly.
func TestIndexMaxSizeRoundDown(t *testing.T) {
	idx := newTestIndex(t, 20) // rounds down to 16 bytes = 2 entries
	defer idx.Close()

	if err := idx.Append(0, 0); err != nil {
		t.Fatalf("first Append failed: %v", err)
	}
	if err := idx.Append(1, 10); err != nil {
		t.Fatalf("second Append failed: %v", err)
	}
	if err := idx.Append(2, 20); !errors.Is(err, ErrIndexFull) {
		t.Fatalf("third Append returned err=%v, want ErrIndexFull", err)
	}
}

// TestIndexAppendAfterClose verifies that operations after Close return an
// error instead of corrupting memory or panicking.
func TestIndexAppendAfterClose(t *testing.T) {
	idx := newTestIndex(t, 1024)
	if err := idx.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if err := idx.Append(0, 0); err == nil {
		t.Errorf("Append after Close returned nil, want error")
	}
}

// TestIndexLookupAfterClose verifies that Lookup after Close returns an error.
func TestIndexLookupAfterClose(t *testing.T) {
	idx := newTestIndex(t, 1024)
	if err := idx.Append(0, 0); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if _, _, err := idx.Lookup(0); err == nil {
		t.Errorf("Lookup after Close returned nil, want error")
	}
}

// TestIndexRecoveryAfterHardCrash simulates a crash before Close() could
// shrink the backing file to the logical size. The next open must recover the
// actual entry count, not treat the trailing preallocated zeros as valid entries.
func TestIndexRecoveryAfterHardCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")

	const maxBytes int64 = 1024
	idx, err := NewIndex(path, maxBytes)
	if err != nil {
		t.Fatalf("NewIndex failed: %v", err)
	}

	// Append exactly five entries.
	positions := []uint32{10, 20, 30, 40, 50}
	for i, pos := range positions {
		if err := idx.Append(uint32(i), pos); err != nil {
			t.Fatalf("Append(%d, %d) failed: %v", i, pos, err)
		}
	}

	// Simulate a hard crash: unmap and close the file WITHOUT truncating it
	// back to the logical size. On a real crash the mapping is released and
	// the file stays at its preallocated size.
	if err := idx.unmapLocked(); err != nil {
		t.Fatalf("unmapLocked failed: %v", err)
	}
	if err := idx.file.Close(); err != nil {
		t.Fatalf("close file failed: %v", err)
	}
	idx.file = nil // make the original index harmless if Close is called

	// Reopen the still-preallocated file.
	idx2, err := NewIndex(path, maxBytes)
	if err != nil {
		t.Fatalf("NewIndex(reopen) failed: %v", err)
	}
	defer idx2.Close()

	if got := idx2.Entries(); got != 5 {
		t.Errorf("Entries() after crash = %d, want 5", got)
	}
	pos, found, err := idx2.Lookup(4)
	if err != nil {
		t.Fatalf("Lookup(4) failed: %v", err)
	}
	if !found {
		t.Errorf("Lookup(4) found = false, want true")
	}
	if pos != 50 {
		t.Errorf("Lookup(4) position = %d, want 50", pos)
	}
}

// TestIndexConcurrent stresses concurrent Append and Lookup operations to
// expose data races or mutex problems. The test is meant to be run with
// the -race flag, but it is also valid without it.
func TestIndexConcurrent(t *testing.T) {
	idx := newTestIndex(t, 4096)
	defer idx.Close()

	workers := 8
	appendsPerWorker := 32

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < appendsPerWorker; j++ {
				off := uint32(id*appendsPerWorker + j)
				if err := idx.Append(off, off*10); err != nil {
					t.Errorf("Append(%d) failed: %v", off, err)
				}
			}
		}(i)
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < appendsPerWorker; j++ {
				off := uint32(id*appendsPerWorker + j)
				idx.Lookup(off)
			}
		}(i)
	}
	wg.Wait()

	want := workers * appendsPerWorker
	if got := idx.Entries(); got != want {
		t.Errorf("Entries() = %d, want %d", got, want)
	}
}
