package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"sort"
	"sync"
)

// entrySize is the fixed on-disk size of a single index entry, in bytes. Each
// entry stores a relativeOffset (uint32) and a log-file position (uint32),
// both big-endian.
const entrySize = 8

// ErrIndexFull is returned by Append when the index has reached its configured
// maximum size and cannot accept another entry.
var ErrIndexFull = errors.New("index: no space left in index")

// Index is a sparse offset index mapping relative offsets to byte positions in
// the companion .log file. The on-disk format is a sequence of fixed-width
// 8-byte entries, each laid out (big-endian) as:
//
//	relativeOffset uint32  // offset - segment.baseOffset
//	position       uint32  // byte offset within the .log file
//
// The backing file is preallocated to maxBytes at open time and the whole
// region is memory-mapped shared. Append writes directly into the mapping and
// advances the logical size; Truncate releases the preallocated slack by
// shrinking the file to the logical size before closing.
type Index struct {
	file     *os.File
	mmapData []byte
	unmap    func() error // releases mmapData; nil when already unmapped
	size     int64        // logical bytes in use (entries * entrySize)
	maxSize  int64        // maximum index size in bytes (the mmap length)
	mu       sync.RWMutex
}

// NewIndex opens (or creates) the index file at path and memory-maps it. The
// file is preallocated to maxBytes so that the mapping covers the full usable
// region and Append never needs to remap.
//
// If the file already exists and contains entries, the logical size is derived
// from its current length (rounded down to a whole entry). maxBytes is assumed
// to be a multiple of entrySize; if it is not, it is rounded down.
func NewIndex(path string, maxBytes int64) (*Index, error) {
	if maxBytes < 0 {
		return nil, errors.New("index: negative max size")
	}
	// Round maxBytes down to a whole number of entries.
	maxBytes -= maxBytes % entrySize
	if maxBytes == 0 {
		return nil, errors.New("index: max size too small for one entry")
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	// Determine the logical size from any pre-existing content.
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	logical := info.Size()
	if logical < 0 {
		logical = 0
	}
	// Round down to a whole entry in case of truncation/corruption.
	logical -= logical % entrySize
	if logical > maxBytes {
		logical = maxBytes
	}

	// Preallocate the file to maxBytes so the mapping below is always valid.
	// This grows the file with zeros (sparse on supporting filesystems) and
	// establishes the backing store for the mapping.
	if err := f.Truncate(maxBytes); err != nil {
		_ = f.Close()
		return nil, err
	}

	data, unmap, err := mmapIndex(f, int(maxBytes))
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	return &Index{
		file:     f,
		mmapData: data,
		unmap:    unmap,
		size:     logical,
		maxSize:  maxBytes,
	}, nil
}

// Append writes a new (relativeOffset, position) entry at the end of the index.
// It fails with ErrIndexFull if there is no room for another entry. Append is
// safe for concurrent use.
func (i *Index) Append(relativeOffset uint32, position uint32) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.size+entrySize > i.maxSize {
		return ErrIndexFull
	}
	if i.mmapData == nil {
		return errors.New("index: not mapped")
	}

	off := i.size
	binary.BigEndian.PutUint32(i.mmapData[off:off+4], relativeOffset)
	binary.BigEndian.PutUint32(i.mmapData[off+4:off+8], position)
	i.size += entrySize
	return nil
}

// Lookup performs a binary search for relativeOffset. On an exact match it
// returns the corresponding position with found=true. When there is no exact
// match, it returns the position of the largest entry whose relativeOffset is
// strictly less than the target (the nearest lower entry) with found=false.
// If the index is empty, or no entry is smaller than the target, it returns
// found=false and a zero position.
func (i *Index) Lookup(relativeOffset uint32) (position uint32, found bool, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	if i.mmapData == nil {
		return 0, false, errors.New("index: not mapped")
	}

	n := int(i.size / entrySize)
	if n == 0 {
		return 0, false, nil
	}

	// sort.Search returns the smallest index idx in [0, n] for which the
	// predicate is true. We use "entry[idx].relativeOffset >= target".
	idx := sort.Search(n, func(j int) bool {
		off := int64(j) * entrySize
		return binary.BigEndian.Uint32(i.mmapData[off:off+4]) >= relativeOffset
	})

	if idx < n {
		off := int64(idx) * entrySize
		rel := binary.BigEndian.Uint32(i.mmapData[off : off+4])
		if rel == relativeOffset {
			return binary.BigEndian.Uint32(i.mmapData[off+4 : off+8]), true, nil
		}
	}

	// No exact match: return the nearest lower entry, if any.
	if idx > 0 {
		off := int64(idx-1) * entrySize
		return binary.BigEndian.Uint32(i.mmapData[off+4 : off+8]), false, nil
	}

	// idx == 0 and no exact match: every entry is >= target but none equals
	// it, and there is no smaller entry to fall back to.
	return 0, false, nil
}

// Truncate releases the preallocated slack by unmapping the file and shrinking
// it to the logical size in use. The underlying file is closed afterwards.
// Truncate is intended to be called when the owning segment is closed.
func (i *Index) Truncate() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	if err := i.unmapLocked(); err != nil {
		return err
	}

	if err := i.file.Truncate(i.size); err != nil {
		_ = i.file.Close()
		i.file = nil
		return err
	}

	if err := i.file.Close(); err != nil {
		i.file = nil
		return err
	}
	i.file = nil
	return nil
}

// Close unmaps the index, shrinks the backing file to the logical size in use,
// and closes it. Close is idempotent: calling it more than once (including
// after Truncate) is a no-op and returns nil.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Already closed (or already Truncated): nothing to do.
	if i.file == nil && i.unmap == nil {
		return nil
	}

	if err := i.unmapLocked(); err != nil {
		if i.file != nil {
			_ = i.file.Close()
			i.file = nil
		}
		return err
	}

	if i.file != nil {
		// Shrink the file to the logical size so that reopening reports the
		// correct number of entries instead of the preallocated size.
		if err := i.file.Truncate(i.size); err != nil {
			_ = i.file.Close()
			i.file = nil
			return err
		}
		if err := i.file.Close(); err != nil {
			i.file = nil
			return err
		}
		i.file = nil
	}
	return nil
}

// unmapLocked unmaps the mmap region if it is currently mapped. It is a no-op
// if the region has already been unmapped. The caller must hold i.mu.
func (i *Index) unmapLocked() error {
	if i.unmap == nil {
		return nil
	}
	if err := i.unmap(); err != nil {
		return err
	}
	i.unmap = nil
	i.mmapData = nil
	return nil
}

// Entries returns the number of index entries currently stored.
func (i *Index) Entries() int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return int(i.size / entrySize)
}

// Size returns the logical size of the index in bytes (entries * entrySize).
func (i *Index) Size() int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.size
}
