//go:build unix

package storage

import (
	"os"

	"golang.org/x/sys/unix"
)

// mmapIndex memory-maps the first size bytes of f as a shared, read/write
// region. It returns the mapped slice together with an unmap function that
// releases the mapping. The slice is backed by the file: writes to it are
// reflected in the underlying file and vice versa.
func mmapIndex(f *os.File, size int) ([]byte, func() error, error) {
	data, err := unix.Mmap(int(f.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return data, func() error { return unix.Munmap(data) }, nil
}
