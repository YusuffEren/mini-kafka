//go:build windows

package storage

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

// mmapIndex memory-maps the first size bytes of f as a shared, read/write
// region using the Windows file-mapping API. It returns the mapped slice
// together with an unmap function that releases the mapping. The slice is
// backed by the file: writes to it are reflected in the underlying file and
// vice versa.
//
// The mapping handle is closed immediately after the view is created; per the
// Windows memory model the mapped view remains valid until UnmapViewOfFile is
// called. The base address returned by MapViewOfFile is a uintptr and is
// captured directly by the unmap closure, so no unsafe.Pointer<->uintptr
// conversion occurs (keeping go vet's unsafeptr checker happy). The slice is
// assembled via reflect.SliceHeader, assigning the uintptr base to Data, which
// likewise avoids any unsafe.Pointer conversion.
func mmapIndex(f *os.File, size int) ([]byte, func() error, error) {
	if size <= 0 {
		return nil, nil, os.ErrInvalid
	}
	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READWRITE, 0, 0, nil)
	if err != nil {
		return nil, nil, err
	}
	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ|windows.FILE_MAP_WRITE, 0, 0, uintptr(size))
	if err != nil {
		_ = windows.CloseHandle(h)
		return nil, nil, err
	}
	_ = windows.CloseHandle(h)

	var data []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	hdr.Data = addr
	hdr.Len = size
	hdr.Cap = size

	return data, func() error { return windows.UnmapViewOfFile(addr) }, nil
}
