// Package mmap is not used for allocating disk files in memory, but rather to
// allocate large contiguous chunks of memory outside the Go runtime memory
// allocator and garbage collection. This also has the benefit of lazily
// allocating memory pages.
package mmap

import (
	"syscall"
)

// New allocates a large contiguous chunk of memory using the OS syscall mmap.
// This is manually managed memory that is not garbage collected by the Go
// runtime. You must call Free with the buffer when finished. Note that the
// size of the returned buffer may not be the equal to `size` because the OS
// will round the byte length up to a multiple of the system's page size.
func New(size int) ([]byte, error) {
	if size < 1 {
		panic("invalid mmap allocation size")
	}

	// Insert `fd` to -1 because we are using `syscall.MAP_ANON`. This indicates
	// that there is no backing disk file.
	data, err := syscall.Mmap(-1, 0, size,
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_ANON|syscall.MAP_PRIVATE,
	)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Free releases the memory allocated by the OS with the mmap syscall. The
// original []byte buffer must be passed back to this function. Do not attempt
// to resize the []byte buffer with append, instead create a new buffer and
// copy() from the old buffer.
func Free(data []byte) error {
	return syscall.Munmap(data)
}
