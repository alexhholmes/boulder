package mmap

import (
	"fmt"
	"syscall"
)

// New allocates a large contiguous chunk of memory using the OS syscall mmap.
// This is manually managed memory that is not garbage collected by the Go
// runtime. You must call Free with the buffer when finished. Note that the
// size of the returned buffer may not be the equal to `size` because the OS
// will round the byte length up to a multiple of the system's page size.
func New(size int) ([]byte, error) {
	if size < 1 {
		return nil, fmt.Errorf("mmap: invalid size; size must be greater than 0: %d", size)
	}

	// Set `fd` to -1 because we are using `syscall.MAP_ANON`. This indicates
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

func Free(data []byte) error {
	return syscall.Munmap(data)
}
