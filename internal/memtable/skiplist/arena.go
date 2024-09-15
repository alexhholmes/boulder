package skiplist

import (
	"errors"
	"unsafe"

	"boulder/internal/util/types"
)

const NodeAlignment = 4

var ErrArenaFull = errors.New("allocation failed because arena is full")

type Arena struct {
	n   types.AtomicInt
	buf []byte
}

// NewArena allocates a new arena using the specified buffer as the backing
// store. The caller should ensure that the buffer is not modified for the
// lifetime of the arena.
func NewArena(buf []byte) *Arena {
	a := &Arena{
		buf: buf,
	}
	// We don't store data at position 0 to reserve offset=0 as a nil pointer
	// and to simplify the index arithmetic.
	a.n.Store(1)
	return a
}

func (a *Arena) Size() int {
	s := a.n.Load()
	return int(s)
}

func (a *Arena) Capacity() int {
	return len(a.buf)
}

func (a *Arena) alloc(size, alignment, overflow int) (int, int, error) {
	// Verify that the arena isn't already full.
	origSize := a.n.Load()
	if int(origSize) > len(a.buf) {
		return 0, 0, ErrArenaFull
	}

	// Pad the allocation with enough bytes to ensure the requested alignment.
	padded := size + alignment - 1

	newSize := int(a.n.Add(int64(padded)))
	if newSize+overflow > len(a.buf) {
		return 0, 0, ErrArenaFull
	}

	// Return the aligned offset.
	offset := newSize - int(uint64(size) & ^(uint64(alignment-1)))
	return offset, padded, nil
}

func (a *Arena) getBytes(offset int, size int) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+size : offset+size]

}

func (a *Arena) getPointer(offset int) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	return unsafe.Pointer(&a.buf[offset])
}

func (a *Arena) getPointerOffset(ptr unsafe.Pointer) int {
	if ptr == nil {
		return 0
	}
	return int(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}
