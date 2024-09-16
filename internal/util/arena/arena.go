package arena

import (
	"errors"
	"unsafe"

	"boulder/internal/util/arch"
)

type Alignment uint

const (
	Align1 Alignment = 0
	Align2 Alignment = 1
	Align4 Alignment = 3
	Align8 Alignment = 7
)

var (
	ErrArenaFull = errors.New("allocation failed because arena is full")
)

// Arena is a lock-free arena allocator.
type Arena struct {
	n   arch.AtomicUint
	buf []byte
}

// NewArena allocates a new arena using the specified buffer as the backing
// store. The caller should ensure that the buffer is not modified for the
// lifetime of the arena.
func NewArena(size uint) *Arena {
	a := &Arena{
		buf: make([]byte, size),
	}

	// We don't store data at position 0 to reserve offset=0 as a nil pointer
	// and to simplify the index arithmetic.
	a.n.Store(1)
	return a
}

func (a *Arena) Allocate(size, overflow uint, align Alignment) (offset, padded uint, err error) {
	// Verify that the arena isn't already full
	originalSize := a.n.Load()
	if uint(originalSize) > uint(len(a.buf)) {
		return 0, 0, ErrArenaFull
	}

	// Pad the allocation with enough bytes to ensure the requested alignment
	padded = size + uint(align)

	newSize := uint(a.n.Add(arch.UintToArchSize(padded)))
	if newSize+overflow > uint(len(a.buf)) {
		// Double check that the arena isn't full after calculating the new size
		return 0, 0, ErrArenaFull
	}

	// Return the aligned offset
	offset = (newSize - padded + uint(align)) & ^uint(align)
	return offset, padded, nil
}

// Len returns the number of bytes allocated by the arena, including the
// reserved 0th byte and padding.
func (a *Arena) Len() uint {
	s := a.n.Load()
	return uint(s)
}

// Cap returns the length of the underlying buffer.
func (a *Arena) Cap() uint {
	return uint(len(a.buf))
}

// Reset sets the arena size to 1, without overwriting the old buffer data.
func (a *Arena) Reset() {
	a.n.Store(1)
}

func (a *Arena) GetBytes(offset uint, size uint) []byte {
	if offset == 0 {
		return nil
	}

	// Return a slice with capacity equal to the size of the allocation so
	// that the caller can't overwrite past the end of the allocation.
	return a.buf[offset : offset+size : offset+size]
}

func (a *Arena) GetPointer(offset uint) unsafe.Pointer {
	if offset == 0 {
		return nil
	}

	return unsafe.Pointer(&a.buf[offset])
}

func (a *Arena) GetPointerOffset(ptr unsafe.Pointer) uint {
	if ptr == nil {
		return 0
	}

	return uint(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}
