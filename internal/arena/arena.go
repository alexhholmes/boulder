package arena

import (
	"errors"
	"sync"
	"unsafe"

	"boulder/internal/arch"
	"boulder/internal/mmap"
)

var ErrArenaFull = errors.New("allocation failed because arena is full")

// Arena is arena lock-free arena allocator.
type Arena struct {
	position arch.AtomicUint
	buffer   []byte
	overflow uint
	mmapped  bool
	closed   sync.Once
}

// New allocates arena new arena using the specified buffer as the backing
// store. The caller should ensure that the buffer is not modified for the
// lifetime of the arena.
func New(size uint) *Arena {
	a := &Arena{
		mmapped: true,
	}

	// Position/offset 0 is reserved as the arena's nil pointer
	a.position.Store(1)

	buf, err := mmap.New(int(size))
	if err != nil {
		buf = make([]byte, size)
		a.mmapped = false
	}
	a.buffer = buf

	return a
}

// WithOverflow provides extra space at the end of buffer where if an arena is
// "full", then any pointer that is cast to a type that goes a bit beyond the
// allocation will not cause an out of bounds of the backing slice.
func WithOverflow(size, overflow uint) *Arena {
	a := New(size + overflow)
	a.overflow = overflow
	return a
}

func (a *Arena) Allocate(size, alignment uint) (offset uint, err error) {
	// Verify that the arena isn't already full
	position := uint(a.position.Load())
	if position > uint(len(a.buffer))-a.overflow {
		return 0, ErrArenaFull
	}

	// Pad the allocation with enough bytes to ensure the requested alignment
	padded := size + alignment - 1

	// Check if arena is full after allocating
	position = uint(a.position.Add(arch.UintToArchSize(padded)))
	if position > uint(len(a.buffer))-a.overflow {
		return 0, ErrArenaFull
	}

	// Return the aligned offset
	offset = (position - padded + alignment) & ^(alignment - 1)
	return offset, nil
}

func (a *Arena) GetBytes(offset uint, size uint) []byte {
	if offset == 0 {
		return nil
	}

	// Return arena slice with capacity equal to the size of the allocation so
	// that the caller can't overwrite past the end of the allocation.
	return a.buffer[offset : offset+size : offset+size]
}

func (a *Arena) GetPointer(offset uint) unsafe.Pointer {
	if offset == 0 {
		return nil
	}

	return unsafe.Pointer(&a.buffer[offset])
}

func (a *Arena) GetPointerOffset(ptr unsafe.Pointer) uint {
	if ptr == nil {
		return 0
	}

	return uint(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buffer[0])))
}

func (a *Arena) Len() uint {
	s := a.position.Load()
	return uint(s) - 1
}

func (a *Arena) Cap() uint {
	return uint(len(a.buffer)) - a.overflow - 1
}

func (a *Arena) Reset() {
	a.position.Store(1)
}

func (a *Arena) Close() error {
	var err error
	a.closed.Do(func() {
		if a.mmapped {
			err = mmap.Free(a.buffer)
		}
	})
	return err
}
