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

func NewArena(buf []byte) *Arena {

}

func (a *Arena) Size() int {
	s := a.n.Load()
	return int(s)
}

func (a *Arena) Capacity() int {
	return len(a.buf)
}

func (a *Arena) alloc(size, alignment, overflow uint32) (uint32, uint32, error) {
	panic("")
}

func (a *Arena) getBytes(offset uint32, size uint32) []byte {
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
