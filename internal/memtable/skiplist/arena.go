package skiplist

import (
	"errors"
	"sync/atomic"
	"unsafe"
)

type Arena struct {
	n   atomic.Int64
	buf []byte
}

const nodeAlignment = 4

var ErrArenaFull = errors.New("allocation failed because arena is full")

func NewArena(buf []byte) *Arena {

}

func (a *Arena) Size() int {
	s := a.n.Load()
	return int(s)
}

func (a *Arena) Capacity() uint64 {
	return uint64(len(a.buf))
}

func (a *Arena) alloc(size, alignment, overflow uint32) (uint32, uint32, error) {
	panic("")
}

func (a *Arena) getBytes(offset uint32, size uint32) []byte {
}

func (a *Arena) getPointer(offset uint32) unsafe.Pointer {
	if offset == 0 {
		return nil
	}
	return unsafe.Pointer(&a.buf[offset])
}

func (a *Arena) getPointerOffset(ptr unsafe.Pointer) uint32 {
	if ptr == nil {
		return 0
	}
	return uint32(uintptr(ptr) - uintptr(unsafe.Pointer(&a.buf[0])))
}
