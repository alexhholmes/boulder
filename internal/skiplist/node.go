package skiplist

import (
	"boulder/internal/arch"
	"boulder/internal/arena"
	"boulder/internal/base"
)

type splice struct {
	prev *node
	next *node
}

type links struct {
	next arch.AtomicUint
	prev arch.AtomicUint
}

type node struct {
	// Immutable fields
	keyTrailer base.InternalKeyTrailer
	keyOffset  uint
	keySize    uint
	valSize    uint

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]links
}

func (n *node) getKey(arena *arena.Arena) []byte {
	return arena.GetBytes(n.keyOffset, n.keySize)
}

func (n *node) getValue(arena *arena.Arena) []byte {
	return arena.GetBytes(n.keyOffset+n.keySize, n.valSize)
}

func (n *node) nextOffset(height int) uint {
	return uint(n.tower[height].next.Load())
}

func (n *node) prevOffset(height int) uint {
	return uint(n.tower[height].prev.Load())
}

func (n *node) nextOffsetCAS(height int, old, val uint) bool {
	return n.tower[height].next.CompareAndSwap(arch.UintToArchSize(old), arch.UintToArchSize(val))
}

func (n *node) prevOffsetCAS(height int, old, val uint) bool {
	return n.tower[height].prev.CompareAndSwap(arch.UintToArchSize(old), arch.UintToArchSize(val))
}
