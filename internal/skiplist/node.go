package skiplist

import (
	"boulder/internal/arch"
	"boulder/internal/arena"
	"boulder/internal/base"
)

func MaxNodeSize(keySize, valSize uint) uint {
	const maxPadding = nodeAlignment - 1
	return maxNodeSize + keySize + valSize + maxPadding
}

type links struct {
	nextOffset arch.AtomicUint
	prevOffset arch.AtomicUint
}

func (l *links) init(prevOffset, nextOffset uint) {
	l.nextOffset.Store(arch.UintToArchSize(nextOffset))
	l.prevOffset.Store(arch.UintToArchSize(prevOffset))
}

type node struct {
	// Immutable fields, so no need to lock to access key.
	keyOffset  uint
	keySize    uint
	valueSize  uint
	allocSize  uint
	keyTrailer base.InternalKeyTrailer

	// Most nodes do not need to use the full height of the tower, since the
	// probability of each successive level decreases exponentially. Because
	// these elements are never accessed, they do not need to be allocated.
	// Therefore, when a node is allocated in the arena, its memory footprint
	// is deliberately truncated to not include unneeded tower elements.
	//
	// All accesses to elements should use CAS operations, with no need to lock.
	tower [maxHeight]links
}

func newNode(
	a *arena.Arena, height uint, key base.InternalKey, value []byte,
) (*node, error) {

	if height < 1 || height > maxHeight {
		panic("height cannot be less than one or greater than the max height")
	}

	keySize := uint(len(key.UserKey))
	valueSize := uint(len(value))

	nd, err := newRawNode(a, height, keySize, valueSize)
	if err != nil {
		return nil, err
	}

	nd.keyTrailer = key.Trailer
	copy(nd.getKey(a), key.UserKey)
	copy(nd.getValueBytes(a), value)

	return nd, err
}

func newRawNode(a *arena.Arena, height, keySize, valueSize uint) (*node, error) {
	// Compute the amount of the tower that will never be used, since the height
	// is less than maxHeight.
	unusedSize := (maxHeight - height) * linksSize
	nodeSize := maxNodeSize - unusedSize

	nodeOffset, allocSize, err := a.Allocate(nodeSize+keySize+valueSize, unusedSize, nodeAlignment)
	if err != nil {
		return nil, err
	}

	nd := (*node)(a.GetPointer(nodeOffset))
	nd.keyOffset = nodeOffset + nodeSize
	nd.keySize = keySize
	nd.valueSize = valueSize
	nd.allocSize = allocSize

	return nd, nil
}

func (n *node) getKey(arena *arena.Arena) []byte {
	return arena.GetBytes(n.keyOffset, n.keySize)
}

func (n *node) getValueBytes(arena *arena.Arena) []byte {
	return arena.GetBytes(n.keyOffset+n.keySize, n.valueSize)
}

func (n *node) nextOffset(h int) uint {
	return uint(n.tower[h].nextOffset.Load())
}

func (n *node) prevOffset(h int) uint {
	return uint(n.tower[h].prevOffset.Load())
}

func (n *node) casNextOffset(h int, old, val uint) bool {
	return n.tower[h].nextOffset.CompareAndSwap(arch.UintToArchSize(old), arch.UintToArchSize(val))
}

func (n *node) casPrevOffset(h int, old, val uint) bool {
	return n.tower[h].prevOffset.CompareAndSwap(arch.UintToArchSize(old), arch.UintToArchSize(val))
}
