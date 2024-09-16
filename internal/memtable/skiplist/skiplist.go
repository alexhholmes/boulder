package skiplist

import (
	"errors"
	"math"
	"unsafe"

	"boulder/internal/util/arch"
	"boulder/internal/util/arena"
)

const (
	maxNodeSize   = uint(unsafe.Sizeof(node{}))
	maxHeight     = uint(20)
	pValue        = 1 / math.E
	linksSize     = uint(unsafe.Sizeof(links{}))
	nodeAlignment = 4
	deletedValue  = 0
)

var probabilities [maxHeight]uint32

func init() {
	// Precompute the skiplist probabilities so that only a single random number
	// needs to be generated and so that the optimal pvalue can be used (inverse
	// of Euler's number).
	p := float64(1.0)
	for i := uint(0); i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

var (
	// ErrRecordExists indicates that an entry with the specified key already
	// exists in the skiplist. Duplicate entries are not directly supported and
	// instead must be handled by the user by appending a unique version suffix to
	// keys.
	ErrRecordExists = errors.New("record with this key already exists")
)

// Skiplist is a fast, concurrent skiplist implementation that supports forward
// and backward iteration. Keys and values are immutable once added to the skiplist
// and deletion is not supported. Instead, higher-level code is expected to add new
// entries that shadow existing entries and perform deletion via tombstones. It
// is up to the user to process these shadow entries and tombstones appropriately
// during retrieval.
type Skiplist struct {
	arena  *arena.Arena
	head   *node
	tail   *node
	height arch.AtomicUint // Current height. 1 <= height <= maxHeight. CAS.
}

// NewSkiplist constructs and initializes a new, empty skiplist. All nodes, keys,
// and values in the skiplist will be allocated from the given arena.
func NewSkiplist(arena *arena2.Arena) *Skiplist {
	skl := &Skiplist{}
	skl.Reset(arena)
	return skl
}

// Reset the skiplist to empty and re-initialize.
func (s *Skiplist) Reset(a *arena.Arena) {
	// Allocate head and tail nodes.
	head, err := newRawNode(a, maxHeight, 0, 0)
	if err != nil {
		panic("arenaSize is not large enough to hold the head node")
	}
	head.keyOffset = 0

	tail, err := newRawNode(a, maxHeight, 0, 0)
	if err != nil {
		panic("arenaSize is not large enough to hold the tail node")
	}
	tail.keyOffset = 0

	// Link all head/tail levels together.
	headOffset := a.getPointerOffset(unsafe.Pointer(head))
	tailOffset := a.getPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset.Store(int64(tailOffset))
		tail.tower[i].prevOffset.Store(int64(headOffset))
	}

	*s = Skiplist{
		arena: a,
		head:  head,
		tail:  tail,
	}
	s.height.Store(1)
}

// Arena returns the arena backing this skiplist.
func (s *Skiplist) Arena() *arena.Arena {
	return s.arena
}

// Height returns the height of the highest tower within any of the nodes that
// have ever been allocated as part of this skiplist.
func (s *Skiplist) Height() uint {
	return uint(s.height.Load())
}

// Size returns the number of bytes that have allocated from the arena.
func (s *Skiplist) Size() uint {
	return s.arena.Size()
}
