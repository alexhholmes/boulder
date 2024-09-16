/*
Adapted from CockroachDB Pebble: https://github.com/cockroachdb/pebble.

Further adapted from RocksDB inline skiplist: https://github.com/facebook/rocksdb.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
	there is no need for values. We don't intend to support versioning. In-place updates of values
	would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.

Further adapted from Badger: https://github.com/dgraph-io/badger.

Key differences:
- Support for previous pointers - doubly linked lists. Note that it's up to higher
  level code to deal with the intermediate state that occurs during insertion,
  where node A is linked to node B, but node B is not yet linked back to node A.
- Iterator includes mutator functions.
*/

package skiplist

import (
	"errors"
	"math"
	"unsafe"

	"boulder/internal/memtable/skiplist/arch"
)

const (
	maxHeight = 20
	pValue    = 1 / math.E
	linkSize  = int(unsafe.Sizeof(links{}))
)

var probabilities [maxHeight]uint32

func init() {
	// Precompute the skiplist probabilities so that only a single random number
	// needs to be generated and so that the optimal pvalue can be used (inverse
	// of Euler's number).
	p := float64(1.0)
	for i := 0; i < maxHeight; i++ {
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
	arena  *Arena
	head   *node
	tail   *node
	height arch.AtomicUint // Current height. 1 <= height <= maxHeight. CAS.
}

// NewSkiplist constructs and initializes a new, empty skiplist. All nodes, keys,
// and values in the skiplist will be allocated from the given arena.
func NewSkiplist(arena *Arena) *Skiplist {
	skl := &Skiplist{}
	skl.Reset(arena)
	return skl
}

// Reset the skiplist to empty and re-initialize.
func (s *Skiplist) Reset(arena *Arena) {
	// Allocate head and tail nodes.
	head, err := newRawNode(arena, maxHeight, 0, 0)
	if err != nil {
		panic("arenaSize is not large enough to hold the head node")
	}
	head.keyOffset = 0

	tail, err := newRawNode(arena, maxHeight, 0, 0)
	if err != nil {
		panic("arenaSize is not large enough to hold the tail node")
	}
	tail.keyOffset = 0

	// Link all head/tail levels together.
	headOffset := arena.getPointerOffset(unsafe.Pointer(head))
	tailOffset := arena.getPointerOffset(unsafe.Pointer(tail))
	for i := 0; i < maxHeight; i++ {
		head.tower[i].nextOffset.Store(int64(tailOffset))
		tail.tower[i].prevOffset.Store(int64(headOffset))
	}

	*s = Skiplist{
		arena: arena,
		head:  head,
		tail:  tail,
	}
	s.height.Store(1)
}

// Arena returns the arena backing this skiplist.
func (s *Skiplist) Arena() *Arena {
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
