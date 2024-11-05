package skiplist

import (
	"errors"
	"math"
	"unsafe"

	"boulder/internal/arch"
	"boulder/internal/arena"
	"boulder/internal/base"
	"boulder/internal/compare"
	"boulder/internal/fastrand"
)

const (
	NodeAlignment = uint(unsafe.Sizeof(arch.UintToArchSize(0)))
	NodeSize      = uint(unsafe.Sizeof(node{}))
	LinkSize      = uint(unsafe.Sizeof(links{}))
	MaxHeight     = uint(20)
	pValue        = 1 / math.E
)

var probabilities [MaxHeight]uint32

func init() {
	// Precompute the skiplist probabilities so that only a single random number
	// needs to be generated and so that the optimal pvalue can be used (inverse
	// of Euler's number).
	p := 1.0
	for i := uint(0); i < MaxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}

var (
	ErrNoBuffer     = errors.New("skiplist does not have an arena")
	ErrBufferFull   = arena.ErrArenaFull
	ErrRecordExists = errors.New("record with this key already exists")
)

// Skiplist is a fast, concurrent skiplist implementation that supports forward
// and backward iteration. Keys and values are immutable once added to the skiplist
// and deletion is not supported. Instead, higher-level code is expected to add new
// entries that shadow existing entries and perform deletion via tombstones. It
// is up to the user to process these shadow entries and tombstones appropriately
// during retrieval.
type Skiplist struct {
	arena   *arena.Arena
	head    *node
	tail    *node
	height  arch.AtomicUint // Current height. 1 <= height <= MaxHeight. CAS.
	compare compare.Compare
}

func New(size uint, compare compare.Compare) *Skiplist {
	skl := &Skiplist{
		compare: compare,
		arena:   arena.WithOverflow(size, NodeSize),
	}
	_ = skl.Reset()

	return skl
}

func NewFromArena(a *arena.Arena, compare compare.Compare) (*Skiplist, error) {
	skl := &Skiplist{
		compare: compare,
		arena:   a,
	}

	err := skl.Reset()
	if err != nil {
		return nil, err
	}

	return skl, nil
}

func (s *Skiplist) Reset() error {
	if s.arena == nil {
		return ErrNoBuffer
	}
	s.arena.Reset()

	head := s.newEmptyNode()
	tail := s.newEmptyNode()

	// Link all head/tail levels together
	headOffset := s.arena.GetPointerOffset(unsafe.Pointer(head))
	tailOffset := s.arena.GetPointerOffset(unsafe.Pointer(tail))
	for i := uint(0); i < MaxHeight; i++ {
		head.tower[i].next.Store(arch.UintToArchSize(tailOffset))
		tail.tower[i].prev.Store(arch.UintToArchSize(headOffset))
	}

	s.head = head
	s.tail = tail
	s.height.Store(1)

	return nil
}

// Add adds a new key if it does not yet exist. If the key already exists, then
// Add returns ErrRecordExists. If there isn't enough room in the arena, then
// Add returns ErrBufferFull.
func (s *Skiplist) Add(key base.InternalKey, value []byte) error {
	var ins inserter
	if s.findSplice(key, &ins) {
		// Found a matching node, but handle case where it's been deleted.
		return ErrRecordExists
	}

	nd, height, err := s.newNode(key, value)
	if err != nil {
		return err
	}

	ndOffset := s.arena.GetPointerOffset(unsafe.Pointer(nd))

	// We always insert from the base level and up. After you add a node in base
	// level, we cannot create a node in the level above because it would have
	// discovered the node in the base level.
	var found bool
	var invalidateSplice bool
	for i := 0; i < int(height); i++ {
		prev := ins.splices[i].prev
		next := ins.splices[i].next

		if prev == nil {
			// New node increased the height of the skiplist, so assume that the
			// new level has not yet been populated.
			if next != nil {
				panic("next is expected to be nil, since prev is nil")
			}

			prev = s.head
			next = s.tail
		}

		// +----------------+     +------------+     +----------------+
		// |      prev      |     |     node     |     |      next      |
		// | prevNextOffset |---->|            |     |                |
		// |                |<----| prev |     |                |
		// |                |     | next |---->|                |
		// |                |     |            |<----| nextPrevOffset |
		// +----------------+     +------------+     +----------------+
		//
		// 1. Initialize prev and next to point to prev and next.
		// 2. CAS prevNextOffset to repoint from next to node.
		// 3. CAS nextPrevOffset to repoint from prev to node.
		for {
			prevOffset := s.arena.GetPointerOffset(unsafe.Pointer(prev))
			nextOffset := s.arena.GetPointerOffset(unsafe.Pointer(next))
			nd.tower[i].prev.Store(arch.UintToArchSize(prevOffset))
			nd.tower[i].next.Store(arch.UintToArchSize(nextOffset))

			// Check whether next has an updated link to prev. If it does not,
			// that can mean one of two things:
			//   1. The thread that added the next node hasn't yet had a chance
			//      to add the prev link (but will shortly).
			//   2. Another thread has added a new node between prev and next.
			nextPrevOffset := next.prevOffset(i)
			if nextPrevOffset != prevOffset {
				// Determine whether #1 or #2 is true by checking whether prev
				// is still pointing to next. As long as the atomic operations
				// have at least acquire/release semantics (no need for
				// sequential consistency), this works, as it is equivalent to
				// the "publication safety" pattern.
				prevNextOffset := prev.nextOffset(i)
				if prevNextOffset == nextOffset {
					// Ok, case #1 is true, so help the other thread along by
					// updating the next node's prev link.
					next.prevOffsetCAS(i, nextPrevOffset, prevOffset)
				}
			}

			if prev.nextOffsetCAS(i, nextOffset, ndOffset) {
				// Managed to insert node between prev and next, so update the next
				// node's prev link and go to the next level.
				next.prevOffsetCAS(i, prevOffset, ndOffset)
				break
			}

			// CAS failed. We need to recompute prev and next. It is unlikely to
			// be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev
			// and next.
			prev, next, found = s.findSpliceForLevel(key, i, prev)
			if found {
				if i != 0 {
					panic("how can another thread have inserted a node at a non-base level?")
				}

				return ErrRecordExists
			}
			invalidateSplice = true
		}
	}

	// If we had to recompute the splice for a level, invalidate the entire
	// cached splice.
	if invalidateSplice {
		ins.height = 0
	} else {
		// The splice was valid. We inserted a node between splices[i].prev and
		// splices[i].next. Optimistically update splices[i].prev for use in a subsequent
		// call to add.
		for i := uint(0); i < height; i++ {
			ins.splices[i].prev = nd
		}
	}

	return nil
}

// Height returns the height of the highest tower within any of the nodes that
// have ever been allocated as part of this skiplist.
func (s *Skiplist) Height() uint {
	return uint(s.height.Load())
}

// Size returns the number of bytes that have been allocated from the arena.
func (s *Skiplist) Size() uint {
	return s.arena.Size()
}

// Arena returns the arena backing this skiplist.
func (s *Skiplist) Arena() *arena.Arena {
	return s.arena
}

func (s *Skiplist) newEmptyNode() *node {
	nodeOffset, err := s.arena.Allocate(NodeSize, NodeAlignment)
	if err != nil {
		panic("arenaSize is not large enough to hold the head node")
	}

	nd := (*node)(s.arena.GetPointer(nodeOffset))
	nd.keyTrailer = 0
	nd.keyOffset = 0
	nd.keySize = 0
	nd.valSize = 0

	return nd
}

func (s *Skiplist) newNode(key base.InternalKey, value []byte) (nd *node, height uint, err error) {
	rnd := fastrand.Uint32()

	// Check with probability table to determine the height of this node
	height = uint(1)
	for height < MaxHeight && rnd <= probabilities[height] {
		height++
	}

	keySize := uint(len(key.LogicalKey))
	valueSize := uint(len(value))
	truncated := NodeSize - (MaxHeight-height)*LinkSize
	totalSize := truncated + keySize + valueSize

	nodeOffset, err := s.arena.Allocate(totalSize, NodeAlignment)
	if err != nil {
		return nil, 0, ErrBufferFull
	}

	nd = (*node)(s.arena.GetPointer(nodeOffset))
	nd.keyOffset = nodeOffset + truncated
	nd.keySize = keySize
	nd.valSize = valueSize

	nd.keyTrailer = key.Trailer
	copy(nd.getKey(s.arena), key.LogicalKey)
	copy(nd.getValue(s.arena), value)

	// Try to increase s.height via CAS
	listHeight := s.Height()
	for height > listHeight {
		if s.height.CompareAndSwap(
			arch.UintToArchSize(listHeight),
			arch.UintToArchSize(height),
		) {
			// Successfully increased skiplist.height
			break
		}

		listHeight = s.Height()
	}

	return
}

func (s *Skiplist) findSplice(key base.InternalKey, ins *inserter) (found bool) {
	listHeight := s.Height()
	var level int

	prev := s.head
	if ins.height < listHeight {
		// Our cached height is less than the list height, which means there were
		// inserts that increased the height of the list. Recompute the splice from
		// scratch.
		ins.height = listHeight
		level = int(ins.height)
	} else {
		// Our cached height is equal to the list height.
		for ; level < int(listHeight); level++ {
			spl := &ins.splices[level]
			if s.getNext(spl.prev, level) != spl.next {
				// One or more nodes have been inserted between the splice at this
				// level.
				continue
			}
			if spl.prev != s.head && !s.keyIsAfterNode(spl.prev, key) {
				// Key lies before splice.
				level = int(listHeight)
				break
			}
			if spl.next != s.tail && s.keyIsAfterNode(spl.next, key) {
				// Key lies after splice.
				level = int(listHeight)
				break
			}
			// The splice brackets the key!
			prev = spl.prev
			break
		}
	}

	for level = level - 1; level >= 0; level-- {
		var next *node
		prev, next, found = s.findSpliceForLevel(key, level, prev)
		if next == nil {
			next = s.tail
		}
		ins.splices[level].prev = prev
		ins.splices[level].next = next
	}

	return
}

func (s *Skiplist) findSpliceForLevel(
	key base.InternalKey, level int, start *node,
) (prev, next *node, found bool) {
	prev = start

	for {
		// Assume prev.key < key.
		next = s.getNext(prev, level)
		if next == s.tail {
			// Tail node, so done.
			break
		}

		offset, size := next.keyOffset, next.keySize
		nextKey := s.arena.GetBytes(offset, size)
		cmp := s.compare(key.LogicalKey, nextKey)
		if cmp < 0 {
			// We are done for this level, since prev.key < key < next.key.
			break
		}
		if cmp == 0 {
			// User-key equality.
			if key.Trailer == next.keyTrailer {
				// Internal key equality.
				found = true
				break
			}
			if key.Trailer > next.keyTrailer {
				// We are done for this level, since prev.key < key < next.key.
				break
			}
		}

		// Keep moving right on this level.
		prev = next
	}

	return
}

func (s *Skiplist) keyIsAfterNode(nd *node, key base.InternalKey) bool {
	ndKey := s.arena.GetBytes(nd.keyOffset, nd.keySize)
	cmp := s.compare(ndKey, key.LogicalKey)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	// User-key equality.
	if key.Trailer == nd.keyTrailer {
		// Internal key equality.
		return false
	}
	return key.Trailer < nd.keyTrailer
}

func (s *Skiplist) getNext(nd *node, h int) *node {
	offset := nd.tower[h].next.Load()
	return (*node)(s.arena.GetPointer(uint(offset)))
}

func (s *Skiplist) getPrev(nd *node, h int) *node {
	offset := nd.tower[h].prev.Load()
	return (*node)(s.arena.GetPointer(uint(offset)))
}
