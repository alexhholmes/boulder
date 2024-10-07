package memtable

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ncw/directio"

	"boulder/internal/arch"
	"boulder/internal/arena"
	"boulder/internal/base"
	"boulder/internal/compare"
	"boulder/internal/skiplist"
	"boulder/pkg/storage"
	"boulder/pkg/wal"
)

// MemTable is a memory table that stores key-value pairs in sorted order
// using a skip-list.
type MemTable struct {
	// seqNum is the sequence number at the time the memtable was created. This
	// is guaranteed to be less than or equal to the sequence number of any
	// record written to the memtable.
	seqNum   base.SeqNum
	skiplist *skiplist.Skiplist
	cmp      compare.Compare

	// wal (write-ahead log) is a disk file that is every write operation is
	// committed to before being added to the memtable. Each memtable has its
	// own WAL that can be garbage-collected once the memtable has been written
	// to an SSTable on disk.
	wal *wal.WAL

	// references tracks the number of readers or writers to the memtable. When
	// the number of references drops to zero, the memtable can be safely
	// retired. The current DB memtable will always be incremented by one when
	// it is active. Once the memtable has been flushed to disk, the reference
	// count will be decremented by one. Once flushed, no new references will be
	// added to the memtable, but this table will exist indefinitely until the
	// referencing readers complete.
	references arch.AtomicUint
	// readOnly indicates that the memtable is no longer accepting writes as it
	// is full and is being flushed to disk.
	readOnly atomic.Bool
	// flushed indicates that the memtable is being written to disk, but it may
	// still be active if there are readers holding a reference to it. The
	// memtable is not considered fully flushed to disk until the references
	// count is decremented by one. However, any reader references will keep
	// this memtable from being garbage-collected.
	flushed atomic.Bool
}

func New(size uint, cmp compare.Compare) *MemTable {
	// Round up the size to a multiple of the block size
	if size < directio.BlockSize {
		// Minimum; single disk block
		size = directio.BlockSize
	} else {
		rem := size % directio.BlockSize
		if rem != 0 {
			size -= rem
		}
	}

	m := &MemTable{
		skiplist: skiplist.NewSkiplist(arena.NewArena(size)),
		cmp:      cmp,
	}

	// A newly created memtable is considered active and has a reference count
	// of 1. The reference count will be decremented when the memtable is
	// flushed to disk.
	m.references.Store(1)

	return m
}

// NewFromArena uses recycles an arena from a retired Memtable.
func NewFromArena(a *arena.Arena, cmp compare.Compare) *MemTable {
	a.Reset()
	return &MemTable{
		skiplist: skiplist.NewSkiplist(a),
		cmp:      cmp,
	}
}

// Add inserts an internal key-value pair into the memtable. This is used for
// all writes including set, delete, and single delete operations because the
// trailer of a delete operation acts as a tombstone.
func (m *MemTable) Add(kv base.InternalKV) error {
	if m.readOnly.Load() {
		return ErrMemtableFlushed
	}

	err := m.skiplist.Add(kv.K, kv.V)
	if err != nil {
		if errors.Is(err, skiplist.ErrArenaFull) {
			// Skiplist is full, flush to disk, caller should create a new
			// memory table and try again.
			if m.flushed.CompareAndSwap(false, true) {
				// Don't want to flush the same memtable twice.
				m.Flush()
			}
			return ErrMemtableFlushed
		}
		if errors.Is(err, skiplist.ErrRecordExists) {
			// Duplicate key, caller should increment the sequence number
			// and try again.
			return ErrRecordExists
		}
		return err
	}
	return nil
}

func (m *MemTable) Empty() bool {
	once.Do(calculateMinimumBytes)
	// Check if the underlying arena was released
	if m.skiplist.Arena() == nil {
		return true
	}

	return m.skiplist.Size() == minimumBytes
}

// Size returns the byte size of the memtable including padding bytes in the
// arena.
func (m *MemTable) Size() uint {
	// Check if the underlying arena was released
	if m.skiplist.Arena() == nil {
		return 0
	}
	return m.skiplist.Size()
}

// Cap returns the byte size of the underlying arena buffer for this memtable.
func (m *MemTable) Cap() uint {
	// Check if the underlying arena was released
	if m.skiplist.Arena() == nil {
		return 0
	}
	return m.skiplist.Arena().Cap()
}

// IsActive returns false if the memtable has been flushed to disk and no
// longer has any reader references. At which point, the memtable can be
// safely reset or destroyed (GC).
func (m *MemTable) IsActive() bool {
	return m.references.Load() != 0
}

// ReleaseArena returns a pointer to the arena used by this memtable and removes
// its reference from the memtable. This is meant for the reuse of the arena for
// a future memtable. This returns nil if the memtable is still active or if the
// arena has already been released.
func (m *MemTable) ReleaseArena() *arena.Arena {
	if !m.IsActive() && m.flushed.Load() {
		return nil
	}

	a := m.skiplist.Arena()
	m.skiplist.Reset(nil)
	return a
}

var _ storage.Flusher = (*MemTable)(nil)

func (m *MemTable) Flush() {
	// TODO
}

var (
	// once is used to initialize the size of an empty skiplist arena.
	once         sync.Once
	minimumBytes uint
)

func calculateMinimumBytes() {
	a := arena.NewArena(16 << 10 /* 16 KB */)
	_ = skiplist.NewSkiplist(a)
	minimumBytes = a.Len()
}
