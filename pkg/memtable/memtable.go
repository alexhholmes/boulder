package memtable

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/ncw/directio"

	"boulder/internal/arch"
	"boulder/internal/arena"
	"boulder/internal/base"
	"boulder/internal/skiplist"
)

// MemTable is a memory table that stores key-value pairs in sorted order
// using a red-black tree.
type MemTable struct {
	// seqNum is the sequence number at the time the memtable was created. This
	// is guaranteed to be less than or equal to the sequence number of any
	// record written to the memtable.
	seqNum   base.SeqNum
	skiplist *skiplist.Skiplist

	// references tracks the number of readers or writers to the memtable. When
	// the number of references drops to zero, the memtable can be safely
	// retired. The current DB memtable will always be incremented by one when
	// it is active. Once the memtable has been flushed to disk, the reference
	// count will be decremented by one. Once flushed, no new references will be
	// added to the memtable, but this table will exist indefinitely until the
	// referencing readers complete.
	references arch.AtomicUint
	// flushing indicates that the memtable is full and is no longer accepting
	// writes.
	flushing atomic.Bool
}

func New(size uint) *MemTable {
	// Align the size to the block size
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
	}

	// A newly created memtable is considered active and has a reference count
	// of 1. The reference count will be decremented when the memtable is
	// flushed to disk.
	m.references.Store(1)

	return m
}

func NewFromArena(a *arena.Arena) *MemTable {
	return &MemTable{
		skiplist: skiplist.NewSkiplist(a),
	}
}

func (m *MemTable) Set(kv base.InternalKV) error {
	if m.flushing.Load() {
		return ErrMemtableFlushed
	}

	err := m.skiplist.Add(kv.K, kv.V)
	if err != nil {
		if errors.Is(err, arena.ErrArenaFull) {
			// Skiplist is full, flush to disk, caller should create a new
			// memory table and try again.
			if m.flushing.CompareAndSwap(false, true) {
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

// Flush is either called by the memtable when an insertion fails because it is
// full or by the DB for a preemptive flush.
func (m *MemTable) Flush() {
	var wg sync.WaitGroup
	wg.Add(1)

	// The flush function will run in a separate goroutine and signal the
	// wait group when the flush is complete.
	// m.flush(, wg)

	// Wait for the flush to complete before decrementing the reference count.
	// This does not mean the memtable is no longer active, but that the
	// memtable has been flushed to disk. Active readers can still hold a
	// reference to the memtable.
	go func() {
		wg.Wait()
		m.references.Add(-1)
	}()
}

// Size returns the byte size of the memtable including padding bytes in the
// arena.
func (m *MemTable) Size() uint {
	return m.skiplist.Size()
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
	if !m.IsActive() {
		return nil
	}

	a := m.skiplist.Arena()
	m.skiplist.Reset(nil)
	return a
}
