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
	"boulder/internal/iterator"
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

	// references tracks the number of readers with reference to the memtable.
	// When the number of references drops to zero, the memtable can be safely
	// retired. The current DB memtable will always be incremented by one when
	// it is active. Once the memtable has been flushed to disk, the reference
	// count will be decremented by one. Once flushed, no new references will be
	// added to the memtable, but this table will exist indefinitely until the
	// referencing readers complete.
	references arch.AtomicUint
	// writers is the number of writers that are currently writing to the
	// memtable. This is tracked to prevent the memtable from being flushed to
	// disk while there are still active writers.
	writers sync.WaitGroup
	// readOnly indicates that the memtable is no longer accepting writes as it
	// is full and is being flushed to disk.
	readOnly atomic.Bool
}

func New(size uint, wal *wal.WAL, cmp compare.Compare) *MemTable {
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
		wal:      wal,
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
	m.writers.Add(1)
	defer m.writers.Done()

	if kv.SeqNum() < m.seqNum {
		return ErrInvalidSeqNum
	}

	// Add a check in case the memtable was flushed while when incrementing the
	// writer count.
	if m.readOnly.Load() {
		return ErrMemtableFlushed
	}

	err := m.skiplist.Add(kv.K, kv.V)
	if err != nil {
		if errors.Is(err, skiplist.ErrArenaFull) {
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
func (m *MemTable) ReleaseArena() (*arena.Arena, error) {
	if !m.IsActive() {
		return nil, ErrMemtableActive
	}

	a := m.skiplist.Arena()
	m.skiplist.Reset(nil)
	return a, nil
}

var _ storage.Flusher = (*MemTable)(nil)

// Flush writes the memtable to disk as an SSTable. This should be called by
// the DB when the memtable is full and no longer able to accept writes or if
// an early flush is necessary. This is an idempotent operation.
// The DB must handle this because it needs to do extra bookkeeping for the
// manifest and the active memtables. It also may provide additional options
// for the disk operation rate limiting and priority.
func (m *MemTable) Flush(flush func(iterator *iterator.Iterator)) {
	if m.readOnly.CompareAndSwap(false, true) {
		go func() {
			// Wait for all writers to finish before getting the flush iterator
			m.writers.Wait()
			// This flush operation will run independently of the DB goroutine
			// and can be rate-limited or prioritized by the DB.
			flush(m.skiplist.FlushIter())
			m.references.Add(-1)
		}()
	}
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
