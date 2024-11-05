package memtable

import (
	"errors"
	"sync"

	"boulder/internal/arena"
	"boulder/internal/base"
	"boulder/internal/compare"
	"boulder/internal/directio"
	"boulder/internal/skiplist"
	"boulder/pkg/wal"
)

// MemTable is a memory table that stores key-value pairs in sorted order
// using a skip-list.
type MemTable struct {
	seq base.SeqNum
	skl *skiplist.Skiplist
	wal *wal.WAL
}

func New(size uint) *MemTable {
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
		skl: skiplist.New(size, compare.SuffixCompare),
		wal: nil,
	}

	return m
}

// NewFromArena recycles an arena from a retired Memtable.
func NewFromArena(a *arena.Arena) (*MemTable, error) {
	skl, err := skiplist.NewFromArena(a, compare.SuffixCompare)
	if err != nil {
		return nil, err
	}

	return &MemTable{
		skl: skl,
		wal: nil,
	}, nil
}

// Insert puts an internal key-value pair into the memtable. This is used for
// all writes including set, delete, and single delete operations because the
// trailer of a delete operation acts as a tombstone.
func (m *MemTable) Insert(kv base.InternalKV) error {
	if kv.SeqNum() < m.seq {
		return ErrInvalidSeqNum
	}

	err := m.skl.Add(kv.K, kv.V)
	if err != nil {
		switch {
		case errors.Is(err, skiplist.ErrBufferFull):
			return ErrFull
		case errors.Is(err, skiplist.ErrRecordExists):
			// Duplicate key, caller should increment the sequence number
			// and try again.
			return ErrRecordExists
		default:
			return err
		}
	}
	return nil
}

var (
	// onceEmpty is used to initialize the size of an empty skl arena
	onceEmpty    sync.Once
	minimumBytes uint
)

func (m *MemTable) Empty() bool {
	onceEmpty.Do(func() {
		a := arena.New(16 << 10 /* 16 KB */)
		_, _ = skiplist.NewFromArena(a, func(a, b []byte) int { return 0 })
		minimumBytes = a.Size()
	})

	// Check if the underlying arena was released
	if m.skl.Arena() == nil {
		return true
	}

	return m.skl.Size() == minimumBytes
}

func (m *MemTable) Size() uint {
	// Check if the underlying arena was released
	if m.skl.Arena() == nil {
		return 0
	}
	return m.skl.Size()
}

func (m *MemTable) Cap() uint {
	// Check if the underlying arena was released
	if m.skl.Arena() == nil {
		return 0
	}
	return m.skl.Arena().Cap()
}

func (m *MemTable) Close() error {
	return m.skl.Arena().Close()
}
