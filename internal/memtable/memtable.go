package memtable

import (
	"io"
	"sync"

	"boulder/internal/wal"
)

type Flush func(w io.Writer, flushed <-chan struct{})

// MemTable is a memory table that stores key-value pairs in sorted order
// using a red-black tree.
type MemTable struct {
	mu       sync.RWMutex
	wal      *wal.WriteAheadLog
	skiplist *SkipList
	dead     map[*BalancedTree]struct{}
	deadChan chan<- *BalancedTree
	flush    chan<- Flush
}

// NewMemTable returns a new MemTable with the given flush channel. The flush
// is created and consumed by the lsm manager.
func NewMemTable(wal *wal.WriteAheadLog, flush chan<- Flush) *MemTable {
	m := &MemTable{
		tree:  NewBalancedTree(4096 * 4),
		dead:  make(map[*BalancedTree]struct{}, 64),
		flush: flush,
	}

	// The dead channel is used to signal when a tree has been fully
	// written to disk and can be removed from the dead map.
	deadChan := make(chan *BalancedTree)
	m.deadChan = deadChan
	go func() {
		select {
		case d := <-deadChan:
			m.mu.Lock()
			delete(m.dead, d)
			m.mu.Unlock()
		}
	}()

	return m
}

func (m *MemTable) Set(key, value []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.tree.WillOverflow(key, value) {
		m.Flush()
	}
	m.tree.Insert(key, value)
}

func (m *MemTable) Get(key []byte) ([]byte, func(), bool) {
	m.mu.RLock()

	val, ok := m.tree.Get(key)

	return val, func() { m.mu.RUnlock() }, ok
}

func (m *MemTable) Delete(key []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.tree.WillOverflow(key, nil) {
		m.Flush()
	}

	m.tree.Delete(key)
}

func (m *MemTable) DeleteRange(start, end []byte) {
	m.mu.RLock()
	defer m.mu.RUnlock()
}

// Flush writes the contents of the MemTable to the given writer then creates
// a new balanced tree.
func (m *MemTable) Flush() {
	m.flush <- m.write
}

func (m *MemTable) write(w io.Writer, flushed <-chan struct{}) {
	m.mu.Lock()

	// Keep a reference to the old tree so we can write its contents to disk
	// and create a new tree.
	old := m.tree
	m.dead[old] = struct{}{}
	m.tree = NewBalancedTree(4096 * 4)

	m.mu.Unlock()

	go func() {
		// The underlying writer managed by lsm will copy the writes to a
		// directio aligned buffer and write to disk in chunks. It will also
		// keep track of the length of padding at the end of the buffer block
		// written. This writer will never return an error.
		//
		// The lsm manager will also do additional bookkeeping to create indexes,
		// bloom filters, and other metadata.
		for key, val := range old.Sorted() {
			_, _ = w.Write(key)
			_, _ = w.Write(val)
		}
	}()

	// Once the write is flushed to disk, we can signal that the tree stump
	// can be dug up from the dead map.
	<-flushed
	m.deadChan <- old
}

func (m *MemTable) Size() uint64 {
	return m.tree.size
}

var _ io.Closer = (*MemTable)(nil)

// Close is called to flush the contents of the memory table to disk. It is
// up to the caller to close this before closing the owning LSM. The close
// call on the LSM will wait for all pending writes to finish before closing.
func (m *MemTable) Close() error {
	m.Flush()
	m.wal.Close()
	return nil
}
