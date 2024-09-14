package memtable

import (
	"errors"
	"io"
)

var ErrMemTableFull = errors.New("memtable is full")

type Flush func(w io.Writer)

// MemTable is a memory table that stores key-value pairs in sorted order
// using a red-black tree.
type MemTable struct {
	tree  BalancedTree
	flush chan<- Flush
}

func NewMemTable(flush chan<- Flush) MemTable {
	return MemTable{
		tree:  NewBalancedTree(4096 * 4),
		flush: flush,
	}
}

func (m *MemTable) Set(key, value []byte) error {
	if m.tree.WillOverflow(key, value) {
	}

}

func (m *MemTable) Get(key []byte) ([]byte, error) {

}

func (m *MemTable) Delete(key []byte) error {

}

func (m *MemTable) DeleteRange(start, end []byte) error {

}

// Flush writes the contents of the MemTable to the given writer then creates
// a new balanced tree.
func (m *MemTable) Flush() {
	m.flush <- m.write
}

func (m *MemTable) write(w io.Writer) {
	// The underlying writer managed by lsm will copy the writes to a
	// directio aligned buffer and write to disk in chunks. It will also
	// keep track of the length of padding at the end of the buffer block
	// written. This writer will never return an error.
	for key, val := range m.tree.Sorted() {
		_, _ = w.Write(key)
		_, _ = w.Write(val)
	}
}

func (m *MemTable) Size() uint64 {
	return m.tree.size
}
