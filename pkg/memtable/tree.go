package memtable

import (
	"bytes"
	"iter"
)

// BalancedTree is a black-red tree implementation of the Tree interface.
type BalancedTree struct {
	buffer  []node
	size    uint64
	maxSize uint64
	len     int
	root    *node
}

// NewBalancedTree returns a new BalancedTree with the given available size
// in bytes.
func NewBalancedTree(size uint64) BalancedTree {
	t := BalancedTree{
		buffer:  make([]node, max(4096, size*2)),
		maxSize: size,
	}

	return t
}

func (t *BalancedTree) Insert(key, value []byte) {
	n := &t.buffer[0]

	if t.root == nil {
		n.key = key
		n.value = value
		n.color = red
		t.root = n
	} else {
	loop:
		for {
			compare := bytes.Compare(key, n.key)
			switch {
			case compare == 0:
				// If the key already exists, we will update the value.
				n.value = value
				break loop
			case compare < 0:
				if n.left == 0 {

				} else {

				}
			case compare > 0:

			}
		}
	}

	t.size += n.size()
	t.len++
}

func (t *BalancedTree) Delete(key []byte) bool {
	return false
}

func (t *BalancedTree) leftRotate(x int) {
}

func (t *BalancedTree) rightRotate(x int) {
}

// Size returns the byte size of all the key-value pairs in the tree.
func (t *BalancedTree) Size() uint64 {
	return t.size
}

func (t *BalancedTree) Available() uint64 {
	if len(t.buffer) == cap(t.buffer) {
		return 0
	}
	return t.maxSize - t.size
}

func (t *BalancedTree) Sorted() iter.Seq2[[]byte, []byte] {
	return func(yield func([]byte, []byte) bool) {
		n := &t.root
		for {
			if !yield((*n).key, (*n).value) {
				return
			}
			n = &t.buffer[(*n).right]
		}
	}
}

// WillOverflow returns true if the key-value pair will cause the tree to
// contain more bytes than the initialized size.
//
// It is up to the caller to check this before any insertions or deletions.
// Although a deletion could potentially free up space, it is possible that it
// will insert a tombstone with the key and a nil value that will take up space.
// A caller of delete should pass the key and an empty byte slice as the value.
//
// Because we reference nodes in the underlying buffer, we need to make sure
// that the buffer does not resize.
func (t *BalancedTree) WillOverflow(key, value []byte) bool {
	if len(t.buffer) == cap(t.buffer) {
		return true
	}
	return t.size+uint64(len(key)+len(value)) > t.maxSize
}

func (t *BalancedTree) Len() int {
	return t.len
}

type color bool

const (
	red   color = true
	black color = false
)

// node represents a node in the red-black tree. We want to keep record of
// deletes to a key just as we would an insert. This tombstone value will
// eventually be written to the SSTable along with the key and a nil value.
// A node should be created with byte slices that are the same capacity as the
// length of the slice.
type node struct {
	key     []byte
	value   []byte
	deleted bool
	color   color
	left    int
	right   int
}

func (n *node) size() uint64 {
	if n.deleted {
		return uint64(len(n.key))
	}
	return uint64(len(n.key) + len(n.value))
}
