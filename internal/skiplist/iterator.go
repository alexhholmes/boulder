package skiplist

import (
	"boulder/internal/base"
	"boulder/internal/iterator"
)

// Iterator is an iterator over the skiplist object. Use Skiplist.Iter
// to construct an iterator. The current state of the iterator can be cloned by
// simply value copying the struct. All iterator methods are thread-safe.
type Iterator struct {
	list  *Skiplist
	nd    *node
	kv    base.InternalKV
	lower []byte
	upper []byte

	// lowerNode and upperNode are lazily populated with an arbitrary node that
	// is beyond the lower or upper bound respectively. Note the node is
	// "arbitrary" because it may not be the first node that exceeds the bound.
	// Concurrent insertions into the skiplist may introduce new nodes with keys
	// that exceed the bounds but are closer to the bounds than the current
	// values of [lower|upper]Node.
	//
	// Once populated, [lower|upper]Node may be used to detect when iteration
	// has reached a bound without performing a key comparison. This may be
	// beneficial when performing repeated SeekGEs with TrySeekUsingNext and an
	// upper bound set. Once the upper bound has been met, no additional key
	// comparisons are necessary.
	lowerNode *node
	upperNode *node

	// close is a closer function provided by the memtable that owns the skiplist this
	// Iterator holds a reference to. This will decrement the number of references to
	// the memtable. This prevents a memtable from being deleted while readers are still
	// using it, even if it has already been written to an SSTable on disk.
	close func() error
}

var _ iterator.Iterator = (*Iterator)(nil)

func (it *Iterator) First() *base.InternalKV {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}

	it.decodeKey()
	// if it.upper != nil && it.list.cmp

	return nil
}

func (it *Iterator) Last() *base.InternalKV {
	return nil
}

func (it *Iterator) Next() *base.InternalKV {
	return nil
}

func (it *Iterator) Prev() *base.InternalKV {
	return nil
}

func (it *Iterator) decodeKey() {
	it.kv.K.LogicalKey = it.list.arena.GetBytes(it.nd.keyOffset, it.nd.keySize)
	it.kv.K.Trailer = it.nd.keyTrailer
}

func (it *Iterator) Close() error {
	err := it.close()
	*it = Iterator{}
	return err
}

type FlushIterator struct {
	Iterator
}

var _ iterator.Iterator = (*FlushIterator)(nil)

func (it *FlushIterator) First() *base.InternalKV {
	return it.Iterator.First()
}

func (it *FlushIterator) Last() *base.InternalKV {
	panic("boulder: Last unimplemented")
}

func (it *FlushIterator) Next() *base.InternalKV {
	it.nd = it.list.getNext(it.nd, 0)
	if it.nd == it.list.tail {
		return nil
	}
	it.decodeKey()
	return &it.kv
}

func (it *FlushIterator) Prev() *base.InternalKV {
	panic("boulder: Prev unimplemented")
}

func (it *FlushIterator) Close() error {
	return nil
}
