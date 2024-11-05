package skiplist

import (
	"boulder/internal/base"
	"boulder/pkg/iterator"
)

// Iterator is an iterator over the skiplist object. Use Skiplist.Iter
// to construct an iterator. The current state of the iterator can be cloned by
// simply value copying the struct. All iterator methods are thread-safe.
type Iterator struct {
	list  *Skiplist
	node  *node
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
	close func()
}

var _ iterator.Iterator = (*Iterator)(nil)

func (it *Iterator) First() *base.InternalKV {
	it.node = it.list.getNext(it.list.head, 0)
	if it.node == it.list.tail || it.node == it.upperNode {
		return nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.compare(it.upper, it.kv.K.LogicalKey) <= 0 {
		it.upperNode = it.node
		return nil
	}
	it.kv.V = it.node.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) Last() *base.InternalKV {
	it.node = it.list.getPrev(it.list.tail, 0)
	if it.node == it.list.head || it.node == it.lowerNode {
		return nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.compare(it.lower, it.kv.K.LogicalKey) > 0 {
		it.lowerNode = it.node
		return nil
	}
	it.kv.V = it.node.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) Next() *base.InternalKV {
	it.node = it.list.getNext(it.node, 0)
	if it.node == it.list.tail || it.node == it.upperNode {
		return nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.compare(it.upper, it.kv.K.LogicalKey) <= 0 {
		it.upperNode = it.node
		return nil
	}
	it.kv.V = it.node.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) Prev() *base.InternalKV {
	it.node = it.list.getPrev(it.node, 0)
	if it.node == it.list.head || it.node == it.lowerNode {
		return nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.compare(it.lower, it.kv.K.LogicalKey) > 0 {
		it.lowerNode = it.node
		return nil
	}
	it.kv.V = it.node.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) decodeKey() {
	it.kv.K.LogicalKey = it.list.arena.GetBytes(it.node.keyOffset, it.node.keySize)
	it.kv.K.Trailer = it.node.keyTrailer
}

func (it *Iterator) Close() error {
	it.close()
	*it = Iterator{}
	return nil
}

// FlushIterator is intended to be used by the memtable to iterate over the
// skiplist in order to flush it to disk.
type FlushIterator struct {
	Iterator
}

var _ iterator.Iterator = (*FlushIterator)(nil)

func (fl *FlushIterator) First() *base.InternalKV {
	return fl.Iterator.First()
}

func (fl *FlushIterator) Last() *base.InternalKV {
	panic("boulder: Last unimplemented")
}

func (fl *FlushIterator) Next() *base.InternalKV {
	fl.node = fl.list.getNext(fl.node, 0)
	if fl.node == fl.list.tail {
		return nil
	}
	fl.decodeKey()
	return &fl.kv
}

func (fl *FlushIterator) Prev() *base.InternalKV {
	panic("boulder: Prev unimplemented")
}

func (fl *FlushIterator) Close() error {
	return nil
}
