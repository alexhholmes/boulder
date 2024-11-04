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
	close func()
}

var _ iterator.Iterator = (*Iterator)(nil)

func (it *Iterator) First() *base.InternalKV {
	it.nd = it.list.getNext(it.list.head, 0)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.compare(it.upper, it.kv.K.LogicalKey) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	it.kv.V = it.nd.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) Last() *base.InternalKV {
	it.nd = it.list.getPrev(it.list.tail, 0)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.compare(it.lower, it.kv.K.LogicalKey) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	it.kv.V = it.nd.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) Next() *base.InternalKV {
	it.nd = it.list.getNext(it.nd, 0)
	if it.nd == it.list.tail || it.nd == it.upperNode {
		return nil
	}
	it.decodeKey()
	if it.upper != nil && it.list.compare(it.upper, it.kv.K.LogicalKey) <= 0 {
		it.upperNode = it.nd
		return nil
	}
	it.kv.V = it.nd.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) Prev() *base.InternalKV {
	it.nd = it.list.getPrev(it.nd, 0)
	if it.nd == it.list.head || it.nd == it.lowerNode {
		return nil
	}
	it.decodeKey()
	if it.lower != nil && it.list.compare(it.lower, it.kv.K.LogicalKey) > 0 {
		it.lowerNode = it.nd
		return nil
	}
	it.kv.V = it.nd.getValue(it.list.arena) // TODO lazy value for internal KV
	return &it.kv
}

func (it *Iterator) decodeKey() {
	it.kv.K.LogicalKey = it.list.arena.GetBytes(it.nd.keyOffset, it.nd.keySize)
	it.kv.K.Trailer = it.nd.keyTrailer
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
	fl.nd = fl.list.getNext(fl.nd, 0)
	if fl.nd == fl.list.tail {
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
