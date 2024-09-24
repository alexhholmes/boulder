package skiplist

//
// import (
// 	"sync"
//
// 	"boulder/internal/base"
// )
//
// // Iterator is an iterator over the skiplist object. Use Skiplist.NewIter
// // to construct an iterator. The current state of the iterator can be cloned by
// // simply value copying the struct. All iterator methods are thread-safe.
// type Iterator struct {
// 	list  *Skiplist
// 	nd    *node
// 	kv    base.InternalKV
// 	lower []byte
// 	upper []byte
//
// 	// lowerNode and upperNode are lazily populated with an arbitrary node that
// 	// is beyond the lower or upper bound respectively. Note the node is
// 	// "arbitrary" because it may not be the first node that exceeds the bound.
// 	// Concurrent insertions into the skiplist may introduce new nodes with keys
// 	// that exceed the bounds but are closer to the bounds than the current
// 	// values of [lower|upper]Node.
// 	//
// 	// Once populated, [lower|upper]Node may be used to detect when iteration
// 	// has reached a bound without performing a key comparison. This may be
// 	// beneficial when performing repeated SeekGEs with TrySeekUsingNext and an
// 	// upper bound set. Once the upper bound has been met, no additional key
// 	// comparisons are necessary.
// 	lowerNode *node
// 	upperNode *node
// }
//
// var iterPool = sync.Pool{
// 	New: func() interface{} {
// 		return &Iterator{}
// 	},
// }
//
// func (it *Iterator) Close() error {
// 	*it = Iterator{}
// 	iterPool.Put(it)
// 	return nil
// }
//
// // SeekGE moves the iterator to the first entry whose key is greater than or
// // equal to the given key. Returns the KV pair if the iterator is pointing at a
// // valid entry, and nil otherwise. Note that SeekGE only checks the upper bound.
// // It is up to the caller to ensure that key is greater than or equal to the
// // lower bound.
// func (it *Iterator) SeekGE(key []byte, flags base.SeekGEFlags) *base.InternalKV {
// 	if flags.TrySeekUsingNext() {
// 		if it.nd == it.list.tail || it.nd == it.upperNode {
// 			// Iterator is done.
// 			return nil
// 		}
// 		less := it.list.cmp(it.kv.K.UserKey, key) < 0
// 		// Arbitrary constant. By measuring the seek cost as a function of the
// 		// number of elements in the skip list, and fitting to a model, we
// 		// could adjust the number of nexts based on the current size of the
// 		// skip list.
// 		const numNexts = 5
// 		kv := &it.kv
// 		for i := 0; less && i < numNexts; i++ {
// 			if kv = it.Next(); kv == nil {
// 				// Iterator is done.
// 				return nil
// 			}
// 			less = it.list.cmp(kv.K.UserKey, key) < 0
// 		}
// 		if !less {
// 			return kv
// 		}
// 	}
// 	_, it.nd, _ = it.seekForBaseSplice(key)
// 	if it.nd == it.list.tail || it.nd == it.upperNode {
// 		return nil
// 	}
// 	it.decodeKey()
// 	if it.upper != nil && it.list.cmp(it.upper, it.kv.K.UserKey) <= 0 {
// 		it.upperNode = it.nd
// 		return nil
// 	}
// 	it.kv.V = base.MakeInPlaceValue(it.value())
// 	return &it.kv
// }
