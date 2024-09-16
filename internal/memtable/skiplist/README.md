# Skiplist

Adapted from CockroachDB Pebble: https://github.com/cockroachdb/pebble
and from Andy Kimball: https://github.com:andy-kimball/arenaskl

Further adapted from RocksDB inline skiplist: https://github.com/facebook/rocksdb.

Key differences:
- No optimization for sequential inserts (no "prev").
- No custom comparator.
- Support overwrites. This requires care when we see the same key when inserting.
  For RocksDB or LevelDB, overwrites are implemented as a newer sequence number in the key, so
  there is no need for values. We don't intend to support versioning. In-place updates of values
  would be more efficient.
- We discard all non-concurrent code.
- We do not support Splices. This simplifies the code a lot.
- No AllocateNode or other pointer arithmetic.
- We combine the findLessThan, findGreaterOrEqual, etc into one function.

Further adapted from Badger: https://github.com/dgraph-io/badger.

Key differences:
- Support for previous pointers - doubly linked lists. Note that it's up to higher
  level code to deal with the intermediate state that occurs during insertion,
  where node A is linked to node B, but node B is not yet linked back to node A.
- Iterator includes mutator functions.
