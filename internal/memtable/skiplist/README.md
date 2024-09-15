# skiplist

Fast, lock-free, arena-based Skiplist implementation in Go that supports iteration
in both directions.

## Pedigree

This code is based on CockroachDB's Pebble arenaskl code:

https://github.com/cockroachdb/pebble/tree/master/internal/arenaskl

The Pebble arenaskl code is based on Andy Kimball's arenaskl code:

https://github.com/andy-kimball/arenaskl

The arenaskl code is based on the skiplist found in Badger, a Go-based
KV store:

https://github.com/dgraph-io/badger/tree/master/skl

The skiplist in Badger is itself based on a C++ skiplist built for
Facebook's RocksDB:

https://github.com/facebook/rocksdb/tree/master/memtable

## Deviation from Pebble

At risk of being ignorant, I implement this skiplist with plain `int` types
for lengths. I chose to follow this word of advice:

"An index, length or capacity should normally be an int. The int type is either 32 or 
64 bits, and always big enough to hold the maximum possible length of an array."

https://yourbasic.org/golang/int-vs-int64/
