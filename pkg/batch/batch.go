package batch

// Both RocksDB and Pebble use batching for every operation, even if it is a
// single get or put. Should we do the same? Will still need to implement batching
// so we can have types operations.
