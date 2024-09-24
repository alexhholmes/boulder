package batch

// Both RocksDB and Pebble use batching for every operation, even if it is a
// single get or put. Should we do the same? Will still need to implement batching
// so we can have types operations.

// RocksDB has both batching and transactions, but I think we'll stick to just
// having batching and give batching the same atomicity guarantees as transactions
// as well as the option for changing the isolation level of individual batches.
// So mostly just semantics.
