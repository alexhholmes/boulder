package storage

// Note, storage is only used for directio writing. Manfiest and other metadata
// files can skip this and use the standard library for writing.

// TODO
// Storage will be a wrapper around a directio file. It will perform the necessary
// buffer copying to buffers of the right block size.
// Each store will correspond to a single append-only file.
// WAL, memtable, and SSTable will own a file store.
// THis will expose a simple iterface for writing a file.

// Maybe have two separate wrappers, one for reading only and one for writing
// since files will be append only then read only once done.
// DB can provide "hints" to a writer to throttle their own writes to give
// priority to WAL and other writers as needed. But otherwise have each writer
// handle their own writes.

// A writer will isntanciate with a default priority and then subsequent writer
// hints can be sent from the DB. The writer should also expose some metrics
// for the DB to know how much data is being written and how much is pending.

// DB will never be directly reading or writing to the writers. It will always
// be through the owning WAL, memtable, or SSTable. The LSM will read from the
// sstable to perform compaction and write to the new sstable. It doesn't
// directly read or write to the storage.

// Each storage will have its own background goroutine running that has sole
// ownership of the file handler it is writing or reading to. This will allow
// the use of a select statement for throttling throughput to the file and
// issuing "cooldowns" to the writer.

// The DB manifest will be used to manage when a file is open to use or being
// used for writing, serving client reads, or compaction.
