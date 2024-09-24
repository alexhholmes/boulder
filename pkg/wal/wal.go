package wal

// WriteAheadLog stores all the changes made to a specific memtable. Once a
// memtable has been committed to disk and removed from memory, it's close
// operation will be called to close the write ahead log and update the
// manifest. It is up to the manifest background goroutine to remove the
// write ahead log from disk.
type WriteAheadLog struct {
}
