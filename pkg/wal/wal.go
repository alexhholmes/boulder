package wal

import (
	"os"

	"github.com/ncw/directio"
)

// WAL (write-ahead log) stores all the changes made to a specific memtable.
// Once a memtable has been committed to disk and removed from memory, it's
// close operation will be called to close the write ahead log and update the
// manifest. It is up to the manifest background goroutine to remove the write
// ahead log from disk.
type WAL struct {
	logfile *os.File
}

func New(path string) (*WAL, error) {
	// Open an append only file for the write-ahead log using direct I/O
	logfile, err := directio.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return nil, err
	}

	return &WAL{
		logfile: logfile,
	}, nil
}

func (w *WAL) Flush() error {

}

func (w *WAL) Close() error {

}
