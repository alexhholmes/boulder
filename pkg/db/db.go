package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"boulder/internal/base"
	"boulder/pkg/manifest"
	"boulder/pkg/memtable"
)

const (
	DataDirectoryName = "data"
	WalDirectoryName  = "wal"
)

type DB struct {
	TransactionHandler

	// mu protects the global database state. This is only held when memtables
	// are being swapped along with the corresponding WAL.
	mu     sync.Mutex
	seqNum base.AtomicSeqNum

	// memtable is a concurrent in-memory KV store for all writes to the
	// database. The memtable is temporary, and once it is full, it is flushed
	// to disk.
	memtable *memtable.MemTable

	// manifest tracks all state changes to the database files. Each update to
	// the manifest creates a new manifest file that is immediately flushed to
	// disk. Depending on the DB configuration, a certain number of manifest
	// file versions will be retained.
	manifest *manifest.Manifest
}

type ReaderHandler func()

type TransactionHandler struct {
	state []int
}

type IteratorHandler struct {
}

// Open opens the database in read-write mode. If the database directory does
// not exist or is empty, a new database is created. If the database directory
// exists, a lock file is created and the persisted database is opened.
func Open(directory string, options ...Option) (db *DB, err error) {
	dataDirectoryPath := filepath.Join(directory, DataDirectoryName)
	walDirectoryPath := filepath.Join(directory, WalDirectoryName)

	// Create directories if they don't exist
	if err = os.MkdirAll(dataDirectoryPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	if err = os.MkdirAll(walDirectoryPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal directory: %w", err)
	}

	// Create lockfile for the directory
	lockFile, err := os.OpenFile(
		filepath.Join(directory, "db.lock"),
		os.O_CREATE|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create lock file: %w", err)
	}
	if err = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return nil, fmt.Errorf("failed to lock directory: %w", err)
	}

	// Open data and WAL directories
	dataDirectory, err := os.OpenFile(
		dataDirectoryPath,
		os.O_CREATE|os.O_RDWR,
		0755,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open data directory: %w", err)
	}
	defer func() {
		if db == nil {
			_ = dataDirectory.Close()
		}
	}()
	walDirectory, err := os.OpenFile(
		walDirectoryPath,
		os.O_CREATE|os.O_RDWR,
		0755,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal directory: %w", err)
	}
	defer func() {
		if db == nil {
			_ = walDirectory.Close()
		}
	}()

	db.dataDirectory = dataDirectory
	db.walDirectory = walDirectory
	db.memtable = memtable.New(db.wal)
	db.openedAt = time.Now()

	// Attempt to close resources on panic
	defer func() {
		if r := recover(); r != nil {
			_ = db.Close()
		}
	}()

	return db, nil
}

// OpenReadOnly opens the database in read-only mode to perform read operations
// on persisted database. Any operation that writes data or mutates database
// state will return an error. This will maintain a directory file-lock on the
// database directory until the database is closed.
func OpenReadOnly(directory string, options ...Option) (db *DB, err error) {
	panic("not implemented")
}

// Close is a blocking call that will wait until all pending writes and
// compactions are finished before safely closing the DB.
func (db *DB) Close() error {
	var errs []error
	if err := db.dataDirectory.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close data directory: %w", err))
	}
	if err := db.walDirectory.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close wal directory: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to close database: %w", errors.Join(errs...))
	}
	return nil
}

// Get returns a copied buffer of the original value.
func (db *DB) Get(key []byte) (value []byte, err error) {
	panic("not implemented")
}

// GetPinned returns a reference to the value buffer stored in the buffer pool.
// This pins the page in the buffer pool and can increase memory usage. This
// is only suitable for use with very large values.
// func (db *DB) GetPinned(key []byte) ([]byte, error) {
// 	panic("not implemented")
// }

// func (db *DB) NewIterator() iterator.Iterator {
// 	panic("not implemented")
// }

// NewSnapshotIterator returns a new iterator on a snapshot.
// func (db *DB) NewSnapshotIterator() iterator.Iterator {
// 	panic("not implemented")
// }

func (db *DB) Set(key, value []byte) error {
	kv := base.InternalKV{
		K: base.MakeInternalKey(key, db.seqNum.Load(), base.InternalKeyKindSet),
		V: value,
	}
	err := db.memtable.Insert(kv)
	if err != nil {
		if errors.Is(err, memtable.ErrFlushed) {
			// TODO handle memtable flush replacement
			return nil
		}
		if errors.Is(err, memtable.ErrFull) {
			// TODO handle memtable flush replacement
		}
		if errors.Is(err, memtable.ErrRecordExists) {
			// Increment sequence number and try again
			db.seqNum.Add(1)
			return db.Set(key, value)
		}
		if errors.Is(err, memtable.ErrInvalidSeqNum) {
			panic("invalid sequence number")
		}
		return err
	}
	return nil
}

func (db *DB) RangeKeySet(keyStart, keyEnd, value []byte) error {
	panic("not implemented")
}

func (db *DB) Delete(key []byte) error {
	kv := base.InternalKV{
		K: base.MakeInternalKey(key, db.seqNum.Load(), base.InternalKeyKindDelete),
		V: nil,
	}
	err := db.memtable.Insert(kv)
	if err != nil {
		if errors.Is(err, memtable.ErrFlushed) {
			// TODO handle memtable flush replacement
			return nil
		}
		if errors.Is(err, memtable.ErrFull) {
			// TODO handle memtable flush replacement
		}
		if errors.Is(err, memtable.ErrRecordExists) {
			// Increment sequence number and try again
			db.seqNum.Add(1)
			return db.Delete(key)
		}
		if errors.Is(err, memtable.ErrInvalidSeqNum) {
			panic("invalid sequence number")
		}
		return err
	}
	return nil
}

func (db *DB) RangeKeyDelete(start, end []byte) error {
	panic("not implemented")
}

func (db *DB) NewSnapshot() error {
	panic("not implemented")
}

func (db *DB) FlushMemtable() error {
	return nil
}
