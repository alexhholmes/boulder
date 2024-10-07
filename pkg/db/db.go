package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"boulder/internal/arena"
	"boulder/internal/base"
	"boulder/internal/compare"
	"boulder/pkg/manifest"
	"boulder/pkg/memtable"
	"boulder/pkg/storage"
	"boulder/pkg/wal"
)

const (
	DataDirectoryName = "data"
	WalDirectoryName  = "wal"
)

type DB struct {
	// mu protects the global database state. This is only held when memtables
	// are being swapped along with the corresponding WAL.
	mu sync.Mutex

	name     string
	session  string
	openedAt time.Time
	seqNum   base.AtomicSeqNum

	// memtable is a concurrent in-memory KV store for all writes to the
	// database. The memtable is temporary, and once it is full, it is flushed
	// to disk.
	memtable *memtable.MemTable
	// We keep track of the current wal so that we can update the manifest file
	// when the memtable is rotated.
	wal *wal.WAL
	// activeMemtables is a list of memtables that are flushing or have been
	// flushed to disk. These memtables are no longer accepting writes, but may
	// still have reader references, thus are still potentially active. The DB
	// will periodically check if these memtables are still active and then
	// retire them and potentially recycle their arena.
	activeLatch     sync.RWMutex
	activeMemtables []*memtable.MemTable
	// recycledArena is an atomic pointer to a used arena that can be used in
	// the next memtable. This should be guaranteed to be reset by the caller of
	// atomic.Pointer.Store().
	recycledArena atomic.Pointer[arena.Arena]

	// flushing is a channel that is used to pass any flushable items to a
	// storage goroutine that will write the data to disk.
	flushing chan<- storage.Flusher
	storage  *storage.Manager

	// manifest tracks all state changes to the database files. Each update to
	// the manifest creates a new manifest file that is immediately flushed to
	// disk. Depending on the DB configuration, a certain number of manifest
	// file versions will be retained.
	manifest      *manifest.Manifest
	dataDirectory *os.File
	walDirectory  *os.File
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

// OpenAndCleanup opens the database in read-write mode to clean up logs,
// compact the database, and removes obsolete entries in the manifest file. Once
// the cleanup operations complete, this function returns nil error, the
// database is closed, and the directory file-lock is release.
//
// A database will typically run these operations sometime after Open, but this
// function provides a blocking cleanup operation without opening the database
// to read/write operations.
func OpenAndCleanup(directory string, options ...Option) (err error) {
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

// func (db *DB) Apply(batch string) error {
// 	panic("not implemented")
// }

func (db *DB) Set(key, value []byte) error {
	kv := base.InternalKV{
		K: base.MakeInternalKey(key, db.seqNum.Load(), base.InternalKeyKindSet),
		V: value,
	}
	err := db.memtable.Add(kv)
	if err != nil {
		if errors.Is(err, memtable.ErrMemtableFlushed) {
			// TODO handle memtable flush replacement
			return nil
		}
		if errors.Is(err, memtable.ErrMemtableFull) {
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

// func (db *DB) RangeKeySet(key, value []byte) error {
// 	panic("not implemented")
// }

func (db *DB) Delete(key []byte) error {
	kv := base.InternalKV{
		K: base.MakeInternalKey(key, db.seqNum.Load(), base.InternalKeyKindDelete),
		V: nil,
	}
	err := db.memtable.Add(kv)
	if err != nil {
		if errors.Is(err, memtable.ErrMemtableFlushed) {
			// TODO handle memtable flush replacement
			return nil
		}
		if errors.Is(err, memtable.ErrMemtableFull) {
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

// func (db *DB) SingleDelete(key []byte) error {
// 	panic("not implemented")
// }
//
// func (db *DB) RangeKeyDelete(start, end []byte) error {
// 	panic("not implemented")
// }
//
// func (db *DB) NewSnapshot() error {
// 	panic("not implemented")
// }

func (db *DB) RotateMemtable() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.memtable.IsActive() {
		// Another goroutine already rotated the memtable
		return nil
	}

	mem := db.memtable

	db.activeLatch.Lock()
	db.activeMemtables = append(db.activeMemtables, mem)
	db.activeLatch.Unlock()

	db.memtable = memtable.New(mem.Cap(), nil, compare.SimpleCompare)
	db.flushing <- mem

	return nil
}
