package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"boulder/internal/base"
	"boulder/pkg/manifest"
	"boulder/pkg/memtable"
	"boulder/pkg/wal"
)

const (
	DataDirectoryName = "data"
	WalDirectoryName  = "wal"
)

type DB struct {
	name     string
	session  string
	openedAt time.Time

	manifest *manifest.Manifest
	memtable *memtable.MemTable
	wal      *wal.WriteAheadLog

	dataDirectory *os.File
	walDirectory  *os.File
}

// TODO
// var _ Reader = (*DB)(nil)
// var _ Writer = (*DB)(nil)

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
	db.memtable = memtable.New(make(chan<- memtable.Flush)) // TODO replace with real flusher channel
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

// Get returns a copy of the value.
func (db *DB) Get(key []byte) (value []byte, err error) {
	panic("not implemented")
}

// GetPinned returns a reference to the value in the buffer.
func (db *DB) GetPinned(key []byte) ([]byte, error) {
	panic("not implemented")
}

func (db *DB) NewIterator() base.Iterator {
	panic("not implemented")
}

// NewSnapshotIterator returns a new iterator on a snapshot.
func (db *DB) NewSnapshotIterator() base.Iterator {
	panic("not implemented")
}

func (db *DB) Apply(batch string) error {
	panic("not implemented")
}

func (db *DB) Set(key, value []byte) error {
	panic("not implemented")
}

func (db *DB) RangeKeySet(key, value []byte) error {
	panic("not implemented")
}

func (db *DB) Delete(key []byte) error {
	panic("not implemented")
}

func (db *DB) SingleDelete(key []byte) error {
	panic("not implemented")
}

func (db *DB) RangeKeyDelete(start, end []byte) error {
	panic("not implemented")
}

func (db *DB) ListGroups() error {
	panic("not implemented")
}

func (db *DB) CreateGroup() error {
	panic("not implemented")
}

func (db *DB) DropGroup() error {
	panic("not implemented")
}

func (db *DB) DestroyGroup() error {
	panic("not implemented")
}

func (db *DB) NewSnapshot() error {
	panic("not implemented")
}
