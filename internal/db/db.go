package db

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"boulder/internal/manifest"
	"boulder/internal/memtable"
	"boulder/internal/storage"
	"boulder/internal/wal"
	"boulder/pkg"
)

const (
	DataDirectoryName = "data"
	WalDirectoryName  = "wal"
)

var (
	ErrNotFound = fmt.Errorf("pkg: not found")
)

type DB struct {
	name     string
	session  string
	openedAt time.Time

	manifest *manifest.Manifest
	memtable *memtable.MemTable
	wal      *wal.WriteAheadLog

	storage       *storage.Storage
	dataDirectory *os.File
	walDirectory  *os.File
}

var _ pkg.ReadWriterCloser = (*DB)(nil)

func Open(directory string, options ...pkg.Option) (db *DB, err error) {
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
	db.storage = storage.Open(dataDirectoryPath, walDirectoryPath)
	db.memtable = memtable.NewMemTable(make(chan<- memtable.Flush)) // TODO replace with real flusher channel
	db.openedAt = time.Now()

	// Attempt to close resources on panic
	defer func() {
		if r := recover(); r != nil {
			_ = db.Close()
		}
	}()

	return db, nil
}

func (db *DB) Get(key []byte) (value []byte, closer io.Closer, err error) {
	value, finish, ok := db.memtable.Get(key)
	if !ok {
		return nil, nil, ErrNotFound
	}

	return value, Close(func() { finish() }), nil
}

func (db *DB) Set(key, value []byte) error {
	db.memtable.Set(key, value)
	return nil
}

func (db *DB) Delete(key []byte) error {
	db.memtable.Delete(key)
	return nil
}

func (db *DB) DeleteRange(start, end []byte) error {
	db.memtable.DeleteRange(start, end)
	return nil
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
