package boulder

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"boulder/pkg/memtable"
)

const (
	DataDirectoryName = "data"
	WalDirectoryName  = "wal"
)

var (
	ErrNotFound = fmt.Errorf("boulder: not found")
)

type DB struct {
	name     string
	session  string
	openedAt time.Time

	memtable *memtable.MemTable

	dataDirectory *os.File
	walDirectory  *os.File
}

var _ Reader = (*DB)(nil)
var _ Writer = (*DB)(nil)
var _ io.Closer = (*DB)(nil)

// Open opens DB whose files reside in the given directory.
func Open(directory string, options ...Option) (db *DB, err error) {
	// Create directories if they don't exist
	if err = os.MkdirAll(filepath.Join(directory, DataDirectoryName), 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	if err = os.MkdirAll(filepath.Join(directory, WalDirectoryName), 0755); err != nil {
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
		filepath.Join(directory, DataDirectoryName),
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
		filepath.Join(directory, WalDirectoryName),
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

func (b *DB) Get(key []byte) (value []byte, closer io.Closer, err error) {
	value, finish, ok := b.memtable.Get(key)
	if !ok {
		return nil, nil, ErrNotFound
	}

	return value, Close(func() { finish() }), nil
}

func (b *DB) Set(key, value []byte) error {
	b.memtable.Set(key, value)
	return nil
}

func (b *DB) Delete(key []byte) error {
	b.memtable.Delete(key)
	return nil
}

func (b *DB) DeleteRange(start, end []byte) error {
	b.memtable.DeleteRange(start, end)
	return nil
}

// Close is a blocking call that will wait until all pending writes and
// compactions are finished before safely closing the DB.
func (b *DB) Close() error {
	var errs []error
	if err := b.dataDirectory.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close data directory: %w", err))
	}
	if err := b.walDirectory.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close wal directory: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("failed to close database: %w", errors.Join(errs...))
	}
	return nil
}
