package pkg

import (
	"io"

	"boulder/internal/db"
)

var _ ReadWriterCloser = (*Boulder)(nil)

type Boulder struct {
	db ReadWriterCloser
}

// Open opens DB whose files reside in the given directory.
func Open(directory string, options ...Option) (Boulder, error) {
	database, err := db.Open(directory, options...)
	if err != nil {
		return Boulder{}, err
	}
	return Boulder{db: database}, nil
}

func (b *Boulder) Get(key []byte) ([]byte, io.Closer, error) {
	return b.db.Get(key)
}

func (b *Boulder) Set(key, value []byte) error {
	return b.db.Set(key, value)
}

func (b *Boulder) Delete(key []byte) error {
	return b.db.Delete(key)
}

func (b *Boulder) DeleteRange(start, end []byte) error {
	return b.db.DeleteRange(start, end)
}

func (b *Boulder) Close() error {
	return b.db.Close()
}
