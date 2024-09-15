package pkg

import "io"

type ReadWriterCloser interface {
	Reader
	Writer
	io.Closer
}

type Reader interface {
	// Get gets the value for the given key. It returns ErrNotFound if the DB
	// does not contain the key.
	//
	// The caller should not modify the contents of the returned slice, but it is
	// safe to modify the contents of the argument after Get returns. The
	// returned slice will remain valid until the returned Closer is closed. On
	// success, the caller MUST call closer.Close() or a memory leak will occur.
	Get(key []byte) (value []byte, closer io.Closer, err error)
}

type Writer interface {
	// Set sets the value for the given key, overwriting any previous value for
	// that key if it exists, and inserting the key-value pair if it does not.
	Set(key, value []byte) error

	// Delete deletes the value for the given key. It is a blind delete, i.e.
	// it does not return an error if the key does not exist.
	Delete(key []byte) error

	// DeleteRange deletes all keys in the range [start, end) (inclusive on
	// start, exclusive on end). Similarly to Delete, it is a blind delete.
	DeleteRange(start, end []byte) error
}
