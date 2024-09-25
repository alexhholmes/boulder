package storage

import (
	"io"
	"sync"
)

// Flusher is an interface that defines a type that can be iterated over and
// flushed as bytes to disk. Types implementing this interface simply provide
// raw bytes of the internal data structure to be written to disk. The caller
// is responsible for any headers, footers, or other metadata that is required
// by the file format.
//
// Implementing types should make this spawn a goroutine to write the data.
type Flusher interface {
	Flush(w io.WriteCloser) (flushed *sync.WaitGroup)
}
