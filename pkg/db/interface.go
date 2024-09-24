package db

import "io"

type Reader interface {
	Get(key []byte) (value []byte, closer io.Closer, err error)
	Close() error
}

type Writer interface {
}
