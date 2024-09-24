package db

import "fmt"

var (
	ErrKeyNotFound = fmt.Errorf("boulder: key not found")
	ErrReadOnly    = fmt.Errorf("boulder: read only")
	ErrClosed      = fmt.Errorf("boulder: database closed")
)
