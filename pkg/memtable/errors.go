package memtable

import (
	"errors"
)

var (
	// ErrActive indicates that the memtable is still has active references.
	ErrActive = errors.New("memtable is still has active references")

	// ErrFull indicates that underlying arena is full.
	ErrFull = errors.New("memtable is full")

	// ErrFlushed indicates that memtable flush() has been called.
	ErrFlushed = errors.New("memtable is has been flushed")

	// ErrRecordExists indicates that record with this key and sequence number already exists.
	ErrRecordExists = errors.New("record with this key already exists")

	// ErrInvalidSeqNum indicates that sequence number is not GTE to the memtable sequence number.
	ErrInvalidSeqNum = errors.New("invalid sequence number")
)
