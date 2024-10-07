package memtable

import "errors"

var (
	ErrRecordExists    = errors.New("record with this key already exists")
	ErrMemtableFlushed = errors.New("memtable is has been flushed")
	ErrMemtableFull    = errors.New("memtable is full")
	ErrMemtableActive  = errors.New("memtable is still has active references")
	ErrInvalidSeqNum   = errors.New("invalid sequence number")
)
