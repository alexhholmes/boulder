package memtable

import "errors"

var (
	ErrRecordExists    = errors.New("record with this key already exists")
	ErrMemtableFlushed = errors.New("memtable flushed")
	ErrMemtableActive  = errors.New("memtable is still has active references")
)
