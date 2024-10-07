package iterator

import (
	"io"

	"boulder/internal/base"
)

type Iterator interface {
	// SeekGE(key []byte, flags SeekGEFlags) *InternalKV
	// SeekPrefixGE(prefix, key []byte, flags SeekGEFlags) *InternalKV
	// SeekLT(key []byte, flags SeekLTFlags) *InternalKV
	First() *base.InternalKV
	Last() *base.InternalKV
	Next() *base.InternalKV
	// NextPrefix(succKey []byte) *InternalKV
	Prev() *base.InternalKV
	// Error() error
	// SetBounds(lower, upper []byte)
	io.Closer
}
