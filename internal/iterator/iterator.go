package iterator

import (
	"io"

	"boulder/internal/base"
)

type Iterator interface {
	First() *base.InternalKV
	Last() *base.InternalKV
	Next() *base.InternalKV
	Prev() *base.InternalKV
	io.Closer
}
