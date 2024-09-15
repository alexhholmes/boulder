//go:build amd64 || arm64

package base

import "sync/atomic"

type AtomicInt = atomic.Int64
