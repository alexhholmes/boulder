//go:build amd64 || arm64

package arch

import "sync/atomic"

type (
	AtomicInt  = atomic.Int64
	AtomicUint = atomic.Uint64
)
