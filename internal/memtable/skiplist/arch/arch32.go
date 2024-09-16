//go:build 386 || arm

package arch

import "sync/atomic"

type (
	AtomicInt  = atomic.Int32
	AtomicUint = atomic.Uint32
)
