//go:build 386 || arm

package types

import "sync/atomic"

type AtomicInt = atomic.Int32
