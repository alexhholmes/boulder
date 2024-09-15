//go:build 386 || arm

package base

import "sync/atomic"

type AtomicInt = atomic.Int32
