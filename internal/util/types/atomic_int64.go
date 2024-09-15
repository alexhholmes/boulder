//go:build amd64 || arm64

package types

import "sync/atomic"

type AtomicInt = atomic.Int64
