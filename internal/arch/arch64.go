//go:build amd64 || arm64

package arch

import "sync/atomic"

type (
	AtomicInt  = atomic.Int64
	AtomicUint = atomic.Uint64
)

func IntToArchSize(n int) int64 {
	return int64(n)
}

func UintToArchSize(n uint) uint64 {
	return uint64(n)
}
