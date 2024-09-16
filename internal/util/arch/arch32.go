//go:build 386 || arm

package arch

import "sync/atomic"

type (
	AtomicInt  = atomic.Int32
	AtomicUint = atomic.Uint32
)

func IntToArchSize(n int) uint32 {
	return uint32(n)
}

func UintToArchSize(n uint) uint32 {
	return uint32(n)
}
