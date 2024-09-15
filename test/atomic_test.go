package test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestAtomics(t *testing.T) {
	var num int

	num = 10

	var ptr atomic.Pointer[int]
	ptr.Store(&num)

	go func() {
		for !ptr.CompareAndSwap(nil, &num) {
		}
		fmt.Println("done", time.Now().String())
	}()

	fmt.Println("outer done", time.Now().String())

	time.Sleep(5 * time.Second)

	ptr.Store(nil)

	t.Log("done")
}
