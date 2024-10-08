//go:build linux

package directio

import (
	"os"
	"syscall"
)

const (
	AlignSize = 4096
	BlockSize = 4096
	DirectIO  = true
)

// OpenFile is a modified version of os.OpenFile which sets O_DIRECT.
func OpenFile(name string, flag int, perm os.FileMode) (file *os.File, err error) {
	return os.OpenFile(name, syscall.O_DIRECT|flag, perm)
}
