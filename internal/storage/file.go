package storage

import (
	"io"
	"os"
	"sync"

	"github.com/ncw/directio"
)

// Writer is a wrapper around `directio.File`. This will write data to the file
// in multiples of the block size. If there is any data that is not a multiple
// of the block size, it will be written to the file in the next block with
// padding.
type Writer struct {
	file  *os.File
	block int
}

var once sync.Once

func NewWriter(name string, flag int) (*Writer, error) {
	file, err := directio.OpenFile(name, flag, 0755)
	if err != nil {
		return nil, err
	}

	block := directio.BlockSize
	once.Do(func() {
		block = len(directio.AlignedBlock(directio.BlockSize))
	})

	return &Writer{
		file:  file,
		block: block,
	}, nil
}

var _ io.WriteCloser = (*Writer)(nil)

// Write writes in multiples of the block size. If the data is not a multiple
// of the block size, it will be written to the file in the next block with
// padding. This will return the number of blocks written to the file. This
// will be useful for the SSTable to keep track of the number of blocks written
// for the footer.
func (f *Writer) Write(buf []byte) (n int, err error) {
	if len(buf) == 0 {
		return 0, nil
	}

	blocks := len(buf) / f.block
	rem := len(buf) % f.block

	if rem > 0 {
		// Write the entire slice except the last block, which will be padded
		n, err = f.file.Write(buf[:len(buf)-rem])
		if err != nil {
			return n, err
		}

		// Write the last block with padding
		var p int
		pad := make([]byte, f.block-rem)
		p, err = f.file.Write(append(buf[len(buf)-rem:], pad...))
		if err != nil {
			return n + p, err
		}

		return blocks + 1, nil
	}

	// Safe to write the entire slice
	n, err = f.file.Write(buf)
	if err != nil {
		return n, err
	}

	return blocks, nil
}

func (f *Writer) Close() error {
	return f.file.Close()
}
