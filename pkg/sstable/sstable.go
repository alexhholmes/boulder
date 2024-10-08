package sstable

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/ncw/directio"

	"boulder/pkg/iterator"
)

type SSTable struct {
	latch    atomic.Int32
	id       uint64
	filename string
	file     *os.File
	level    uint64
	size     int64
}

type TableFormat int32

type footer struct {
	format TableFormat
}

func New(filename string, id, level uint64, iterator iterator.Iterator) (*SSTable, error) {
	// Open the file and write the contents of the reader to it
	file, err := directio.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to open new lsm file: %w", err)
	}

	// Copy the contents of the reader to the file
	_, err = io.Copy(file, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to copy to new lsm: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	return &SSTable{
		id:       id,
		filename: filename,
		file:     file,
		level:    level,
		size:     stat.Size(),
	}, nil
}

func (s *SSTable) Level() uint64 {
	return s.level
}

func (s *SSTable) Read() (reader io.ReadSeeker, close func()) {
	// Add a latch to the lsm so that we can track the number of readers
	// when we are compacting the tables and need to delete this lsm once
	// it has been merged. If the latch is non-zero, then a background cleanup
	// goroutine will eventually delete this lsm once all readers have
	// finished.
	s.latch.Add(1)
	return s.file, func() {
		s.latch.Add(-1)
	}
}

func (s *SSTable) Close() error {
	return s.file.Close()
}
