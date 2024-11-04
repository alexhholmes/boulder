package sstable

import "boulder/pkg/memtable"

type SSTable struct {
	memtables []memtable.MemTable
}

func (s *SSTable) NextBlock() {

}
