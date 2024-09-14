package batchmanager

import (
	"boulder/pkg/lsm"
	"boulder/pkg/memtable"
)

// BatchManager will handle atomic transactions of multiple operations on the
// database. This will keep a record of latches maintained on specific keys
// and key ranges. This orchestrates the execution and ordering of batch
// operations.
type BatchManager struct {
	memtable *memtable.MemTable
	lsm      *lsm.LSM
}

func New(memtable *memtable.MemTable, lsm *lsm.LSM) *BatchManager {
	return &BatchManager{
		memtable: memtable,
		lsm:      lsm,
	}
}

func (bm *BatchManager) Execute(batch Batch) error {
	return nil
}
