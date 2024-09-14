package batchmanager

type Operation struct {
}

type Batch interface {
}

func NewBatch(ops ...Operation) Batch {
	return nil
}
