package storage

// Flusher is an interface for objects that can be flushed to disk. It is
// expected that the object is in a read-only state when Flush is called.
type Flusher interface {
	Flush()
	AvailableBytes() uint
	UsedBytes() uint
	TotalBytes() uint
}
