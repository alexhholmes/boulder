package base

import "sync/atomic"

// SeqNum is a sequence number defining precedence among identical keys. A key
// with a higher sequence number takes precedence over a key with an equal user
// key of a lower sequence number. Sequence numbers are stored durably within
// the internal key "trailer" as a 7-byte (uint56) uint, and the maximum
// sequence number is 2^56-1. As keys are committed to the database, they're
// assigned increasing sequence numbers. Readers use sequence numbers to read a
// consistent database state, ignoring keys with sequence numbers larger than
// the readers' "visible sequence number."
//
// The database maintains an invariant that no two point keys with equal user
// keys may have equal sequence numbers. Keys with differing user keys may have
// equal sequence numbers. A point key and a range deletion or range key that
// include that point key can have equal sequence numbers - in that case, the
// range key does not apply to the point key. A key's sequence number may be
// changed to zero during compactions when it can be proven that no identical
// keys with lower sequence numbers exist.
type SeqNum uint64

const SeqNumMax = SeqNum(^uint64(0) >> 8)

type AtomicSeqNum struct {
	value atomic.Uint64
}

// Load atomically loads and returns the stored SeqNum.
func (asn *AtomicSeqNum) Load() SeqNum {
	return SeqNum(asn.value.Load())
}

// Store atomically stores s.
func (asn *AtomicSeqNum) Store(s SeqNum) {
	asn.value.Store(uint64(s))
}

// Add atomically adds delta to asn and returns the new value.
func (asn *AtomicSeqNum) Add(delta SeqNum) SeqNum {
	return SeqNum(asn.value.Add(uint64(delta)))
}

// CompareAndSwap executes the compare-and-swap operation.
func (asn *AtomicSeqNum) CompareAndSwap(old, new SeqNum) bool {
	return asn.value.CompareAndSwap(uint64(old), uint64(new))
}
