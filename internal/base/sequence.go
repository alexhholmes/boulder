package base

import (
	"fmt"
	"strconv"
	"sync/atomic"
)

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

const (
	// SeqNumZero is the zero sequence number, set by compactions if they can
	// guarantee there are no keys underneath an internal key.
	SeqNumZero SeqNum = 0
	// SeqNumStart is the first sequence number assigned to a key. Sequence
	// numbers 1-9 are reserved for potential future use.
	SeqNumStart SeqNum = 10
	// SeqNumMax is the largest valid sequence number.
	SeqNumMax SeqNum = 1<<56 - 1
	// SeqNumBatchBit is set on batch sequence numbers which prevents those
	// entries from being excluded from iteration.
	SeqNumBatchBit SeqNum = 1 << 55
)

func (s SeqNum) String() string {
	if s == SeqNumMax {
		return "inf"
	}
	var batch string
	if s&SeqNumBatchBit != 0 {
		batch = "b"
		s &^= SeqNumBatchBit
	}
	return fmt.Sprintf("%s%d", batch, s)
}

// ParseSeqNum parses the string representation of a sequence number.
// "inf" is supported as the maximum sequence number (mainly used for exclusive
// end keys).
func ParseSeqNum(s string) SeqNum {
	if s == "inf" {
		return SeqNumMax
	}
	batch := s[0] == 'b'
	if batch {
		s = s[1:]
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("error parsing %q as seqnum: %s", s, err))
	}
	seqNum := SeqNum(n)
	if batch {
		seqNum |= SeqNumBatchBit
	}
	return seqNum
}

// AtomicSeqNum is an atomic SeqNum.
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
