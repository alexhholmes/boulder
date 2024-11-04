package base

type InternalKeyKind uint8

const (
	InternalKeyKindSet InternalKeyKind = iota
	InternalKeyKindDelete
	InternalKeyKindSingleDelete
	InternalKeyKindRangeKeySet
	InternalKeyKindRangeKeyDelete

	InternalKeyMask = 0xff
)

// InternalKV represents a single internal key-value pair.
type InternalKV struct {
	K InternalKey
	V []byte
}

// Kind returns the KVs internal key kind.
func (kv *InternalKV) Kind() InternalKeyKind {
	return kv.K.Trailer.Kind()
}

// SeqNum returns the KVs internal key sequence number.
func (kv *InternalKV) SeqNum() SeqNum {
	return kv.K.Trailer.SeqNum()
}

// InternalKey is a key used for the in-memory and on-disk partial DBs that
// make up a pebble DB.
//
// It consists of the logical key (as given by the code that uses package
// pebble) followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for the uint56 (uint64) sequence number, in little-endian format.
type InternalKey struct {
	LogicalKey []byte
	Trailer    InternalKeyTrailer
}

// MakeInternalKey constructs an internal key from a specified user key,
// sequence number and kind.
func MakeInternalKey(logicalKey []byte, seqNum SeqNum, kind InternalKeyKind) InternalKey {
	return InternalKey{
		LogicalKey: logicalKey,
		Trailer:    MakeTrailer(seqNum, kind),
	}
}

type InternalKeyTrailer uint64

// MakeTrailer constructs an internal key trailer from the specified sequence
// number and kind. A 56-bit sequence number followed by an 8-bit internal key
// kind.
func MakeTrailer(seqNum SeqNum, kind InternalKeyKind) InternalKeyTrailer {
	return (InternalKeyTrailer(seqNum) << 8) | InternalKeyTrailer(kind)
}

// SeqNum returns the sequence number component of the trailer.
func (t InternalKeyTrailer) SeqNum() SeqNum {
	return SeqNum(t >> 8)
}

// Kind returns the key kind component of the trailer (the final 8-bits of the
// uin64).
func (t InternalKeyTrailer) Kind() InternalKeyKind {
	return InternalKeyKind(t & InternalKeyMask)
}
