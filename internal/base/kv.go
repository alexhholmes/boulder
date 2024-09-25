package base

type InternalKeyKind uint8

const (
	InternalKeyKindDelete       InternalKeyKind = 0
	InternalKeyKindSet          InternalKeyKind = 1
	InternalKeyKindMerge        InternalKeyKind = 2
	InternalKeyKindLogData      InternalKeyKind = 3
	InternalKeyKindSingleDelete InternalKeyKind = 7
	InternalKeyKindRangeDelete  InternalKeyKind = 15

	// InternalKeyKindSeparator is a key used for separator / successor keys
	// written to sstable block indexes.
	InternalKeyKindSeparator InternalKeyKind = 17

	// InternalKeyKindRangeKeyDelete removes all range keys within a key range.
	// See the internal/rangekey package for more details.
	InternalKeyKindRangeKeyDelete InternalKeyKind = 19

	// InternalKeyKindRangeKeyUnset InternalKeyKindRangeKeySet represent
	// keys that set and unset values associated with ranges of key
	// space. See the internal/rangekey package for more details.
	InternalKeyKindRangeKeyUnset InternalKeyKind = 20
	InternalKeyKindRangeKeySet   InternalKeyKind = 21

	InternalKeyKindRangeKeyMin InternalKeyKind = InternalKeyKindRangeKeyDelete
	InternalKeyKindRangeKeyMax InternalKeyKind = InternalKeyKindRangeKeySet

	// InternalKeyKindIngestSST is used to distinguish a batch that corresponds to
	// the WAL entry for ingested sstables that are added to the flushable
	// queue. This InternalKeyKind cannot appear, amongst other key kinds in a
	// batch, or in an sstable.
	InternalKeyKindIngestSST InternalKeyKind = 22

	// InternalKeyKindMax maximum value isn't part of the file format.
	// Future extensions may increase this value.
	//
	// When constructing an internal key to pass to DB.Seek{GE,LE},
	// internalKeyComparer sorts decreasing by kind (after sorting increasing by
	// user key and decreasing by sequence number). Thus, use InternalKeyKindMax,
	// which sorts 'less than or equal to' any other valid internalKeyKind, when
	// searching for any kind of internal key formed by a certain user key and
	// seqNum.
	InternalKeyKindMax InternalKeyKind = 23

	// InternalKeyZeroSeqNumMaxTrailer is the largest trailer with a
	// zero sequence number.
	InternalKeyZeroSeqNumMaxTrailer InternalKeyTrailer = 255

	// InternalKeyRangeDeleteSentinel is the marker for a range delete sentinel
	// key. This sequence number and kind are used for the upper stable boundary
	// when a range deletion tombstone is the largest key in an sstable. This is
	// necessary because sstable boundaries are inclusive, while the end key of a
	// range deletion tombstone is exclusive.
	InternalKeyRangeDeleteSentinel = (InternalKeyTrailer(SeqNumMax) << 8) | InternalKeyTrailer(InternalKeyKindRangeDelete)

	// InternalKeyBoundaryRangeKey is the marker for a range key boundary. This
	// sequence number and kind are used during interleaved range key and point
	// iteration to allow an iterator to stop at range key start keys where
	// there exists no point key.
	InternalKeyBoundaryRangeKey = (InternalKeyTrailer(SeqNumMax) << 8) | InternalKeyTrailer(InternalKeyKindRangeKeySet)
)

type InternalKeyTrailer uint64

// MakeTrailer constructs an internal key trailer from the specified sequence
// number and kind.
func MakeTrailer(seqNum SeqNum, kind InternalKeyKind) InternalKeyTrailer {
	return (InternalKeyTrailer(seqNum) << 8) | InternalKeyTrailer(kind)
}

// SeqNum returns the sequence number component of the trailer.
func (t InternalKeyTrailer) SeqNum() SeqNum {
	return SeqNum(t >> 8)
}

// Kind returns the key kind component of the trailer.
func (t InternalKeyTrailer) Kind() InternalKeyKind {
	return InternalKeyKind(t & 0xff)
}

// InternalKey is a key used for the in-memory and on-disk partial DBs that
// make up a pebble DB.
//
// It consists of the user key (as given by the code that uses package pebble)
// followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in little-endian format.
type InternalKey struct {
	UserKey []byte
	Trailer InternalKeyTrailer
}

// MakeInternalKey constructs an internal key from a specified user key,
// sequence number and kind.
func MakeInternalKey(userKey []byte, seqNum SeqNum, kind InternalKeyKind) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: MakeTrailer(seqNum, kind),
	}
}

// InternalKV represents a single internal key-value pair.
type InternalKV struct {
	K InternalKey
	V []byte
}

// TODO
// // Kind returns the KV's internal key kind.
// func (kv *InternalKV) Kind() InternalKeyKind {
// 	return kv.K.Kind()
// }

// TODO
// // SeqNum returns the KV's internal key sequence number.
// func (kv *InternalKV) SeqNum() SeqNum {
// 	return kv.K.SeqNum()
// }
//
// // InPlaceValue returns the KV's in-place value.
// func (kv *InternalKV) InPlaceValue() []byte {
// 	return kv.V.InPlaceValue()
// }
//
// // Value return's the KV's underlying value.
// func (kv *InternalKV) Value(buf []byte) (val []byte, callerOwned bool, err error) {
// 	return kv.V.Value(buf)
// }

// MakeSearchKey constructs an internal key that is appropriate for searching
// for a the specified user key. The search key contain the maximal sequence
// number and kind ensuring that it sorts before any other internal keys for
// the same user key.
func MakeSearchKey(userKey []byte) InternalKey {
	return MakeInternalKey(userKey, SeqNumMax, InternalKeyKindMax)
}

// MakeRangeDeleteSentinelKey constructs an internal key that is a range
// deletion sentinel key, used as the upper boundary for an sstable when a
// range deletion is the largest key in an sstable.
func MakeRangeDeleteSentinelKey(userKey []byte) InternalKey {
	return InternalKey{
		UserKey: userKey,
		Trailer: InternalKeyRangeDeleteSentinel,
	}
}

// MakeExclusiveSentinelKey constructs an internal key that is an
// exclusive sentinel key, used as the upper boundary for an sstable
// when a ranged key is the largest key in an sstable.
func MakeExclusiveSentinelKey(kind InternalKeyKind, userKey []byte) InternalKey {
	return MakeInternalKey(userKey, SeqNumMax, kind)
}
