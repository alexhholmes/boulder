package compare

import (
	"bytes"
)

type Compare func(a, b []byte) int

// SuffixCompare compares the suffix of a and b if the prefix of a and b are
// equal. If the prefix of a and b are different, it returns the result of
// bytes.Compare(a, b). This is because a base.InternalKey can have the same
// user key but different sequence numbers. This ignores the kind field of the
// last byte of the internal key.
func SuffixCompare(a, b []byte) int {
	seqA := a[len(a)-8 : len(a)-1]
	seqB := b[len(b)-8 : len(b)-1]
	return bytes.Compare(seqA, seqB)
}
