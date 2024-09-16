package skiplist

import (
	"math"
	"unsafe"
)

const (
	maxNodeSize   = uint(unsafe.Sizeof(node{}))
	linksSize     = uint(unsafe.Sizeof(links{}))
	maxHeight     = uint(20)
	pValue        = 1 / math.E
	nodeAlignment = 4
)

var probabilities [maxHeight]uint32

func init() {
	// Precompute the skiplist probabilities so that only a single random number
	// needs to be generated and so that the optimal pvalue can be used (inverse
	// of Euler's number).
	p := float64(1.0)
	for i := uint(0); i < maxHeight; i++ {
		probabilities[i] = uint32(float64(math.MaxUint32) * p)
		p *= pValue
	}
}
