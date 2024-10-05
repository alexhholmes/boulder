package memtable

import (
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"

	"boulder/internal/base"
)

func TestMemtable(t *testing.T) {
	var err error
	memtable := New(directio.BlockSize * 8)

	for i := 0; i < directio.BlockSize; i++ {
		key := base.MakeInternalKey([]byte{}, base.SeqNum(i), base.InternalKeyKindSet)
		kv := base.InternalKV{
			K: key,
			V: []byte{1, 0, 1, 0, 1, 0, 1},
		}

		err = memtable.Set(kv)
		if err != nil {
			break
		}
	}

	assert.ErrorIs(t, err, ErrMemtableFlushed)
}
