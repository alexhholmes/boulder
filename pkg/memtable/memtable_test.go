package memtable

import (
	"testing"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"

	"boulder/internal/base"
)

func TestMemtableFull(t *testing.T) {
	var err error
	memtable := New(directio.BlockSize * 8)
	defer func() {
		// Release mmap allocation for arena
		_ = memtable.ReleaseArena().Close()
	}()

	for i := 0; i < directio.BlockSize; i++ {
		key := base.MakeInternalKey([]byte{}, base.SeqNum(i), base.InternalKeyKindSet)
		kv := base.InternalKV{
			K: key,
			V: []byte{1, 0, 1, 0, 1, 0, 1},
		}

		err = memtable.Add(kv)
		if err != nil {
			break
		}
	}

	assert.ErrorIs(t, err, ErrMemtableFlushed)
}

func TestRecordExistsError(t *testing.T) {
	memtable := New(directio.BlockSize * 8)
	defer func() {
		// Release mmap allocation for arena
		_ = memtable.ReleaseArena().Close()
	}()

	key := base.MakeInternalKey([]byte{}, base.SeqNum(1), base.InternalKeyKindSet)
	kv := base.InternalKV{
		K: key,
		V: []byte{1, 0, 1, 0, 1, 0, 1},
	}

	err := memtable.Add(kv)
	assert.NoError(t, err)

	err = memtable.Add(kv)
	assert.ErrorIs(t, err, ErrRecordExists)
}
