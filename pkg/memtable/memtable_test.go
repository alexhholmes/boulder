package memtable

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"boulder/internal/base"
	"boulder/internal/compare"
	"boulder/internal/directio"
)

func TestMemtableFull(t *testing.T) {
	var err error
	memtable := New(directio.BlockSize*8, nil, compare.SimpleCompare)
	defer func() {
		// Release mmap allocation for arena
		a, err := memtable.ReleaseArena()
		assert.NoError(t, err)
		err = a.Close()
		assert.NoError(t, err)
	}()

	for i := 0; i < directio.BlockSize; i++ {
		key := base.MakeInternalKey([]byte{}, base.SeqNum(i), base.InternalKeyKindSet)
		kv := base.InternalKV{
			K: key,
			V: []byte{1, 0, 1, 0, 1, 0, 1},
		}

		t.Logf("Adding key %d", i)
		err = memtable.Insert(kv)
		if err != nil {
			break
		}
	}

	assert.ErrorIs(t, err, ErrFlushed)
}

func TestRecordExistsError(t *testing.T) {
	memtable := New(directio.BlockSize, nil, compare.SimpleCompare)
	defer func() {
		// Release mmap allocation for arena
		a, err := memtable.ReleaseArena()
		assert.NoError(t, err)
		err = a.Close()
		assert.NoError(t, err)
	}()

	key := base.MakeInternalKey([]byte{}, base.SeqNum(1), base.InternalKeyKindSet)
	kv := base.InternalKV{
		K: key,
		V: []byte{1, 0, 1, 0, 1, 0, 1},
	}

	err := memtable.Insert(kv)
	assert.NoError(t, err)

	err = memtable.Insert(kv)
	assert.ErrorIs(t, err, ErrRecordExists)
}

func TestEmptyMemtable(t *testing.T) {
	memtable := New(directio.BlockSize, nil, compare.SimpleCompare)
	defer func() {
		// Release mmap allocation for arena
		a, err := memtable.ReleaseArena()
		assert.NoError(t, err)
		err = a.Close()
		assert.NoError(t, err)
	}()

	assert.True(t, memtable.Empty())

	key := base.MakeInternalKey([]byte{}, base.SeqNum(1), base.InternalKeyKindSet)
	kv := base.InternalKV{
		K: key,
		V: []byte{},
	}

	err := memtable.Insert(kv)
	assert.NoError(t, err)
	assert.False(t, memtable.Empty())

	// TODO assert false after deleting a key
}
