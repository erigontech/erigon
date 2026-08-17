package state

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSystemCallStoragePropagation_DirectDomainPut pins that a system-call
// write in one block is visible to the next block's read via sd.mem (serial path).
func TestSystemCallStoragePropagation_DirectDomainPut(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x00, 0x00, 0x09, 0x61})
	slot := accounts.InternKey([32]byte{0x04})

	values := [][]byte{
		{0x3f, 0x2f, 0x74, 0x24},
		{0x7c, 0x1f, 0xed, 0x52},
		{0x3f, 0x2f, 0x74, 0x24},
		{0x7c, 0x1f, 0xed, 0x52},
		{0x3f, 0x2f, 0x74, 0x24},
	}

	sdMem := map[string][]byte{}

	composite := make([]byte, 20+32)
	addrVal := addr.Value()
	copy(composite, addrVal[:])
	slotVal := slot.Value()
	copy(composite[20:], slotVal[:])
	sdMem[string(composite)] = values[0]

	for blockIdx := range 5 {
		currentVal := sdMem[string(composite)]
		t.Logf("Block %d: read slot4=%x", blockIdx, currentVal)

		if blockIdx > 0 {
			expectedVal := values[blockIdx-1]
			assert.True(t, bytes.Equal(currentVal, expectedVal),
				"Block %d should read value written by block %d: got %x, want %x",
				blockIdx, blockIdx-1, currentVal, expectedVal)
		}

		newVal := values[blockIdx]
		sdMem[string(composite)] = newVal
		t.Logf("Block %d: wrote slot4=%x", blockIdx, newVal)
	}
}

// TestSystemCallStoragePropagation_BlockStateCache pins the parallel path:
// BlockStateCache Flush writes reach sd.mem for the next block to read.
func TestSystemCallStoragePropagation_BlockStateCache(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x00, 0x00, 0x09, 0x61})
	slot := accounts.InternKey([32]byte{0x04})

	values := [][]byte{
		{0x3f, 0x2f, 0x74, 0x24},
		{0x7c, 0x1f, 0xed, 0x52},
		{0x3f, 0x2f, 0x74, 0x24},
		{0x7c, 0x1f, 0xed, 0x52},
		{0x3f, 0x2f, 0x74, 0x24},
	}

	sdMem := map[string][]byte{}

	composite := make([]byte, 20+32)
	addrVal := addr.Value()
	copy(composite, addrVal[:])
	slotVal := slot.Value()
	copy(composite[20:], slotVal[:])

	sdMem[string(composite)] = values[0]

	for blockIdx := range 5 {
		cache := NewBlockStateCache()

		val, ok := cache.GetCurrentStorage(addr, slot)
		if !ok {
			val, ok = cache.GetCommittedStorage(addr, slot)
		}
		if !ok {
			val = sdMem[string(composite)]
			ok = len(val) > 0
			if ok {
				cache.PutCommittedStorage(addr, slot, val)
			}
		}

		t.Logf("Block %d: read slot4=%x (from %s)", blockIdx, val, func() string {
			if ok {
				return "sd.mem"
			}
			return "empty"
		}())

		if blockIdx > 0 {
			expectedVal := values[blockIdx-1]
			assert.True(t, bytes.Equal(val, expectedVal),
				"Block %d should read value written by block %d: got %x, want %x",
				blockIdx, blockIdx-1, val, expectedVal)
		}

		newVal := values[blockIdx]
		cache.WriteStorage(addr, slot, newVal, uint64(blockIdx*100+1))

		for i := range cache.writeLog {
			op := &cache.writeLog[i]
			if op.kind != bcOpPutStorage {
				continue
			}
			opAddrVal := op.addr.Value()
			opKeyVal := op.key.Value()
			c := make([]byte, 20+32)
			copy(c, opAddrVal[:])
			copy(c[20:], opKeyVal[:])
			sdMem[string(c)] = op.val
			t.Logf("Block %d: flushed slot=%x val=%x at txNum=%d", blockIdx, opKeyVal, op.val, op.txNum)
		}
	}

	finalVal := sdMem[string(composite)]
	assert.True(t, bytes.Equal(finalVal, values[4]),
		"Final sd.mem should have last written value: got %x, want %x", finalVal, values[4])
}

// TestBlockStateCacheStorageWriteLog pins that a same-value rewrite still
// appends to writeLog: the commitment trie needs the touch even though DomainPut no-ops.
func TestBlockStateCacheStorageWriteLog(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x42})
	slot := accounts.InternKey([32]byte{0x01})

	cache.PutCommittedStorage(addr, slot, []byte{0x01})

	cache.WriteStorage(addr, slot, []byte{0x01}, 7)
	require.Len(t, cache.writeLog, 1)
	assert.Equal(t, bcOpPutStorage, cache.writeLog[0].kind)
	assert.Equal(t, uint64(7), cache.writeLog[0].txNum)

	cache.WriteStorage(addr, slot, []byte{0x02}, 11)
	require.Len(t, cache.writeLog, 2)
	assert.Equal(t, uint64(11), cache.writeLog[1].txNum)
	assert.Equal(t, []byte{0x02}, cache.writeLog[1].val)

	val, ok := cache.GetCurrentStorage(addr, slot)
	require.True(t, ok)
	assert.Equal(t, []byte{0x02}, val, "Should have the latest value")
}

// TestBlockStateCacheWriteLogPerTxNum pins that Flush replays each write at
// its own recorded txNum, preserving per-tx granularity for GetAsOf readers.
func TestBlockStateCacheWriteLogPerTxNum(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x42})
	slot := accounts.InternKey([32]byte{0x04})

	oldVal := []byte{0x3f, 0x2f}
	newVal := []byte{0x7c, 0x1f}

	cache.PutCommittedStorage(addr, slot, oldVal)
	cache.WriteStorage(addr, slot, newVal, 42)

	require.Len(t, cache.writeLog, 1)
	op := cache.writeLog[0]
	assert.Equal(t, bcOpPutStorage, op.kind)
	assert.Equal(t, addr, op.addr)
	assert.Equal(t, slot, op.key)
	assert.Equal(t, newVal, op.val)
	assert.Equal(t, uint64(42), op.txNum)

	val, ok := cache.GetCurrentStorage(addr, slot)
	require.True(t, ok)
	assert.Equal(t, newVal, val)

	committed, ok := cache.GetCommittedStorage(addr, slot)
	require.True(t, ok)
	assert.Equal(t, oldVal, committed)
}
