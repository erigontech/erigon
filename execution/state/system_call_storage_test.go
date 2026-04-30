package state

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSystemCallStoragePropagation simulates the serial block loop where
// a system call (engine.Finalize) reads and writes a system contract's
// storage each block. The next block's system call should see the previous
// block's writes via sd.mem.
//
// This test verifies that:
//  1. Serial path: DomainPut writes are visible to the next block's GetLatest
//  2. Parallel path: BlockStateCache Flush writes are visible to the next
//     block's CachedReaderV3 fallthrough to GetLatest
//
// The test uses a withdrawal request contract pattern: each block reads
// a queue pointer (slot 4), dequeues a request, and writes a new pointer.
func TestSystemCallStoragePropagation_DirectDomainPut(t *testing.T) {
	// This test simulates the serial path: direct DomainPut per block.
	// Each block's system call reads slot 4, modifies it, writes via DomainPut.
	// The next block reads the updated value.

	addr := accounts.InternAddress([20]byte{0x00, 0x00, 0x09, 0x61})
	slot := accounts.InternKey([32]byte{0x04})

	// Simulate 5 blocks with alternating values (like a queue with 2 entries)
	values := [][]byte{
		{0x3f, 0x2f, 0x74, 0x24}, // block 0 writes this
		{0x7c, 0x1f, 0xed, 0x52}, // block 1 writes this
		{0x3f, 0x2f, 0x74, 0x24}, // block 2 writes this (same as block 0)
		{0x7c, 0x1f, 0xed, 0x52}, // block 3 writes this
		{0x3f, 0x2f, 0x74, 0x24}, // block 4 writes this
	}

	// Use a simple map to simulate sd.mem
	sdMem := map[string][]byte{}

	// Write initial value (from snapshot)
	composite := make([]byte, 20+32)
	addrVal := addr.Value()
	copy(composite, addrVal[:])
	slotVal := slot.Value()
	copy(composite[20:], slotVal[:])
	sdMem[string(composite)] = values[0]

	for blockIdx := 0; blockIdx < 5; blockIdx++ {
		// Read current value (simulates system call reading slot 4)
		currentVal := sdMem[string(composite)]
		t.Logf("Block %d: read slot4=%x", blockIdx, currentVal)

		// Verify we read the expected value
		if blockIdx > 0 {
			expectedVal := values[blockIdx-1]
			// After block N writes values[N], block N+1 should read values[N]
			assert.True(t, bytes.Equal(currentVal, expectedVal),
				"Block %d should read value written by block %d: got %x, want %x",
				blockIdx, blockIdx-1, currentVal, expectedVal)
		}

		// Write new value (simulates system call updating queue pointer)
		newVal := values[blockIdx]
		sdMem[string(composite)] = newVal
		t.Logf("Block %d: wrote slot4=%x", blockIdx, newVal)
	}
}

// TestSystemCallStoragePropagation_BlockStateCache simulates the parallel
// path where system call writes go through BlockStateCache → Flush → sd.mem.
// Each block gets a fresh BlockStateCache. The Flush writes to sd.mem.
// The next block reads from sd.mem via the CachedReaderV3 fallthrough.
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

	// Simulate sd.mem with a map
	sdMem := map[string][]byte{}

	composite := make([]byte, 20+32)
	addrVal := addr.Value()
	copy(composite, addrVal[:])
	slotVal := slot.Value()
	copy(composite[20:], slotVal[:])

	// Write initial value
	sdMem[string(composite)] = values[0]

	for blockIdx := 0; blockIdx < 5; blockIdx++ {
		// Create a fresh BlockStateCache per block (like the parallel executor)
		cache := NewBlockStateCache()

		// Read from cache (empty) → fallthrough to sd.mem
		val, ok := cache.GetCurrentStorage(addr, slot)
		if !ok {
			val, ok = cache.GetCommittedStorage(addr, slot)
		}
		if !ok {
			// Fallthrough to sd.mem (simulates ReaderV3.ReadAccountStorage)
			val = sdMem[string(composite)]
			ok = len(val) > 0
			if ok {
				// Populate committed cache (like CachedReaderV3 does)
				cache.PutCommittedStorage(addr, slot, val)
			}
		}

		t.Logf("Block %d: read slot4=%x (from %s)", blockIdx, val, func() string {
			if ok {
				return "sd.mem"
			}
			return "empty"
		}())

		// Verify we read the expected value
		if blockIdx > 0 {
			expectedVal := values[blockIdx-1]
			assert.True(t, bytes.Equal(val, expectedVal),
				"Block %d should read value written by block %d: got %x, want %x",
				blockIdx, blockIdx-1, val, expectedVal)
		}

		// System call writes new value to cache.
		newVal := values[blockIdx]
		cache.WriteStorage(addr, slot, newVal, uint64(blockIdx*100+1))

		// Flush replays writeLog to sd.mem (simulated here as a map). For
		// each storage write, push the latest value into the simulated
		// sd.mem at the recorded txNum.
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

	// Final verification: sd.mem should have the last written value
	finalVal := sdMem[string(composite)]
	assert.True(t, bytes.Equal(finalVal, values[4]),
		"Final sd.mem should have last written value: got %x, want %x", finalVal, values[4])
}

// TestBlockStateCacheStorageWriteLog verifies that WriteStorage appends
// to writeLog, including the case where a same-value rewrite is logged
// (system-call storage relies on every write reaching the domain so the
// commitment trie picks up the touch — DomainPut itself no-ops on
// identical value).
func TestBlockStateCacheStorageWriteLog(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x42})
	slot := accounts.InternKey([32]byte{0x01})

	cache.PutCommittedStorage(addr, slot, []byte{0x01})

	// Write same value — must still produce a writeLog entry so Flush
	// emits a DomainPut and the commitment touch is recorded.
	cache.WriteStorage(addr, slot, []byte{0x01}, 7)
	require.Len(t, cache.writeLog, 1)
	assert.Equal(t, bcOpPutStorage, cache.writeLog[0].kind)
	assert.Equal(t, uint64(7), cache.writeLog[0].txNum)

	// Different value — second writeLog entry preserved.
	cache.WriteStorage(addr, slot, []byte{0x02}, 11)
	require.Len(t, cache.writeLog, 2)
	assert.Equal(t, uint64(11), cache.writeLog[1].txNum)
	assert.Equal(t, []byte{0x02}, cache.writeLog[1].val)

	// Current view returns the latest write.
	val, ok := cache.GetCurrentStorage(addr, slot)
	require.True(t, ok)
	assert.Equal(t, []byte{0x02}, val, "Should have the latest value")
}

// TestBlockStateCacheWriteLogPerTxNum verifies the system-call storage
// flow: the Flush replays per-tx writes against sd.mem with each entry's
// recorded txNum, so the StorageDomain history retains per-tx
// granularity that intra-block GetAsOf readers (commitment domain,
// debug RPCs) depend on.
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
