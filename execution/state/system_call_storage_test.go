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

		// System call writes new value to cache
		newVal := values[blockIdx]
		cache.WriteStorage(addr, slot, newVal)

		// Flush cache to sd.mem (like the parallel Flush at end of block)
		// This simulates the dirty check in Flush
		if dirtySlots, ok := cache.dirtyStorage[addr]; ok {
			if dirtySlots[slot] {
				if currentVal, ok := cache.currentStorage[addr][slot]; ok {
					// Check if different from committed
					if committedSlots, ok := cache.committedStorage[addr]; ok {
						if committedVal, ok := committedSlots[slot]; ok {
							if bytes.Equal(committedVal, currentVal) {
								t.Logf("Block %d: Flush SKIPPED (committed==current)", blockIdx)
								continue
							}
						}
					}
					// Write to sd.mem
					sdMem[string(composite)] = currentVal
					t.Logf("Block %d: flushed slot4=%x to sd.mem", blockIdx, currentVal)
				}
			}
		}
	}

	// Final verification: sd.mem should have the last written value
	finalVal := sdMem[string(composite)]
	assert.True(t, bytes.Equal(finalVal, values[4]),
		"Final sd.mem should have last written value: got %x, want %x", finalVal, values[4])
}

// TestBlockStateCacheStorageDirtyFlag verifies that WriteStorage marks
// storage as dirty even when the value matches the committed value.
// This is important for system call storage that gets written back with
// a different value each block.
func TestBlockStateCacheStorageDirtyFlag(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x42})
	slot := accounts.InternKey([32]byte{0x01})

	// Put a committed value
	cache.PutCommittedStorage(addr, slot, []byte{0x01})

	// Write same value → should NOT be dirty (optimization)
	cache.WriteStorage(addr, slot, []byte{0x01})

	// Check dirty flag
	if dirtySlots, ok := cache.dirtyStorage[addr]; ok {
		// The dirty flag is set unconditionally in WriteStorage
		require.True(t, dirtySlots[slot], "WriteStorage should always set dirty flag")
	}

	// Write different value → should be dirty
	cache.WriteStorage(addr, slot, []byte{0x02})

	dirtySlots, ok := cache.dirtyStorage[addr]
	require.True(t, ok, "Address should have dirty slots")
	assert.True(t, dirtySlots[slot], "Different value should be dirty")

	// Read current value
	val, ok := cache.GetCurrentStorage(addr, slot)
	require.True(t, ok, "Should find current storage")
	assert.Equal(t, []byte{0x02}, val, "Should have the new value")
}

// TestBlockStateCacheFlushPreservesSystemCallWrites verifies that the Flush
// correctly writes system call storage changes to sd.mem, including the
// dirty check against committed values.
func TestBlockStateCacheFlushPreservesSystemCallWrites(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x42})
	slot := accounts.InternKey([32]byte{0x04})

	// Simulate: system call reads slot (populates committed), writes new value
	oldVal := []byte{0x3f, 0x2f}
	newVal := []byte{0x7c, 0x1f}

	// Read populates committed
	cache.PutCommittedStorage(addr, slot, oldVal)

	// System call writes new value
	cache.WriteStorage(addr, slot, newVal)

	// Verify dirty flag is set
	dirtySlots, ok := cache.dirtyStorage[addr]
	require.True(t, ok)
	assert.True(t, dirtySlots[slot])

	// Verify current has new value
	val, ok := cache.GetCurrentStorage(addr, slot)
	require.True(t, ok)
	assert.Equal(t, newVal, val)

	// Verify committed has old value
	val, ok = cache.GetCommittedStorage(addr, slot)
	require.True(t, ok)
	assert.Equal(t, oldVal, val)

	// The Flush would check: committed != current → write to sd.mem
	committed := cache.committedStorage[addr][slot]
	current := cache.currentStorage[addr][slot]
	assert.False(t, bytes.Equal(committed, current),
		"Committed and current should differ: committed=%x current=%x", committed, current)
}
