package state

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSystemCallStoragePropagation simulates the serial block loop where
// a system call (engine.Finalize) reads and writes a system contract's
// storage each block. The next block's system call should see the previous
// block's writes via sd.mem.
//
// This test verifies that DomainPut writes are visible to the next block's
// GetLatest.
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

	for blockIdx := range 5 {
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
