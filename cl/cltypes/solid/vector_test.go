// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package solid

import (
	"fmt"
	"testing"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a simple attestation for testing
func newTestAttestation(slot, committeeIndex uint64, numBytes int) *Attestation {
	att := &Attestation{
		AggregationBits: NewBitList(numBytes, 2048),
		Data: &AttestationData{
			Slot:           slot,
			CommitteeIndex: committeeIndex,
		},
		Signature: common.Bytes96{},
	}
	// Set some bytes for variation
	for i := 0; i < numBytes && i < 10; i++ {
		att.AggregationBits.Set(i, byte(i))
	}
	return att
}

// TestNewVectorSSZ tests the creation of a new VectorSSZ
func TestNewVectorSSZ(t *testing.T) {
	size := 5
	vec := NewVectorSSZ[*Checkpoint](size)

	assert.NotNil(t, vec)
	assert.Equal(t, size, vec.Length())
	assert.Equal(t, size, vec.Cap())
}

// TestVectorSSZ_GetSet tests Get and Set operations
func TestVectorSSZ_GetSet(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Checkpoint](size)

	// Create test checkpoints
	cp1 := &Checkpoint{
		Epoch: 100,
		Root:  common.HexToHash("0x01"),
	}

	cp2 := &Checkpoint{
		Epoch: 200,
		Root:  common.HexToHash("0x02"),
	}

	// Set values
	vec.Set(0, cp1)
	vec.Set(1, cp2)

	// Get and verify values
	retrieved1 := vec.Get(0)
	retrieved2 := vec.Get(1)

	assert.Equal(t, cp1.Epoch, retrieved1.Epoch)
	assert.Equal(t, cp1.Root, retrieved1.Root)
	assert.Equal(t, cp2.Epoch, retrieved2.Epoch)
	assert.Equal(t, cp2.Root, retrieved2.Root)
}

// TestVectorSSZ_LengthCap tests Length and Cap operations
func TestVectorSSZ_LengthCap(t *testing.T) {
	size := 10
	vec := NewVectorSSZ[*Checkpoint](size)

	assert.Equal(t, size, vec.Length())
	assert.Equal(t, size, vec.Cap())
}

// TestVectorSSZ_Static tests the Static method
func TestVectorSSZ_Static(t *testing.T) {
	vec := NewVectorSSZ[*Checkpoint](3)
	// Checkpoint is a static type (fixed size)
	assert.True(t, vec.Static())
}

// TestVectorSSZ_Clear tests the Clear operation
func TestVectorSSZ_Clear(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Checkpoint](size)

	// Set some values
	cp := &Checkpoint{
		Epoch: 100,
		Root:  common.HexToHash("0x01"),
	}
	vec.Set(0, cp)
	vec.Set(1, cp)

	// Clear
	vec.Clear()

	// Verify all values are reset to zero
	for i := 0; i < size; i++ {
		retrieved := vec.Get(i)
		assert.Equal(t, uint64(0), retrieved.Epoch)
		assert.Equal(t, common.Hash{}, retrieved.Root)
	}
}

// TestVectorSSZ_CopyTo tests the CopyTo operation
func TestVectorSSZ_CopyTo(t *testing.T) {
	size := 3
	vec1 := NewVectorSSZ[*Checkpoint](size)
	vec2 := NewVectorSSZ[*Checkpoint](size)

	// Set values in vec1
	cp := &Checkpoint{
		Epoch: 100,
		Root:  common.HexToHash("0x01"),
	}
	vec1.Set(0, cp)
	vec1.Set(1, cp)

	// Copy to vec2
	vec1.CopyTo(vec2)

	// Verify values are copied
	for i := 0; i < 2; i++ {
		assert.Equal(t, vec1.Get(i).Epoch, vec2.Get(i).Epoch)
		assert.Equal(t, vec1.Get(i).Root, vec2.Get(i).Root)
	}
}

// TestVectorSSZ_CopyTo_DifferentSizes tests CopyTo with different sizes should panic
func TestVectorSSZ_CopyTo_DifferentSizes(t *testing.T) {
	vec1 := NewVectorSSZ[*Checkpoint](3)
	vec2 := NewVectorSSZ[*Checkpoint](5)

	assert.Panics(t, func() {
		vec1.CopyTo(vec2)
	})
}

// TestVectorSSZ_Range tests the Range operation
func TestVectorSSZ_Range(t *testing.T) {
	size := 5
	vec := NewVectorSSZ[*Checkpoint](size)

	// Set some values
	for i := 0; i < size; i++ {
		cp := &Checkpoint{
			Epoch: uint64(i * 100),
			Root:  common.HexToHash(fmt.Sprintf("0x%d", i)),
		}
		vec.Set(i, cp)
	}

	// Range through all values
	count := 0
	vec.Range(func(index int, value *Checkpoint, length int) bool {
		assert.Equal(t, size, length)
		assert.Equal(t, uint64(index*100), value.Epoch)
		count++
		return true
	})
	assert.Equal(t, size, count)

	// Test early termination
	count = 0
	vec.Range(func(index int, value *Checkpoint, length int) bool {
		count++
		return index < 2 // Stop after 3 iterations
	})
	assert.Equal(t, 3, count)
}

// TestVectorSSZ_EncodeDecodeSSZ tests SSZ encoding and decoding
func TestVectorSSZ_EncodeDecodeSSZ(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Checkpoint](size)

	// Set some values
	for i := 0; i < size; i++ {
		cp := &Checkpoint{
			Epoch: uint64(i * 100),
			Root:  common.HexToHash(fmt.Sprintf("0x%d", i)),
		}
		vec.Set(i, cp)
	}

	// Encode
	encoded, err := vec.EncodeSSZ(nil)
	require.NoError(t, err)
	assert.NotEmpty(t, encoded)

	// Decode
	vec2 := NewVectorSSZ[*Checkpoint](size)
	err = vec2.DecodeSSZ(encoded, 0)
	require.NoError(t, err)

	// Verify decoded values match original
	for i := 0; i < size; i++ {
		original := vec.Get(i)
		decoded := vec2.Get(i)
		assert.Equal(t, original.Epoch, decoded.Epoch)
		assert.Equal(t, original.Root, decoded.Root)
	}
}

// TestVectorSSZ_EncodingSizeSSZ tests the encoding size calculation
func TestVectorSSZ_EncodingSizeSSZ(t *testing.T) {
	size := 5
	vec := NewVectorSSZ[*Checkpoint](size)

	expectedSize := size * CheckpointSizeSSZ // Checkpoint size is 40 bytes
	assert.Equal(t, expectedSize, vec.EncodingSizeSSZ())

	// Verify it matches actual encoded size
	encoded, err := vec.EncodeSSZ(nil)
	require.NoError(t, err)
	assert.Equal(t, expectedSize, len(encoded))
}

// TestVectorSSZ_HashSSZ tests SSZ hashing
func TestVectorSSZ_HashSSZ(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Checkpoint](size)

	// Set some values
	cp1 := &Checkpoint{
		Epoch: 100,
		Root:  common.HexToHash("0x01"),
	}
	cp2 := &Checkpoint{
		Epoch: 200,
		Root:  common.HexToHash("0x02"),
	}
	vec.Set(0, cp1)
	vec.Set(1, cp1)

	// Calculate hash
	hash1, err := vec.HashSSZ()
	require.NoError(t, err)
	assert.NotEqual(t, [32]byte{}, hash1)

	// Hash should be cached
	hash2, err := vec.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2)

	// Modifying vector should invalidate cache
	vec.Set(2, cp2) // Use a different checkpoint
	hash3, err := vec.HashSSZ()
	require.NoError(t, err)
	assert.NotEqual(t, hash1, hash3, "Hash should change when vector content changes")
}

// TestVectorSSZ_HashSSZ_EmptyVector tests hashing an empty vector
func TestVectorSSZ_HashSSZ_EmptyVector(t *testing.T) {
	vec := NewVectorSSZ[*Checkpoint](0)

	_, err := vec.HashSSZ()
	require.NoError(t, err)
}

// TestVectorSSZ_Bytes tests the Bytes method
func TestVectorSSZ_Bytes(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Checkpoint](size)

	cp := &Checkpoint{
		Epoch: 100,
		Root:  common.HexToHash("0x01"),
	}
	vec.Set(0, cp)

	bytes := vec.Bytes()
	assert.NotEmpty(t, bytes)
	assert.Equal(t, vec.EncodingSizeSSZ(), len(bytes))
}

// TestVectorSSZ_Clone tests the Clone operation
func TestVectorSSZ_Clone(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Checkpoint](size)

	cp := &Checkpoint{
		Epoch: 100,
		Root:  common.HexToHash("0x01"),
	}
	vec.Set(0, cp)

	// Clone
	cloned := vec.Clone().(*VectorSSZ[*Checkpoint])

	assert.NotNil(t, cloned)
	assert.Equal(t, vec.Length(), cloned.Length())

	// Cloned vector should have zero values (fresh instance)
	clonedVal := cloned.Get(0)
	assert.Equal(t, uint64(0), clonedVal.Epoch)
}

// TestVectorSSZ_Append_ShouldPanic tests that Append panics
func TestVectorSSZ_Append_ShouldPanic(t *testing.T) {
	vec := NewVectorSSZ[*Checkpoint](3)
	cp := &Checkpoint{}

	assert.Panics(t, func() {
		vec.Append(cp)
	})
}

// TestVectorSSZ_Pop_ShouldPanic tests that Pop panics
func TestVectorSSZ_Pop_ShouldPanic(t *testing.T) {
	vec := NewVectorSSZ[*Checkpoint](3)

	assert.Panics(t, func() {
		vec.Pop()
	})
}

// TestVectorSSZ_DecodeSSZ_InvalidData tests decoding with invalid data
func TestVectorSSZ_DecodeSSZ_InvalidData(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Checkpoint](size)

	// Try to decode with insufficient data
	invalidData := make([]byte, 10) // Too small
	err := vec.DecodeSSZ(invalidData, 0)
	assert.Error(t, err)
}

// TestVectorSSZ_InterfaceCompliance tests that VectorSSZ implements expected interfaces
func TestVectorSSZ_InterfaceCompliance(t *testing.T) {
	vec := NewVectorSSZ[*Checkpoint](3)

	// Test that it implements Vector interface
	var _ Vector[*Checkpoint] = vec

	// Test that it implements IterableSSZ interface
	var _ IterableSSZ[*Checkpoint] = vec
}

// TestVectorSSZ_DynamicElements tests VectorSSZ with dynamic-sized elements (Attestation)
func TestVectorSSZ_DynamicElements(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Attestation](size)

	// Verify it's not static
	assert.False(t, vec.Static())

	// Set attestations with different sizes (different number of validators)
	att1 := newTestAttestation(100, 1, 10)
	att2 := newTestAttestation(200, 2, 20)
	att3 := newTestAttestation(300, 3, 5)

	vec.Set(0, att1)
	vec.Set(1, att2)
	vec.Set(2, att3)

	// Test encoding size (dynamic elements use offsets)
	encodingSize := vec.EncodingSizeSSZ()
	assert.Greater(t, encodingSize, 0)

	// Test encoding and decoding
	encoded, err := vec.EncodeSSZ(nil)
	require.NoError(t, err)
	assert.Equal(t, encodingSize, len(encoded))

	// Decode into a new vector
	vec2 := NewVectorSSZ[*Attestation](size)
	err = vec2.DecodeSSZ(encoded, 0)
	require.NoError(t, err)

	// Verify decoded values
	assert.Equal(t, att1.Data.Slot, vec2.Get(0).Data.Slot)
	assert.Equal(t, att1.Data.CommitteeIndex, vec2.Get(0).Data.CommitteeIndex)
	assert.Equal(t, att2.Data.Slot, vec2.Get(1).Data.Slot)
	assert.Equal(t, att2.Data.CommitteeIndex, vec2.Get(1).Data.CommitteeIndex)
	assert.Equal(t, att3.Data.Slot, vec2.Get(2).Data.Slot)
	assert.Equal(t, att3.Data.CommitteeIndex, vec2.Get(2).Data.CommitteeIndex)

	// Test hashing
	hash1, err := vec.HashSSZ()
	require.NoError(t, err)
	assert.NotEqual(t, [32]byte{}, hash1)

	// Hash should be consistent
	hash2, err := vec.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2)

	// Modify an element and hash should change
	vec.Set(0, newTestAttestation(999, 99, 15))
	hash3, err := vec.HashSSZ()
	require.NoError(t, err)
	assert.NotEqual(t, hash1, hash3)
}

// TestVectorSSZ_DynamicElements_CopyTo tests CopyTo with dynamic elements
func TestVectorSSZ_DynamicElements_CopyTo(t *testing.T) {
	size := 2
	vec1 := NewVectorSSZ[*Attestation](size)
	vec2 := NewVectorSSZ[*Attestation](size)

	att1 := newTestAttestation(100, 1, 10)
	att2 := newTestAttestation(200, 2, 15)

	vec1.Set(0, att1)
	vec1.Set(1, att2)

	// Copy to vec2
	vec1.CopyTo(vec2)

	// Verify copied values
	assert.Equal(t, att1.Data.Slot, vec2.Get(0).Data.Slot)
	assert.Equal(t, att1.Data.CommitteeIndex, vec2.Get(0).Data.CommitteeIndex)
	assert.Equal(t, att2.Data.Slot, vec2.Get(1).Data.Slot)
	assert.Equal(t, att2.Data.CommitteeIndex, vec2.Get(1).Data.CommitteeIndex)
}

// TestVectorSSZ_DynamicElements_Clear tests Clear with dynamic elements
func TestVectorSSZ_DynamicElements_Clear(t *testing.T) {
	size := 2
	vec := NewVectorSSZ[*Attestation](size)

	att := newTestAttestation(100, 1, 10)
	vec.Set(0, att)
	vec.Set(1, att)

	// Clear the vector
	vec.Clear()

	// All items should be reset to freshly cloned empty attestations
	for i := 0; i < size; i++ {
		retrieved := vec.Get(i)
		assert.NotNil(t, retrieved)
		// After Clear, items are freshly cloned (Clone() returns &Attestation{})
		// So Data will be nil for a newly cloned Attestation
		assert.Nil(t, retrieved.Data)
		assert.Nil(t, retrieved.AggregationBits)
	}
}

// TestVectorSSZ_DynamicElements_Range tests Range with dynamic elements
func TestVectorSSZ_DynamicElements_Range(t *testing.T) {
	size := 3
	vec := NewVectorSSZ[*Attestation](size)

	// Set items
	for i := 0; i < size; i++ {
		vec.Set(i, newTestAttestation(uint64(i*100), uint64(i), 10))
	}

	// Range through all values
	count := 0
	vec.Range(func(index int, value *Attestation, length int) bool {
		assert.Equal(t, size, length)
		assert.Equal(t, uint64(index*100), value.Data.Slot)
		assert.Equal(t, uint64(index), value.Data.CommitteeIndex)
		count++
		return true
	})
	assert.Equal(t, size, count)
}

// TestMerkleizeVector_Direct tests MerkleizeVector directly
func TestMerkleizeVector_Direct(t *testing.T) {
	cp1 := &Checkpoint{Epoch: 100, Root: common.HexToHash("0x01")}
	cp2 := &Checkpoint{Epoch: 200, Root: common.HexToHash("0x02")}
	cpZero := &Checkpoint{}

	// Calculate hashes
	h1, _ := cp1.HashSSZ()
	h2, _ := cp2.HashSSZ()
	hZero, _ := cpZero.HashSSZ()

	t.Logf("h1: %x", h1)
	t.Logf("h2: %x", h2)
	t.Logf("hZero: %x", hZero)

	// For proper merkleization, length should be next power of 2
	vectorLength := merkle_tree.NextPowerOfTwo(3) // 3 -> 4

	// Test 1: [h1, h1, hZero]
	leaves1 := [][32]byte{h1, h1, hZero}
	root1, err := merkle_tree.MerkleizeVector(leaves1, vectorLength)
	require.NoError(t, err)
	t.Logf("root1 (with hZero): %x", root1)

	// Test 2: [h1, h1, h2]
	leaves2 := [][32]byte{h1, h1, h2}
	root2, err := merkle_tree.MerkleizeVector(leaves2, vectorLength)
	require.NoError(t, err)
	t.Logf("root2 (with h2): %x", root2)

	// These should be different!
	assert.NotEqual(t, root1, root2, "Roots should be different when leaves are different")
}
