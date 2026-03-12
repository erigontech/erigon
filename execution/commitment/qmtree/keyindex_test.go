// Copyright 2025 The Erigon Authors
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

package qmtree

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func makeKeyHash(b byte) common.Hash {
	var h common.Hash
	h[31] = b
	return h
}

// TestKeyIndex_EmptyRoot verifies that an empty index returns the zero hash.
func TestKeyIndex_EmptyRoot(t *testing.T) {
	ki := NewKeyIndex()
	require.Equal(t, common.Hash{}, ki.Root())
}

// TestKeyIndex_SingleEntry verifies a single-entry index and its inclusion proof.
func TestKeyIndex_SingleEntry(t *testing.T) {
	ki := NewKeyIndex()
	kh := makeKeyHash(0x01)
	ki.UpdateKey(kh, 42)

	root := ki.Root()
	require.NotEqual(t, common.Hash{}, root)

	proof := ki.GetProof(kh)
	require.True(t, proof.Exists)
	require.Equal(t, uint64(42), proof.TxNum)
	require.Equal(t, root, proof.Root)
	require.Equal(t, 1, proof.LeafCount)
	require.True(t, proof.Verify(), "single-entry inclusion proof must verify")
}

// TestKeyIndex_InclusionProof verifies multi-entry inclusion proofs.
func TestKeyIndex_InclusionProof(t *testing.T) {
	ki := NewKeyIndex()
	keys := []common.Hash{makeKeyHash(0x10), makeKeyHash(0x20), makeKeyHash(0x30), makeKeyHash(0x40)}
	for i, kh := range keys {
		ki.UpdateKey(kh, uint64(i+1)*100)
	}

	root := ki.Root()
	for _, kh := range keys {
		proof := ki.GetProof(kh)
		require.True(t, proof.Exists, "key must exist")
		require.Equal(t, root, proof.Root)
		require.True(t, proof.Verify(), "inclusion proof must verify for key %x", kh)
	}
}

// TestKeyIndex_NonMembershipProof verifies that a key between two existing entries
// produces a valid non-membership proof.
func TestKeyIndex_NonMembershipProof(t *testing.T) {
	ki := NewKeyIndex()
	// Insert keys with hashes 0x10 and 0x30. Query for 0x20 (between them).
	ki.UpdateKey(makeKeyHash(0x10), 10)
	ki.UpdateKey(makeKeyHash(0x30), 30)

	root := ki.Root()
	missing := makeKeyHash(0x20)
	proof := ki.GetProof(missing)

	require.False(t, proof.Exists)
	require.Equal(t, root, proof.Root)
	require.NotNil(t, proof.PrevEntry, "prev entry must exist (0x10)")
	require.NotNil(t, proof.NextEntry, "next entry must exist (0x30)")
	require.True(t, proof.Verify(), "non-membership proof must verify")
}

// TestKeyIndex_NonMembershipBeforeAll verifies non-membership for a key smaller
// than all entries.
func TestKeyIndex_NonMembershipBeforeAll(t *testing.T) {
	ki := NewKeyIndex()
	ki.UpdateKey(makeKeyHash(0x50), 50)
	ki.UpdateKey(makeKeyHash(0x60), 60)

	missing := makeKeyHash(0x01) // smaller than all
	proof := ki.GetProof(missing)

	require.False(t, proof.Exists)
	require.Nil(t, proof.PrevEntry, "no prev entry when key is before all")
	require.NotNil(t, proof.NextEntry)
	require.True(t, proof.Verify())
}

// TestKeyIndex_NonMembershipAfterAll verifies non-membership for a key larger
// than all entries.
func TestKeyIndex_NonMembershipAfterAll(t *testing.T) {
	ki := NewKeyIndex()
	ki.UpdateKey(makeKeyHash(0x10), 10)
	ki.UpdateKey(makeKeyHash(0x20), 20)

	var maxKey common.Hash
	for i := range maxKey {
		maxKey[i] = 0xff
	}
	proof := ki.GetProof(maxKey)

	require.False(t, proof.Exists)
	require.NotNil(t, proof.PrevEntry)
	require.Nil(t, proof.NextEntry, "no next entry when key is after all")
	require.True(t, proof.Verify())
}

// TestKeyIndex_UpdateKeepMax verifies that UpdateKey retains the maximum txNum.
func TestKeyIndex_UpdateKeepMax(t *testing.T) {
	ki := NewKeyIndex()
	kh := makeKeyHash(0x01)
	ki.UpdateKey(kh, 10)
	ki.UpdateKey(kh, 50) // higher — should win
	ki.UpdateKey(kh, 30) // lower — should be ignored

	proof := ki.GetProof(kh)
	require.True(t, proof.Exists)
	require.Equal(t, uint64(50), proof.TxNum)
}

// TestKeyIndex_RootDeterministic verifies that the same key set always produces
// the same root regardless of insertion order.
func TestKeyIndex_RootDeterministic(t *testing.T) {
	keys := []common.Hash{makeKeyHash(0xAA), makeKeyHash(0xBB), makeKeyHash(0x11)}

	ki1 := NewKeyIndex()
	for i, kh := range keys {
		ki1.UpdateKey(kh, uint64(i+1))
	}

	ki2 := NewKeyIndex()
	for i := len(keys) - 1; i >= 0; i-- {
		ki2.UpdateKey(keys[i], uint64(i+1))
	}

	require.Equal(t, ki1.Root(), ki2.Root(), "root must be order-independent")
}

// TestKeyIndex_OddLeafCount verifies that a tree with an odd number of leaves
// produces valid proofs for all entries.
func TestKeyIndex_OddLeafCount(t *testing.T) {
	ki := NewKeyIndex()
	for i := byte(1); i <= 5; i++ {
		ki.UpdateKey(makeKeyHash(i*0x10), uint64(i))
	}

	root := ki.Root()
	for i := byte(1); i <= 5; i++ {
		kh := makeKeyHash(i * 0x10)
		proof := ki.GetProof(kh)
		require.True(t, proof.Exists)
		require.Equal(t, root, proof.Root)
		require.Truef(t, proof.Verify(), "odd-leaf inclusion proof must verify for index %d", i)
	}
}

// TestKeyHash_Encoding verifies that KeyHash produces distinct hashes for
// different (domain, key) pairs.
func TestKeyHash_Encoding(t *testing.T) {
	h1 := KeyHash(0x00, []byte{0x01, 0x02})
	h2 := KeyHash(0x01, []byte{0x01, 0x02}) // same key, different domain
	h3 := KeyHash(0x00, []byte{0x01, 0x03}) // same domain, different key

	require.NotEqual(t, h1, h2)
	require.NotEqual(t, h1, h3)
	require.NotEqual(t, h2, h3)
}
