// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package trie

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestHexCompact(t *testing.T) {
	tests := []struct{ hex, compact []byte }{
		// empty keys, with and without terminator.
		{hex: []byte{}, compact: []byte{0x00}},
		{hex: []byte{16}, compact: []byte{0x20}},
		// odd length, no terminator
		{hex: []byte{1, 2, 3, 4, 5}, compact: []byte{0x11, 0x23, 0x45}},
		// even length, no terminator
		{hex: []byte{0, 1, 2, 3, 4, 5}, compact: []byte{0x00, 0x01, 0x23, 0x45}},
		// odd length, terminator
		{hex: []byte{15, 1, 12, 11, 8, 16 /*term*/}, compact: []byte{0x3f, 0x1c, 0xb8}},
		// even length, terminator
		{hex: []byte{0, 15, 1, 12, 11, 8, 16 /*term*/}, compact: []byte{0x20, 0x0f, 0x1c, 0xb8}},
	}
	for _, test := range tests {
		if c := hexToCompact(test.hex); !bytes.Equal(c, test.compact) {
			t.Errorf("hexToCompact(%x) -> %x, want %x", test.hex, c, test.compact)
		}
		if h := compactToHex(test.compact); !bytes.Equal(h, test.hex) {
			t.Errorf("compactToHex(%x) -> %x, want %x", test.compact, h, test.hex)
		}
	}
}

func TestHexKeybytes(t *testing.T) {
	tests := []struct{ key, hexIn, hexOut []byte }{
		{key: []byte{}, hexIn: []byte{16}, hexOut: []byte{16}},
		{key: []byte{}, hexIn: []byte{}, hexOut: []byte{16}},
		{
			key:    []byte{0x12, 0x34, 0x56},
			hexIn:  []byte{1, 2, 3, 4, 5, 6, 16},
			hexOut: []byte{1, 2, 3, 4, 5, 6, 16},
		},
		{
			key:    []byte{0x12, 0x34, 0x5},
			hexIn:  []byte{1, 2, 3, 4, 0, 5, 16},
			hexOut: []byte{1, 2, 3, 4, 0, 5, 16},
		},
		{
			key:    []byte{0x12, 0x34, 0x56},
			hexIn:  []byte{1, 2, 3, 4, 5, 6},
			hexOut: []byte{1, 2, 3, 4, 5, 6, 16},
		},
	}
	for _, test := range tests {
		if h := KeybytesToHex(test.key); !bytes.Equal(h, test.hexOut) {
			t.Errorf("keybytesToHex(%x) -> %x, want %x", test.key, h, test.hexOut)
		}
		if k := hexToKeybytes(test.hexIn); !bytes.Equal(k, test.key) {
			t.Errorf("hexToKeybytes(%x) -> %x, want %x", test.hexIn, k, test.key)
		}
	}
}

func TestKeybytesToCompact(t *testing.T) {
	keybytes := Keybytes{common.FromHex("5a70"), true, true}
	compact := keybytes.ToCompact()
	assert.Equal(t, common.FromHex("35a7"), compact)

	keybytes = Keybytes{common.FromHex("5a70"), true, false}
	compact = keybytes.ToCompact()
	assert.Equal(t, common.FromHex("15a7"), compact)

	keybytes = Keybytes{common.FromHex("5a7c"), false, true}
	compact = keybytes.ToCompact()
	assert.Equal(t, common.FromHex("205a7c"), compact)

	keybytes = Keybytes{common.FromHex("5a7c"), false, false}
	compact = keybytes.ToCompact()
	assert.Equal(t, common.FromHex("005a7c"), compact)
}

func TestCompactToKeybytes(t *testing.T) {
	compact := common.FromHex("35a7")
	keybytes := CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a70"), true, true}, keybytes)

	compact = common.FromHex("15a7")
	keybytes = CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a70"), true, false}, keybytes)

	compact = common.FromHex("205a7c")
	keybytes = CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a7c"), false, true}, keybytes)

	compact = common.FromHex("005a7c")
	keybytes = CompactToKeybytes(compact)
	assert.Equal(t, Keybytes{common.FromHex("5a7c"), false, false}, keybytes)
}

func BenchmarkHexToCompact(b *testing.B) {
	testBytes := []byte{0, 15, 1, 12, 11, 8, 16 /*term*/}
	for b.Loop() {
		hexToCompact(testBytes)
	}
}

func BenchmarkCompactToHex(b *testing.B) {
	testBytes := []byte{0, 15, 1, 12, 11, 8, 16 /*term*/}
	for b.Loop() {
		compactToHex(testBytes)
	}
}

func BenchmarkKeybytesToHex(b *testing.B) {
	testBytes := []byte{7, 6, 6, 5, 7, 2, 6, 2, 16}
	for b.Loop() {
		KeybytesToHex(testBytes)
	}
}

func BenchmarkHexToKeybytes(b *testing.B) {
	testBytes := []byte{7, 6, 6, 5, 7, 2, 6, 2, 16}
	for b.Loop() {
		hexToKeybytes(testBytes)
	}
}

func TestRLPEncodeDecodeWithAccountsAndStorage(t *testing.T) {
	// This test creates a single trie containing accounts with embedded storage subtries.
	// - Accounts are created using UpdateAccount (creates AccountNode entries)
	// - Storage is added using Update with composite keys (addressHash + storageKeyHash)
	// - Storage gets embedded into AccountNode.Storage subtries
	//
	// Note: This test verifies that encoding captures all nodes (accounts + storage),
	// and that the storage subtrie hashes match. Full roundtrip decode with AccountNode
	// reconstruction is not yet implemented.

	stateTrie := newEmpty()

	// Define test addresses
	addresses := []common.Address{
		common.HexToAddress("0x1111111111111111111111111111111111111111"), // EOA
		common.HexToAddress("0x2222222222222222222222222222222222222222"), // Contract with storage
		common.HexToAddress("0x3333333333333333333333333333333333333333"), // EOA
		common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"), // Contract with storage
	}

	// Hash the addresses for trie keys
	addrHashes := make([]common.Hash, len(addresses))
	for i, addr := range addresses {
		addrHashes[i] = crypto.Keccak256Hash(addr.Bytes())
	}

	// Create accounts
	testAccounts := []*accounts.Account{
		{
			Nonce:    1,
			Balance:  *uint256.NewInt(1000000000000000000), // 1 ETH
			Root:     EmptyRoot,
			CodeHash: accounts.EmptyCodeHash,
		},
		{
			Nonce:    42,
			Balance:  *uint256.NewInt(10000000000000000000), // 10 ETH
			Root:     EmptyRoot,                             // Will be updated when storage is added
			CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0x60, 0x80, 0x60, 0x40})),
		},
		{
			Nonce:    0,
			Balance:  *uint256.NewInt(0),
			Root:     EmptyRoot,
			CodeHash: accounts.EmptyCodeHash,
		},
		{
			Nonce:    999,
			Balance:  *uint256.NewInt(0xffffffffffffffff),
			Root:     EmptyRoot,
			CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash([]byte("contract code"))),
		},
	}

	// Insert accounts using UpdateAccount (creates AccountNode entries)
	for i, addr := range addresses {
		key := crypto.Keccak256(addr.Bytes())
		stateTrie.UpdateAccount(key, testAccounts[i])
	}

	// Define storage for contract at index 1
	contract1AddrHash := addrHashes[1]
	storageSlots1 := []struct {
		slot  common.Hash
		value []byte
	}{
		{common.HexToHash("0x0"), common.HexToHash("0x1").Bytes()},
		{common.HexToHash("0x1"), common.HexToHash("0xdeadbeef").Bytes()},
		{common.HexToHash("0x2"), common.HexToHash("0x1234567890abcdef").Bytes()},
		{common.HexToHash("0x100"), common.HexToHash("0xff").Bytes()},
	}

	// Insert storage using composite keys: addressHash + keccak256(slot)
	// This inserts into AccountNode.Storage
	for _, slot := range storageSlots1 {
		compositeKey := make([]byte, 64)
		copy(compositeKey[:32], contract1AddrHash[:])
		copy(compositeKey[32:], crypto.Keccak256(slot.slot.Bytes()))
		stateTrie.Update(compositeKey, slot.value)
	}

	// Define storage for contract at index 3
	contract2AddrHash := addrHashes[3]
	storageSlots2 := []struct {
		slot  common.Hash
		value []byte
	}{
		{common.HexToHash("0x0"), common.HexToHash("0xabcd").Bytes()},
		{common.HexToHash("0x5"), common.HexToHash("0x9999").Bytes()},
	}

	for _, slot := range storageSlots2 {
		compositeKey := make([]byte, 64)
		copy(compositeKey[:32], contract2AddrHash[:])
		copy(compositeKey[32:], crypto.Keccak256(slot.slot.Bytes()))
		stateTrie.Update(compositeKey, slot.value)
	}

	// Get the storage root hashes via DeepHash
	_, storageRoot1 := stateTrie.DeepHash(contract1AddrHash[:])
	_, storageRoot2 := stateTrie.DeepHash(contract2AddrHash[:])

	// Update expected accounts with computed storage roots
	// (storage was added via Update, so the trie's AccountNode.Root is updated)
	testAccounts[1].Root = storageRoot1
	testAccounts[3].Root = storageRoot2

	// Compute original state root BEFORE encoding
	originalStateRoot := stateTrie.Hash()

	// Encode the unified trie (includes accounts AND their storage subtries)
	encoded, err := stateTrie.RLPEncode()
	require.NoError(t, err)
	require.NotEmpty(t, encoded)

	t.Logf("Unified state trie: %d nodes, %d bytes", len(encoded), totalSize(encoded))
	t.Logf("Contains %d accounts (%d with storage), %d total storage slots",
		len(addresses), 2, len(storageSlots1)+len(storageSlots2))
	t.Logf("Original state root: %s", originalStateRoot.Hex())
	t.Logf("Storage root 1: %s", storageRoot1.Hex())
	t.Logf("Storage root 2: %s", storageRoot2.Hex())

	// Decode the unified trie back
	decodedStateTrie, err := RLPDecode(encoded)
	require.NoError(t, err)

	// Verify the decoded trie hash matches the original
	decodedStateRoot := decodedStateTrie.Hash()
	assert.Equal(t, originalStateRoot, decodedStateRoot, "decoded state trie hash should match original")

	// Verify that encoding captured all expected nodes:
	// - Account trie nodes
	// - Storage subtrie nodes for both contracts
	assert.GreaterOrEqual(t, len(encoded), 10, "should have multiple nodes for accounts + storage")

	for i, addr := range addresses {
		key := crypto.Keccak256(addr.Bytes())
		acc, ok := decodedStateTrie.GetAccount(key)
		require.True(t, ok)
		require.EqualValues(t, testAccounts[i], acc)
	}

}

func totalSize(nodes [][]byte) int {
	size := 0
	for _, n := range nodes {
		size += len(n)
	}
	return size
}
