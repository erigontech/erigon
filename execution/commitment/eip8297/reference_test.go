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

package eip8297

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/sha3"

	"github.com/erigontech/erigon/common"
	keccak "github.com/erigontech/fastkeccak"
)

type referenceCorpus struct {
	name    string
	entries []Entry
}

func referenceValue(seed uint64) []byte {
	value := make([]byte, ValueLength)
	for i := range 8 {
		value[i] = 0xA5
	}
	for i := range 8 {
		value[24+i] = byte(seed >> (56 - 8*i))
	}
	return value
}

func referenceAddress(seed uint64) []byte {
	address := make([]byte, 20)
	for i := range 8 {
		address[12+i] = byte(seed >> (56 - 8*i))
	}
	return address
}

func referenceSlot(seed uint64) []byte {
	slot := make([]byte, 32)
	for i := range 8 {
		slot[24+i] = byte(seed >> (56 - 8*i))
	}
	return slot
}

func referenceSharedBits(a, b []byte) int {
	aBits, bBits := BitsFromBytes(a), BitsFromBytes(b)
	shared := 0
	for shared < len(aBits) && shared < len(bBits) && aBits[shared] == bBits[shared] {
		shared++
	}
	return shared
}

func referenceCorpora() []referenceCorpus {
	return []referenceCorpus{
		{name: "empty"},
		{
			name:    "single key",
			entries: []Entry{{Key: TreeKeyAccount(referenceAddress(1), BasicDataLeafKey), Value: referenceValue(1)}},
		},
		{
			name: "split at bit 0",
			entries: func() []Entry {
				address := referenceAddress(2)
				return []Entry{
					{Key: TreeKeyAccount(address, BasicDataLeafKey), Value: referenceValue(1)},
					{Key: TreeKeyStorage(address, referenceSlot(1000)), Value: referenceValue(2)},
				}
			}(),
		},
		{
			name: "split at bit 527",
			entries: func() []Entry {
				address := referenceAddress(3)
				return []Entry{
					{Key: TreeKeyStorage(address, referenceSlot(256)), Value: referenceValue(1)},
					{Key: TreeKeyStorage(address, referenceSlot(257)), Value: referenceValue(2)},
				}
			}(),
		},
		{
			name: "split inside prefix",
			entries: []Entry{
				{Key: referenceSyntheticAccountKey(0x00), Value: referenceValue(1)},
				{Key: referenceSyntheticAccountKey(0x01), Value: referenceValue(2)},
				{Key: referenceSyntheticAccountKey(0x40), Value: referenceValue(3)},
			},
		},
		referenceOneAccountCorpus(),
		referenceDeepSharedPrefixCorpus(),
	}
}

func referenceSyntheticAccountKey(stemByte byte) []byte {
	key := make([]byte, AccountKeyLength)
	key[1] = stemByte
	return key
}

func referenceOneAccountCorpus() referenceCorpus {
	address := referenceAddress(4)
	entries := []Entry{
		{Key: TreeKeyAccount(address, BasicDataLeafKey), Value: referenceValue(1)},
		{Key: TreeKeyAccount(address, CodeHashLeafKey), Value: referenceValue(2)},
	}
	for i, slot := range []uint64{0, 1, 63, 64, 65, 255, 256, 1000} {
		entries = append(entries, Entry{
			Key:   TreeKeyStorage(address, referenceSlot(slot)),
			Value: referenceValue(uint64(10 + i)),
		})
	}
	return referenceCorpus{name: "one account", entries: entries}
}

func referenceDeepSharedPrefixCorpus() referenceCorpus {
	entries := make([]Entry, 0, 4)
	for i, address := range referenceMinedAddresses() {
		entries = append(entries, Entry{
			Key:   TreeKeyAccount(address, BasicDataLeafKey),
			Value: referenceValue(uint64(i)),
		})
	}
	return referenceCorpus{name: "mined deep shared prefix", entries: entries}
}

var referenceMinedAddresses = sync.OnceValue(func() [][]byte {
	const sharedBits = 20
	const count = 4
	const limit = 1 << 24
	var target []byte
	addresses := make([][]byte, 0, count)
	for i := uint64(0); i < limit && len(addresses) < count; i++ {
		address := referenceAddress(i)
		key := TreeKeyAccount(address, BasicDataLeafKey)
		if target == nil {
			target = key
			addresses = append(addresses, address)
			continue
		}
		if referenceSharedBits(target, key) >= sharedBits {
			addresses = append(addresses, address)
		}
	}
	if len(addresses) < count {
		panic("could not mine reference addresses")
	}
	return addresses
})

func referenceRoot(entries []Entry) common.Hash {
	var tree Tree
	for _, entry := range entries {
		tree.Insert(entry.Key, entry.Value)
	}
	return tree.RootHash()
}

func referenceOrderings(entries []Entry) map[string][]Entry {
	byKeyAsc := slices.Clone(entries)
	slices.SortFunc(byKeyAsc, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
	byKeyDesc := slices.Clone(byKeyAsc)
	slices.Reverse(byKeyDesc)
	reversed := slices.Clone(entries)
	slices.Reverse(reversed)
	shuffled := slices.Clone(entries)
	rnd := rand.New(rand.NewSource(0x8297))
	rnd.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
	return map[string][]Entry{
		"reversed":       reversed,
		"key ascending":  byKeyAsc,
		"key descending": byKeyDesc,
		"shuffled":       shuffled,
	}
}

func TestReferenceEncodeBitPrefix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		prefix []byte
		want   string
	}{
		{name: "empty prefix", want: "0000"},
		{name: "one zero bit", prefix: []byte{0}, want: "000100"},
		{name: "one set bit", prefix: []byte{1}, want: "000180"},
		{name: "three bits", prefix: []byte{1, 0, 1}, want: "0003a0"},
		{name: "seven bits", prefix: []byte{1, 1, 1, 1, 1, 1, 1}, want: "0007fe"},
		{name: "full byte", prefix: []byte{1, 0, 1, 0, 1, 0, 1, 0}, want: "0008aa"},
		{name: "nine bits", prefix: []byte{1, 0, 1, 0, 1, 0, 1, 0, 1}, want: "0009aa80"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, hex.EncodeToString(EncodeBitPrefix(tc.prefix)))
		})
	}
}

func TestReferenceEncodeBitPrefixLongRun(t *testing.T) {
	prefix := bytes.Repeat([]byte{1}, MaxPathBits)
	got := EncodeBitPrefix(prefix)
	require.Len(t, got, 2+66)
	require.Equal(t, []byte{0x02, 0x10}, got[:2])
	require.Equal(t, bytes.Repeat([]byte{0xFF}, 66), got[2:])
}

func TestReferenceEmptyTreeHash(t *testing.T) {
	var tree Tree
	require.Equal(t, common.Hash{}, tree.RootHash())
}

func TestReferenceSingleKeyRootIsLeafHash(t *testing.T) {
	entry := referenceCorpora()[1].entries[0]
	var tree Tree
	tree.Insert(entry.Key, entry.Value)
	require.IsType(t, &Leaf{}, tree.Root)
	preimage := append([]byte{0x00}, entry.Key...)
	preimage = append(preimage, entry.Value...)
	require.Equal(t, handKeccak(preimage), tree.RootHash())
}

func TestReferenceTwoKeyRootIsBranchHash(t *testing.T) {
	entries := referenceCorpora()[2].entries
	var tree Tree
	for _, entry := range entries {
		tree.Insert(entry.Key, entry.Value)
	}
	branch, ok := tree.Root.(*Branch)
	require.True(t, ok)
	require.Empty(t, branch.Prefix)
	left, right := treeHash(branch.Left), treeHash(branch.Right)
	preimage := []byte{0x01, 0, 0}
	preimage = append(preimage, left[:]...)
	preimage = append(preimage, right[:]...)
	require.Equal(t, handKeccak(preimage), tree.RootHash())
}

func TestReferenceMerkelizeIndependentBranchOrder(t *testing.T) {
	entries := []Entry{
		{Key: []byte{0x00}, Value: referenceValue(1)},
		{Key: []byte{0x80}, Value: referenceValue(2)},
	}
	left := handKeccak(append(append([]byte{0x00}, entries[0].Key...), entries[0].Value...))
	right := handKeccak(append(append([]byte{0x00}, entries[1].Key...), entries[1].Value...))
	preimage := []byte{0x01, 0, 0}
	preimage = append(preimage, left[:]...)
	preimage = append(preimage, right[:]...)

	require.Equal(t, handKeccak(preimage), StateRoot(entries))
}

func TestReferenceBranchPreimageUsesHandAssembledBytes(t *testing.T) {
	left := common.Hash{1}
	right := common.Hash{2}
	want := []byte{0x01, 0, 3, 0xa0}
	want = append(want, left[:]...)
	want = append(want, right[:]...)
	prefix := Bitpath{BitLen: 3}
	prefix.SetBitAt(0, 1)
	prefix.SetBitAt(1, 0)
	prefix.SetBitAt(2, 1)
	require.Equal(t, want, BranchPreimage(nil, &prefix, &left, &right))
}

func TestReferenceMerkelizeSupportsVariableLengthPrefixes(t *testing.T) {
	keyA := make([]byte, 67)
	keyB := make([]byte, 67)
	keyB[len(keyB)-1] = 1
	var got common.Hash
	require.NotPanics(t, func() {
		got = StateRoot([]Entry{{Key: keyA, Value: referenceValue(1)}, {Key: keyB, Value: referenceValue(2)}})
	})
	require.NotEqual(t, common.Hash{}, got)
}

func handKeccak(preimage []byte) common.Hash {
	h := sha3.NewLegacyKeccak256()
	_, _ = h.Write(preimage)
	return common.BytesToHash(h.Sum(nil))
}

func TestEmbedStateRemovalsRunBeforeBatchUpdates(t *testing.T) {
	address := referenceAddress(9)
	slot := referenceSlot(64)
	value := EncodeStorageValue([]byte{0x42})
	entries := EmbedState([][]State{
		{{Address: address, Nonce: 1}},
		{
			{Address: address, Slots: map[string][]byte{string(slot): {0x42}}},
			{Address: address, Deleted: true},
		},
	})
	want := []Entry{{Key: TreeKeyStorage(address, slot), Value: value[:]}}
	require.Equal(t, want, entries)
	require.Equal(t, StateRoot(want), StateRoot(entries))
}

func TestEmbedStateLastAccountDeletionWins(t *testing.T) {
	address := referenceAddress(10)
	entries := EmbedState([][]State{{
		{Address: address, Nonce: 1},
		{Address: address, Deleted: true},
	}})
	require.Empty(t, entries)
	require.Equal(t, common.Hash{}, StateRoot(entries))
}

func TestEmbedStateAccountRewriteKeepsEarlierSlots(t *testing.T) {
	address := referenceAddress(11)
	slot := referenceSlot(65)
	encoded := EncodeStorageValue([]byte{0x42})
	wantSlot := Entry{Key: TreeKeyStorage(address, slot), Value: encoded[:]}
	entries := EmbedState([][]State{
		{{Address: address, Nonce: 1, Slots: map[string][]byte{string(slot): {0x42}}}},
		{
			{Address: address, Deleted: true},
			{Address: address, Nonce: 2},
		},
	})
	found := false
	for _, entry := range entries {
		if bytes.Equal(entry.Key, wantSlot.Key) {
			found = true
			require.Equal(t, wantSlot.Value, entry.Value)
		}
	}
	require.True(t, found)
}

func TestEmbedStateStorageOnlyBatchKeepsAccount(t *testing.T) {
	address := referenceAddress(12)
	slot := referenceSlot(64)
	basic, err := EncodeBasicData(7, new(uint256.Int), 0)
	require.NoError(t, err)
	storage := EncodeStorageValue([]byte{0x42})
	entries := EmbedState([][]State{
		{{Address: address, Nonce: 7}},
		{{Address: address, Slots: map[string][]byte{string(slot): {0x42}}}},
	})
	require.Contains(t, entries, Entry{Key: TreeKeyAccount(address, BasicDataLeafKey), Value: basic[:]})
	require.Contains(t, entries, Entry{Key: TreeKeyStorage(address, slot), Value: storage[:]})
}

func TestEmbedStateStorageOnlyUpdateKeepsPendingAccount(t *testing.T) {
	address := referenceAddress(14)
	code := []byte{0x60, 0x01}
	basic, err := EncodeBasicData(7, uint256.NewInt(9), uint64(len(code)))
	require.NoError(t, err)
	codeHash := CodeHashValue(common.Hash(keccak.Sum256(code)))
	chunks := ChunkifyCode(code)
	storage := EncodeStorageValue([]byte{0x42})
	entries := EmbedState([][]State{{
		{Address: address, Nonce: 7, Balance: *uint256.NewInt(9), Code: code},
		{Address: address, Slots: map[string][]byte{string(referenceSlot(64)): {0x42}}},
	}})
	want := []Entry{
		{Key: TreeKeyAccount(address, BasicDataLeafKey), Value: basic[:]},
		{Key: TreeKeyAccount(address, CodeHashLeafKey), Value: codeHash[:]},
		{Key: TreeKeyCodeChunk(common.Hash(keccak.Sum256(code)), 0), Value: chunks[0][:]},
		{Key: TreeKeyStorage(address, referenceSlot(64)), Value: storage[:]},
	}
	require.Equal(t, want, entries)
}

func TestEmbedStateRandomPendingAccountUpdates(t *testing.T) {
	for sequence := range 200 {
		rnd := rand.New(rand.NewSource(int64(sequence + 1)))
		states := make([]State, 0, 20)
		want := make(map[string]Entry)
		for accountIndex := byte(1); accountIndex <= 4; accountIndex++ {
			address := referenceAddress(uint64(accountIndex))
			code := []byte{0x60, accountIndex}
			balance := *uint256.NewInt(uint64(rnd.Intn(100)))
			state := State{Address: address, Nonce: uint64(rnd.Intn(10)), Balance: balance, Code: code}
			states = append(states, state)
			basic, err := EncodeBasicData(state.Nonce, &state.Balance, uint64(len(code)))
			require.NoError(t, err)
			want[string(TreeKeyAccount(address, BasicDataLeafKey))] = Entry{Key: TreeKeyAccount(address, BasicDataLeafKey), Value: basic[:]}
			codeHash := common.Hash(keccak.Sum256(code))
			codeHashValue := CodeHashValue(codeHash)
			want[string(TreeKeyAccount(address, CodeHashLeafKey))] = Entry{Key: TreeKeyAccount(address, CodeHashLeafKey), Value: codeHashValue[:]}
			chunks := ChunkifyCode(code)
			chunk := chunks[0]
			want[string(TreeKeyCodeChunk(codeHash, 0))] = Entry{Key: TreeKeyCodeChunk(codeHash, 0), Value: chunk[:]}
		}
		for range rnd.Intn(16) + 1 {
			accountIndex := byte(rnd.Intn(4) + 1)
			address := referenceAddress(uint64(accountIndex))
			slot := referenceSlot(uint64([]uint64{0, 64, 256, 257}[rnd.Intn(4)]))
			value := byte(rnd.Intn(4))
			states = append(states, State{Address: address, Slots: map[string][]byte{string(slot): {value}}})
			key := TreeKeyStorage(address, slot)
			if value == 0 {
				delete(want, string(key))
				continue
			}
			encoded := EncodeStorageValue([]byte{value})
			want[string(key)] = Entry{Key: key, Value: encoded[:]}
		}
		got := EmbedState([][]State{states})
		wantEntries := make([]Entry, 0, len(want))
		for _, entry := range want {
			wantEntries = append(wantEntries, entry)
		}
		slices.SortFunc(wantEntries, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
		slices.SortFunc(got, func(a, b Entry) int { return bytes.Compare(a.Key, b.Key) })
		require.Equal(t, wantEntries, got, "sequence %d", sequence)
	}
}

func TestEmbedStateStorageOnlyFirstBatchCreatesCodelessAccount(t *testing.T) {
	address := referenceAddress(13)
	slot := referenceSlot(64)
	zero := common.Hash{}
	emptyCodeHash := CodeHashValue(zero)
	entries := EmbedState([][]State{{{Address: address, Slots: map[string][]byte{string(slot): {0x42}}}}})
	require.NotContains(t, entries, Entry{Key: TreeKeyAccount(address, BasicDataLeafKey)})
	require.Contains(t, entries, Entry{Key: TreeKeyAccount(address, CodeHashLeafKey), Value: emptyCodeHash[:]})
}

func TestReferenceDelegationHelpersUseSpecBytes(t *testing.T) {
	code := append([]byte{0xEF, 0x01, 0x00}, bytes.Repeat([]byte{0xAB}, 20)...)
	require.True(t, IsDelegation(code))
	require.False(t, IsDelegation(code[:22]))
	got := EncodeDelegation(code)
	want := [ValueLength]byte{}
	copy(want[:], []byte{0xEF, 0x01, 0x00})
	copy(want[3:], bytes.Repeat([]byte{0xAB}, 20))
	require.Equal(t, want, got)
}

func treeHash(node Node) common.Hash {
	return (&Tree{Root: node}).RootHash()
}

func TestReferenceSplitAtLastBit(t *testing.T) {
	entries := referenceCorpora()[3].entries
	require.Equal(t, MaxPathBits-1, referenceSharedBits(entries[0].Key, entries[1].Key))
	var tree Tree
	for _, entry := range entries {
		tree.Insert(entry.Key, entry.Value)
	}
	branch, ok := tree.Root.(*Branch)
	require.True(t, ok)
	require.Len(t, branch.Prefix, MaxPathBits-1)
}

func TestReferenceSplitInsidePrefix(t *testing.T) {
	entries := referenceCorpora()[4].entries
	var pair Tree
	pair.Insert(entries[0].Key, entries[0].Value)
	pair.Insert(entries[1].Key, entries[1].Value)
	pairRoot, ok := pair.Root.(*Branch)
	require.True(t, ok)
	require.Len(t, pairRoot.Prefix, 15)

	var tree Tree
	for _, entry := range entries {
		tree.Insert(entry.Key, entry.Value)
	}
	root, ok := tree.Root.(*Branch)
	require.True(t, ok)
	require.Len(t, root.Prefix, 9)
	survivor, ok := root.Left.(*Branch)
	require.True(t, ok)
	require.Len(t, survivor.Prefix, 5)
	require.Equal(t, pairRoot.Prefix[10:], survivor.Prefix)
	require.IsType(t, &Leaf{}, root.Right)
}

func TestReferenceDuplicateKeyUpdatesValue(t *testing.T) {
	entries := referenceCorpora()[2].entries
	updated := referenceValue(0xDEAD)
	var tree Tree
	for _, entry := range entries {
		tree.Insert(entry.Key, entry.Value)
	}
	tree.Insert(entries[0].Key, updated)
	var want Tree
	want.Insert(entries[0].Key, updated)
	want.Insert(entries[1].Key, entries[1].Value)
	require.Equal(t, want.RootHash(), tree.RootHash())
	require.NotEqual(t, referenceRoot(entries), tree.RootHash())
}

func TestReferenceRejectsInvalidInsert(t *testing.T) {
	key := referenceCorpora()[1].entries[0].Key
	for _, tc := range []struct {
		name string
		call func(*Tree)
	}{
		{name: "value length", call: func(tree *Tree) { tree.Insert(key, make([]byte, ValueLength-1)) }},
		{name: "empty key", call: func(tree *Tree) { tree.Insert(nil, referenceValue(0)) }},
		{name: "key length", call: func(tree *Tree) { tree.Insert(make([]byte, maxReferenceKeyLength+1), referenceValue(0)) }},
		{name: "key prefix", call: func(tree *Tree) { tree.Insert(key[:8], referenceValue(1)) }},
		{name: "key extension", call: func(tree *Tree) { tree.Insert(key, referenceValue(1)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tree Tree
			if tc.name == "key prefix" {
				tree.Insert(key, referenceValue(0))
			} else if tc.name == "key extension" {
				tree.Insert(key[:8], referenceValue(0))
			}
			require.Panics(t, func() { tc.call(&tree) })
		})
	}
}

func TestReferenceCorporaArePrefixFree(t *testing.T) {
	for _, corpus := range referenceCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			for i, a := range corpus.entries {
				require.Contains(t, []int{AccountKeyLength, StorageKeyLength}, len(a.Key), "key %d", i)
				require.Len(t, a.Value, ValueLength)
				for j, b := range corpus.entries {
					if i != j {
						require.False(t, bytes.HasPrefix(b.Key, a.Key), "keys %d and %d", i, j)
					}
				}
			}
		})
	}
}

func TestReferencePermutationIndependence(t *testing.T) {
	for _, corpus := range referenceCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			want := referenceRoot(corpus.entries)
			for name, order := range referenceOrderings(corpus.entries) {
				require.Equal(t, want, referenceRoot(order), name)
			}
		})
	}
}

func TestReferenceDeepSharedPrefixCorpus(t *testing.T) {
	corpus := referenceDeepSharedPrefixCorpus()
	require.GreaterOrEqual(t, len(corpus.entries), 4)
	for _, entry := range corpus.entries[1:] {
		require.GreaterOrEqual(t, referenceSharedBits(corpus.entries[0].Key, entry.Key), 20)
	}
	var tree Tree
	for _, entry := range corpus.entries {
		tree.Insert(entry.Key, entry.Value)
	}
	root, ok := tree.Root.(*Branch)
	require.True(t, ok)
	require.GreaterOrEqual(t, len(root.Prefix), 19)
}

func TestReferenceStemSharedCorpus(t *testing.T) {
	corpus := referenceOneAccountCorpus()
	var storage [][]byte
	for _, entry := range corpus.entries {
		if len(entry.Key) == StorageKeyLength {
			storage = append(storage, entry.Key)
		}
	}
	require.GreaterOrEqual(t, len(storage), 2)
	for _, key := range storage[1:] {
		require.GreaterOrEqual(t, referenceSharedBits(storage[0], key), 8+256)
	}
}
