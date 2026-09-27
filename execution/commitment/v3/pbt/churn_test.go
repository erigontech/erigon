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

package pbt

import (
	"bytes"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestTrieRandomizedChurn(t *testing.T) {
	previous := eip8297.HashSuiteName()
	t.Cleanup(func() { require.NoError(t, eip8297.SetHashSuite(previous)) })
	seeds := []int64{0x6a09e667f3bcc909, 0x3c6ef372fe94f82b}
	for _, suite := range []string{eip8297.HashKeccak, eip8297.HashBlake3} {
		require.NoError(t, eip8297.SetHashSuite(suite))
		for _, seed := range seeds {
			t.Run(suite+"/"+formatChurnSeed(seed), func(t *testing.T) {
				runTrieChurn(t, seed)
			})
		}
	}
}

func TestTrieChurnWindowCoverage(t *testing.T) {
	address := bytes.Repeat([]byte{0xb1}, 20)
	prefix := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
	for bit := 264; bit <= 271; bit++ {
		t.Run("storage-bit-"+formatChurnSeed(int64(bit)), func(t *testing.T) {
			checkStorageSplit(t, prefix, bit)
		})
	}
	for window := 66; window <= 131; window++ {
		bit := window*4 + 1
		t.Run("storage-window-"+formatChurnSeed(int64(window)), func(t *testing.T) {
			checkStorageSplit(t, prefix, bit)
		})
	}
	for window := 2; window <= 67; window++ {
		bit := window*4 + 1
		t.Run("code-window-"+formatChurnSeed(int64(window)), func(t *testing.T) {
			checkCodeSplit(t, bit)
		})
	}
	ctx := newTrieTestContext()
	account := accountKey(0, eip8297.BasicDataLeafKey)
	code := trieCodeKey(0, 0, 1)
	_, err := NewTrie(ctx).Process([]Op{{Key: account, Value: testTrieValue(1)}, {Key: code, Value: testTrieValue(2)}})
	require.NoError(t, err)
}

func checkStorageSplit(t *testing.T, prefix []byte, bit int) {
	t.Helper()
	keyA := storageKeyWithSuffix(prefix, 0, 0)
	keyB := bytes.Clone(keyA)
	keyB[bit/8] ^= 1 << uint(7-bit%8)
	ctx := newTrieTestContext()
	ops := []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	_, err := NewTrie(ctx).Process(ops)
	require.NoError(t, err)
	bucketKey, err := bucketKeyForStorage(keyA)
	require.NoError(t, err)
	record, err := DecodeRecord(bucketKey, ctx.records[string(bucketKey)])
	require.NoError(t, err)
	if bit < 268 {
		require.Equal(t, RowRoot, record.Form)
		return
	}
	require.Equal(t, ExtRoot, record.Form)
	require.Equal(t, int16(bit-264), record.SelfExt.BitLen)
}

func checkCodeSplit(t *testing.T, bit int) {
	t.Helper()
	keyA := trieCodeKey(0, 0, 1)
	keyB := bytes.Clone(keyA)
	keyB[bit/8] ^= 1 << uint(7-bit%8)
	ctx := newTrieTestContext()
	ops := []Op{{Key: keyA, Value: testTrieValue(1)}, {Key: keyB, Value: testTrieValue(2)}}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(ops[i].Key, ops[j].Key) < 0 })
	_, err := NewTrie(ctx).Process(ops)
	require.NoError(t, err)
	require.NoError(t, NewTrie(ctx).Verify())
}

func runTrieChurn(t *testing.T, seed int64) {
	t.Helper()
	batches := 5000
	if testing.Short() {
		batches = 100
	}
	rng := rand.New(rand.NewSource(seed))
	keys, prefixes := churnKeys()
	mergeBatches, mergeFinal := mergeChurnBatches()
	state := make(map[string]Op)
	ctx := newTrieTestContext()
	forms := make(map[RootForm]bool)
	sawEmpty := false
	for batch := range batches {
		var ops []Op
		switch batch {
		case 0:
			ops = []Op{{Key: trieCodeKey(0, 0, 1), Value: testTrieValue(1)}}
		case 1:
			address := bytes.Repeat([]byte{0xa0}, 20)
			ops = []Op{{Key: eip8297.TreeKeyStorage(address, storageSlot(64)), Value: testTrieValue(2)}}
		case 2:
			address := bytes.Repeat([]byte{0xa0}, 20)
			ops = []Op{{Key: eip8297.TreeKeyStorage(address, storageSlot(64))}}
		case 3:
			ops = []Op{{Key: trieCodeKey(0x10, 0, 2), Value: testTrieValue(3)}}
		case 4:
			ops = []Op{{Key: trieCodeKey(0, 0, 1)}, {Key: trieCodeKey(0x10, 0, 2)}}
		case 5, 6:
			ops = mergeBatches[batch-5]
		default:
			ops = churnBatch(rng, keys, prefixes, state)
		}
		start := cloneRecords(ctx.records)
		trie := NewTrie(ctx)
		root, err := trie.Process(ops)
		if err != nil {
			t.Fatalf("seed=%d batch=%d process: %v ops=%x", seed, batch, err, churnOpKeys(ops))
		}
		switch batch {
		case 5:
			updateChurnState(state, ops)
		case 6:
			for _, entry := range mergeFinal {
				state[string(entry.Key)] = entry
			}
		default:
			updateChurnState(state, ops)
		}
		entries := churnEntries(state)
		want := eip8297.StateRootWithHash(entriesFromOps(entries), eip8297.SelectedHash())
		if root != want {
			t.Fatalf("seed=%d batch=%d root mismatch: got %x want %x ops=%x", seed, batch, root, want, churnOpKeys(ops))
		}
		if batch%31 == 0 {
			deltas := trie.TakeDeltas()
			for _, delta := range deltas {
				require.Equal(t, start[string(delta.Key)], delta.Prev, "seed=%d batch=%d delta=%x", seed, batch, delta.Key)
			}
			for _, delta := range slices.Backward(deltas) {
				require.NoError(t, ctx.PutBranch(delta.Key, delta.Prev, delta.Data))
			}
			require.Equal(t, start, ctx.records)
			root, err = NewTrie(ctx).Process(ops)
			if err != nil {
				t.Fatalf("seed=%d batch=%d replay: %v", seed, batch, err)
			}
			require.Equal(t, want, root, "seed=%d batch=%d replay root", seed, batch)
		}
		data := ctx.records[string(GlobalRootKey())]
		if len(data) == 0 {
			sawEmpty = true
		} else {
			record, err := DecodeRecord(GlobalRootKey(), data)
			require.NoError(t, err)
			forms[record.Form] = true
		}
		assertPersistedTrie(t, ctx, entries)
	}
	require.True(t, sawEmpty, "seed=%d did not reach an empty root", seed)
	for _, form := range []RootForm{LeafRoot, RowRoot, ExtRoot} {
		require.True(t, forms[form], "seed=%d did not reach root form %d", seed, form)
	}
}

func churnKeys() ([][]byte, [][]byte) {
	keys := make([][]byte, 0, 300)
	seen := make(map[string]struct{})
	appendKey := func(key []byte) {
		if _, ok := seen[string(key)]; ok {
			return
		}
		seen[string(key)] = struct{}{}
		keys = append(keys, bytes.Clone(key))
	}
	appendPair := func(key []byte, bit int) {
		other := bytes.Clone(key)
		other[bit/8] ^= 1 << uint(7-bit%8)
		appendKey(key)
		appendKey(other)
	}
	for stemNibble := range 4 {
		key := make([]byte, eip8297.AccountKeyLength)
		key[0] = eip8297.AccountZone
		key[len(key)-2] = byte(stemNibble)
		key[len(key)-1] = eip8297.BasicDataLeafKey
		appendKey(key)
	}
	accountBase := eip8297.TreeKeyAccount(bytes.Repeat([]byte{0x45}, 20), eip8297.BasicDataLeafKey)
	for window := 2; window <= 67; window++ {
		appendPair(accountBase, window*4+1)
	}
	prefixes := make([][]byte, 0, 2)
	for addressIndex := range 2 {
		address := bytes.Repeat([]byte{0xa0 + byte(addressIndex)}, 20)
		stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
		prefixes = append(prefixes, bytes.Clone(stem))
		for _, first := range []byte{0x20, 0x40, 0xc6} {
			for last := range 8 {
				appendKey(storageKeyWithSuffix(stem, first, byte(last)))
			}
		}
		base := eip8297.TreeKeyStorage(address, storageSlot(64))
		for bit := 264; bit <= 271; bit++ {
			appendPair(base, bit)
		}
		for window := 66; window <= 131; window++ {
			appendPair(base, window*4+1)
		}
	}
	codeBase := trieCodeKey(0, 0, 1)
	for window := 2; window <= 67; window++ {
		appendPair(codeBase, window*4+1)
	}
	for _, first := range []byte{0, 0x10} {
		for _, second := range []byte{0, 8} {
			for seed := byte(1); seed < 4; seed++ {
				appendKey(trieCodeKey(first, second, seed))
			}
		}
	}
	return keys, prefixes
}

func churnBatch(rng *rand.Rand, keys, prefixes [][]byte, state map[string]Op) []Op {
	count := 1 + rng.Intn(6)
	used := make(map[string]struct{}, count)
	ops := make([]Op, 0, count+2)
	if rng.Intn(2) == 0 {
		prefix := prefixes[rng.Intn(len(prefixes))]
		used[string(prefix)] = struct{}{}
		ops = append(ops, Drop(prefix))
	}
	for len(ops) < count {
		key := keys[rng.Intn(len(keys))]
		if _, ok := used[string(key)]; ok {
			continue
		}
		used[string(key)] = struct{}{}
		if rng.Intn(10) != 0 {
			ops = append(ops, Op{Key: key})
		} else {
			ops = append(ops, Op{Key: key, Value: testTrieValue(byte(rng.Intn(255) + 1))})
		}
	}
	sort.Slice(ops, func(i, j int) bool { return bytes.Compare(churnOpKey(ops[i]), churnOpKey(ops[j])) < 0 })
	return ops
}

func churnOpKey(op Op) []byte {
	if len(op.Drop) != 0 {
		return op.Drop
	}
	return op.Key
}

func churnOpKeys(ops []Op) [][]byte {
	keys := make([][]byte, len(ops))
	for i := range ops {
		keys[i] = churnOpKey(ops[i])
	}
	return keys
}

func updateChurnState(state map[string]Op, ops []Op) {
	for _, op := range ops {
		if len(op.Drop) != 0 {
			for key := range state {
				if bytes.HasPrefix([]byte(key), op.Drop) {
					delete(state, key)
				}
			}
			continue
		}
		if op.Value == ([eip8297.ValueLength]byte{}) {
			delete(state, string(op.Key))
			continue
		}
		state[string(op.Key)] = op
	}
}

func churnEntries(state map[string]Op) []Op {
	entries := make([]Op, 0, len(state))
	for _, entry := range state {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	return entries
}

func mergeChurnBatches() ([][]Op, []Op) {
	addresses := [][]byte{
		bytes.Repeat([]byte{0x11}, 20),
		bytes.Repeat([]byte{0x22}, 20),
		bytes.Repeat([]byte{0x33}, 20),
	}
	codeValue := eip8297.CodeHashValue(common.Hash{})
	initial := []Op{
		{Key: eip8297.TreeKeyAccount(addresses[0], eip8297.CodeHashLeafKey), Value: codeValue},
		{Key: eip8297.TreeKeyAccount(addresses[1], eip8297.CodeHashLeafKey), Value: codeValue},
	}
	balance := *new(uint256.Int)
	update := []Op{
		{Key: eip8297.TreeKeyAccount(addresses[0], eip8297.BasicDataLeafKey), merge: &feedMerge{kind: mergeBasicData, nonce: 7, balance: balance, codeHash: common.Hash{}}},
		{Key: eip8297.TreeKeyAccount(addresses[0], eip8297.CodeHashLeafKey), merge: &feedMerge{kind: mergeCodeHash, codeHash: common.Hash{}}},
		{Key: eip8297.TreeKeyAccount(addresses[1], eip8297.BasicDataLeafKey), merge: &feedMerge{kind: mergeBasicData, balance: balance, codeHash: common.Hash{}}},
		{Key: eip8297.TreeKeyAccount(addresses[1], eip8297.CodeHashLeafKey), merge: &feedMerge{kind: mergeCodeHash, codeHash: common.Hash{}}},
		{Key: eip8297.TreeKeyAccount(addresses[2], eip8297.CodeHashLeafKey), merge: &feedMerge{kind: mergeCodeHash, codeHash: common.Hash{}}},
	}
	sort.Slice(update, func(i, j int) bool { return bytes.Compare(update[i].Key, update[j].Key) < 0 })
	basic, err := eip8297.EncodeBasicData(7, &balance, 0)
	if err != nil {
		panic(err)
	}
	final := []Op{
		initial[0],
		{Key: eip8297.TreeKeyAccount(addresses[0], eip8297.BasicDataLeafKey), Value: basic},
		initial[1],
		{Key: eip8297.TreeKeyAccount(addresses[2], eip8297.CodeHashLeafKey), Value: codeValue},
	}
	sort.Slice(initial, func(i, j int) bool { return bytes.Compare(initial[i].Key, initial[j].Key) < 0 })
	sort.Slice(final, func(i, j int) bool { return bytes.Compare(final[i].Key, final[j].Key) < 0 })
	return [][]Op{initial, update}, final
}

func formatChurnSeed(seed int64) string {
	return string([]byte{
		hexDigit(byte(uint64(seed) >> 60)),
		hexDigit(byte(uint64(seed) >> 56)),
		hexDigit(byte(uint64(seed) >> 52)),
		hexDigit(byte(uint64(seed) >> 48)),
		hexDigit(byte(uint64(seed) >> 44)),
		hexDigit(byte(uint64(seed) >> 40)),
		hexDigit(byte(uint64(seed) >> 36)),
		hexDigit(byte(uint64(seed) >> 32)),
		hexDigit(byte(uint64(seed) >> 28)),
		hexDigit(byte(uint64(seed) >> 24)),
		hexDigit(byte(uint64(seed) >> 20)),
		hexDigit(byte(uint64(seed) >> 16)),
		hexDigit(byte(uint64(seed) >> 12)),
		hexDigit(byte(uint64(seed) >> 8)),
		hexDigit(byte(uint64(seed) >> 4)),
		hexDigit(byte(seed)),
	})
}

func hexDigit(value byte) byte {
	return "0123456789abcdef"[value&0xf]
}
