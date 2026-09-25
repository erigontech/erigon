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

	"github.com/stretchr/testify/require"

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

func runTrieChurn(t *testing.T, seed int64) {
	t.Helper()
	batches := 5000
	if testing.Short() {
		batches = 100
	}
	rng := rand.New(rand.NewSource(seed))
	keys, prefixes := churnKeys()
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
		default:
			ops = churnBatch(rng, keys, prefixes, state)
		}
		start := cloneRecords(ctx.records)
		trie := NewTrie(ctx)
		root, err := trie.Process(ops)
		if err != nil {
			t.Fatalf("seed=%d batch=%d process: %v ops=%x", seed, batch, err, churnOpKeys(ops))
		}
		updateChurnState(state, ops)
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
	keys := make([][]byte, 0, 40)
	for stemNibble := byte(0); stemNibble < 4; stemNibble++ {
		key := make([]byte, eip8297.AccountKeyLength)
		key[0] = eip8297.AccountZone
		key[len(key)-2] = stemNibble
		key[len(key)-1] = eip8297.BasicDataLeafKey
		keys = append(keys, key)
	}
	prefixes := make([][]byte, 0, 2)
	for addressIndex := byte(0); addressIndex < 2; addressIndex++ {
		address := bytes.Repeat([]byte{0xa0 + addressIndex}, 20)
		stem := eip8297.TreeKeyStorage(address, storageSlot(64))[:33]
		prefixes = append(prefixes, bytes.Clone(stem))
		for _, first := range []byte{0x20, 0x40, 0xc6} {
			for last := byte(0); last < 8; last++ {
				keys = append(keys, storageKeyWithSuffix(stem, first, last))
			}
		}
	}
	for _, first := range []byte{0, 0x10} {
		for _, second := range []byte{0, 8} {
			for seed := byte(1); seed < 4; seed++ {
				keys = append(keys, trieCodeKey(first, second, seed))
			}
		}
	}
	return keys, prefixes
}

func churnBatch(rng *rand.Rand, keys, prefixes [][]byte, state map[string]Op) []Op {
	count := 1 + rng.Intn(6)
	used := make(map[string]struct{}, count)
	ops := make([]Op, 0, count+2)
	if rng.Intn(4) == 0 {
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
		if rng.Intn(5) == 0 {
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
