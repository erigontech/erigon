// Copyright 2024 The Erigon Authors
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

package vm

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// internMemoWords exercises every limb of the source word, alone and together,
// and repeats one word so a second round has something to hit.
var internMemoWords = []uint256.Int{
	{},
	*uint256.NewInt(1),
	{0, 0, 1, 0},
	{0, 0, 0, 1},
	{1, 1, 1, 1},
	*uint256.NewInt(1),
}

// assertMemoMatches drives a memo past its gate and pins every result to the
// handle the interning function returns. Handles are compared by identity: the
// codebase keys maps and access lists on the pointer inside, so a value-equal
// handle from another allocation is a different account.
func assertMemoMatches[H comparable](t *testing.T, rounds int, memo func(*uint256.Int) H, intern func(*uint256.Int) H) {
	t.Helper()
	for round := range rounds {
		for i := range internMemoWords {
			word := internMemoWords[i]
			require.True(t, intern(&word) == memo(&word), "round %d word %s", round, &word)
		}
	}
}

func TestInternStorageKeyMatchesInternKey(t *testing.T) {
	evm := &EVM{}
	assertMemoMatches(t, storageKeyCacheMinOps+10, evm.internStorageKey,
		func(w *uint256.Int) accounts.StorageKey { return accounts.InternKey(w.Bytes32()) })
}

func TestInternAddressMatchesInternAddress(t *testing.T) {
	evm := &EVM{}
	assertMemoMatches(t, addressCacheMinOps+10, evm.internAddress,
		func(w *uint256.Int) accounts.Address { return accounts.InternAddress(w.Bytes20()) })
}

func TestStorageKeyCacheNotAllocatedForShortLivedEVM(t *testing.T) {
	evm := &EVM{}
	word := uint256.NewInt(7)
	for range storageKeyCacheMinOps {
		evm.internStorageKey(word)
	}
	assert.Nil(t, evm.internCache)

	evm.internStorageKey(word)
	assert.NotNil(t, evm.internCache)
}

func TestAddressCacheNotAllocatedForShortLivedEVM(t *testing.T) {
	evm := &EVM{}
	word := uint256.NewInt(7)
	for range addressCacheMinOps {
		evm.internAddress(word)
	}
	assert.Nil(t, evm.addrCache)

	evm.internAddress(word)
	assert.NotNil(t, evm.addrCache)
}

// poisonEntry replaces the table entry holding want with sentinel and returns
// how many entries it hit. A later call that returns sentinel proves the handle
// came out of the table rather than from unique.Make again.
func poisonEntry[H comparable](handles []H, want, sentinel H) int {
	poisoned := 0
	for i := range handles {
		if handles[i] == want {
			handles[i] = sentinel
			poisoned++
		}
	}
	return poisoned
}

func TestInternAddressServesHitsFromTheTable(t *testing.T) {
	evm := &EVM{}
	word := uint256.Int{0x1122334455667788, 0x99aabbccddeeff00, 0x12345678, 0}
	for range addressCacheMinOps + 1 {
		evm.internAddress(&word)
	}
	require.NotNil(t, evm.addrCache)

	sentinel := accounts.InternAddress(common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"))
	require.Equal(t, 1, poisonEntry(evm.addrCache.handles[:], accounts.InternAddress(word.Bytes20()), sentinel),
		"the warmed word must occupy exactly one entry")

	require.True(t, sentinel == evm.internAddress(&word),
		"a repeat word must be served from the table, not interned again")
}

// TestInternAddressSharesOneEntryPerAddress pins the table's key: the same
// address must hit from any word carrying it, whatever the bits above the low 20
// bytes hold. Keying on the whole word would leave the two forms fighting over
// one bucket, each evicting the other.
func TestInternAddressSharesOneEntryPerAddress(t *testing.T) {
	evm := &EVM{}
	clean := uint256.Int{0x1122334455667788, 0x99aabbccddeeff00, 0x12345678, 0}
	dirty := clean
	dirty[2] |= 0xffffffff00000000
	dirty[3] = 0xdeadbeefdeadbeef

	for range addressCacheMinOps + 1 {
		evm.internAddress(&clean)
	}
	sentinel := accounts.InternAddress(common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"))
	require.Equal(t, 1, poisonEntry(evm.addrCache.handles[:], accounts.InternAddress(clean.Bytes20()), sentinel))

	require.True(t, sentinel == evm.internAddress(&dirty),
		"a word with dirt above the address must hit the entry its clean form filled")
}

func TestInternAddressIgnoresBitsAboveTheAddress(t *testing.T) {
	evm := &EVM{}
	clean := uint256.Int{0x1122334455667788, 0x99aabbccddeeff00, 0x12345678, 0}
	dirty := clean
	dirty[2] |= 0xffffffff00000000
	dirty[3] = 0xdeadbeefdeadbeef

	want := accounts.InternAddress(clean.Bytes20())
	for range addressCacheMinOps + 10 {
		require.True(t, want == evm.internAddress(&clean))
		require.True(t, want == evm.internAddress(&dirty))
	}
}

// TestInternAddressSurvivesResetBetweenBlocks pins what makes the memo worth
// having on a reused worker EVM: interning is pure, so a warmed entry stays
// valid across blocks and clearing it would throw away exactly the reuse the
// table exists for.
func TestInternAddressSurvivesResetBetweenBlocks(t *testing.T) {
	evm := &EVM{}
	word := uint256.Int{0x1122334455667788, 0x99aabbccddeeff00, 0x12345678, 0}
	for range addressCacheMinOps + 1 {
		evm.internAddress(&word)
	}
	sentinel := accounts.InternAddress(common.HexToAddress("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"))
	require.Equal(t, 1, poisonEntry(evm.addrCache.handles[:], accounts.InternAddress(word.Bytes20()), sentinel))

	evm.ResetBetweenBlocks(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, Config{}, &chain.Rules{})

	require.NotNil(t, evm.addrCache, "the memo is per-EVM, not per-block")
	require.True(t, sentinel == evm.internAddress(&word),
		"a warmed address must still hit after the worker moves to the next block")
}

var internSink accounts.Address

// BenchmarkInternAddressHit measures the path every repeat word takes — the one
// the table exists to keep off unique.Make.
func BenchmarkInternAddressHit(b *testing.B) {
	evm := &EVM{}
	words := make([]uint256.Int, 64)
	for i := range words {
		words[i] = uint256.Int{uint64(i+1) * 0x9e3779b97f4a7c15, uint64(i+1) * 0xc2b2ae3d27d4eb4f, uint64(i+1) & 0xffffffff, 0}
	}
	for range addressCacheMinOps + 1 {
		evm.internAddress(&words[0])
	}
	for i := range words {
		evm.internAddress(&words[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		internSink = evm.internAddress(&words[i&63])
	}
}

var keySink accounts.StorageKey

func BenchmarkInternStorageKeyHit(b *testing.B) {
	evm := &EVM{}
	words := make([]uint256.Int, 64)
	for i := range words {
		words[i] = uint256.Int{uint64(i+1) * 0x9e3779b97f4a7c15, uint64(i+1) * 0xc2b2ae3d27d4eb4f, uint64(i + 1), uint64(i + 1)}
	}
	for range storageKeyCacheMinOps + 1 {
		evm.internStorageKey(&words[0])
	}
	for i := range words {
		evm.internStorageKey(&words[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		keySink = evm.internStorageKey(&words[i&63])
	}
}
