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

package commitment

import (
	"context"
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// The deembedded layout persists branch children differently but must produce
// the identical trie root. The tests below drive the same update stream through
// a default (embedded) trie and a deembedded trie and assert the roots match
// round by round. Branch-data maps (MockState.cm) are intentionally NOT compared
// — they differ by design.

func newDeembedTrie(ms *MockState) *HexPatriciaHashed {
	cfg := DefaultTrieConfig()
	cfg.Variant = VariantDeembeddedHexPatricia
	return NewHexPatriciaHashed(length.Addr, ms, cfg)
}

func requireDeembedMatchesEmbedded(tb testing.TB, rounds ...*UpdateBuilder) {
	tb.Helper()
	ctx := context.Background()

	stateEmb := NewMockState(tb)
	stateDe := NewMockState(tb)
	trieEmb := NewHexPatriciaHashed(length.Addr, stateEmb, DefaultTrieConfig())
	trieDe := newDeembedTrie(stateDe)

	for i, builder := range rounds {
		plainKeys, updates := builder.Build()
		require.NoErrorf(tb, stateEmb.applyPlainUpdates(plainKeys, updates), "round %d embedded state update", i)
		require.NoErrorf(tb, stateDe.applyPlainUpdates(plainKeys, updates), "round %d deembed state update", i)

		updsEmb := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		rootEmb, err := trieEmb.Process(ctx, updsEmb, "", nil, WarmupConfig{})
		updsEmb.Close()
		require.NoErrorf(tb, err, "round %d embedded process", i)

		updsDe := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		rootDe, err := trieDe.Process(ctx, updsDe, "", nil, WarmupConfig{})
		updsDe.Close()
		require.NoErrorf(tb, err, "round %d deembed process", i)

		require.Equalf(tb, rootEmb, rootDe, "round %d root mismatch\nembedded=%x\ndeembed =%x", i, rootEmb, rootDe)
	}
}

func Test_Deembed_MatchesEmbedded_Basic(t *testing.T) {
	t.Parallel()
	requireDeembedMatchesEmbedded(t,
		NewUpdateBuilder().
			Balance("1000000000000000000000000000000000000001", 1).
			Nonce("1000000000000000000000000000000000000002", 2).
			Storage(
				"1000000000000000000000000000000000000001",
				"0100000000000000000000000000000000000000000000000000000000000000",
				"0102",
			),
		NewUpdateBuilder().
			Balance("1000000000000000000000000000000000000001", 3).
			CodeHash(
				"1000000000000000000000000000000000000003",
				"0300000000000000000000000000000000000000000000000000000000000000",
			).
			DeleteStorage(
				"1000000000000000000000000000000000000001",
				"0100000000000000000000000000000000000000000000000000000000000000",
			),
		NewUpdateBuilder().
			Delete("1000000000000000000000000000000000000002").
			Storage(
				"1000000000000000000000000000000000000004",
				"0400000000000000000000000000000000000000000000000000000000000000",
				"040506",
			),
	)
}

func Test_Deembed_MatchesEmbedded_Fixtures(t *testing.T) {
	t.Parallel()
	requireDeembedMatchesEmbedded(t, fixtureBaseWithCode())
	requireDeembedMatchesEmbedded(t, fixtureBaseAccounts())
}

// Test_Deembed_MatchesEmbedded_Random drives many rounds of random inserts,
// storage writes and deletes so the trie repeatedly grows, splits, collapses
// and reloads branches from the persisted deembedded layout.
func Test_Deembed_MatchesEmbedded_Random(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(20260719))
	addrs := make([]string, 0, 256)
	slots := make(map[string][]string) // addr -> storage locations (hex)

	randSlot := func(ub *UpdateBuilder, a string) {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		val := make([]byte, 32)
		rnd.Read(val)
		locHex := hex.EncodeToString(loc)
		ub.Storage(a, locHex, hex.EncodeToString(val))
		slots[a] = append(slots[a], locHex)
	}

	const rounds = 25
	builders := make([]*UpdateBuilder, 0, rounds)
	for range rounds {
		ub := NewUpdateBuilder()

		// Insert a handful of fresh accounts, some with storage.
		for range 8 {
			addr := make([]byte, length.Addr)
			rnd.Read(addr)
			a := hex.EncodeToString(addr)
			addrs = append(addrs, a)
			ub.Balance(a, rnd.Uint64()+1)
			for s := 0; s < rnd.Intn(6); s++ {
				randSlot(ub, a)
			}
		}

		// Mutate / add storage to some existing accounts.
		for i := 0; i < 6 && len(addrs) > 0; i++ {
			a := addrs[rnd.Intn(len(addrs))]
			if rnd.Intn(2) == 0 {
				ub.Nonce(a, rnd.Uint64())
			}
			randSlot(ub, a)
		}

		// Delete a few existing accounts entirely — clearing their storage too,
		// as a real selfdestruct would.
		if len(addrs) > 20 {
			for range 3 {
				idx := rnd.Intn(len(addrs))
				a := addrs[idx]
				for _, locHex := range slots[a] {
					ub.DeleteStorage(a, locHex)
				}
				delete(slots, a)
				ub.Delete(a)
				addrs = append(addrs[:idx], addrs[idx+1:]...)
			}
		}

		builders = append(builders, ub)
	}

	requireDeembedMatchesEmbedded(t, builders...)
}

// Test_Deembed_MatchesEmbedded_Whale exercises a single account carrying a large
// storage trie (deep branch nodes — the case the deembedded layout targets).
func Test_Deembed_MatchesEmbedded_Whale(t *testing.T) {
	t.Parallel()

	pk, upds := buildWhaleCorpus(bigAccountWhale(4000))

	ctx := context.Background()
	stateEmb := NewMockState(t)
	stateDe := NewMockState(t)
	require.NoError(t, stateEmb.applyPlainUpdates(pk, upds))
	require.NoError(t, stateDe.applyPlainUpdates(pk, upds))

	trieEmb := NewHexPatriciaHashed(length.Addr, stateEmb, DefaultTrieConfig())
	trieDe := newDeembedTrie(stateDe)

	updsEmb := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	rootEmb, err := trieEmb.Process(ctx, updsEmb, "", nil, WarmupConfig{})
	updsEmb.Close()
	require.NoError(t, err)

	updsDe := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	rootDe, err := trieDe.Process(ctx, updsDe, "", nil, WarmupConfig{})
	updsDe.Close()
	require.NoError(t, err)

	require.Equal(t, rootEmb, rootDe)
}
