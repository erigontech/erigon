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

package commitment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// withDeEmbedCommitment toggles the DeEmbedCommitment flag for the duration of
// a test. It is not safe to run under t.Parallel() because the flag is a
// process-global variable.
func withDeEmbedCommitment(tb testing.TB, enabled bool) {
	tb.Helper()
	prev := DeEmbedCommitment
	DeEmbedCommitment = enabled
	tb.Cleanup(func() { DeEmbedCommitment = prev })
}

// runScenarioHash runs a sequence of UpdateBuilder batches against a fresh
// MockState/HexPatriciaHashed pair and returns the final root hash. Between
// batches the trie is Reset so it re-unfolds from the updated branch storage,
// which exercises the de-embedded read path.
func runScenarioHash(t *testing.T, accountKeyLen int16, batches [][2]any) []byte {
	t.Helper()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(accountKeyLen, ms)
	hph.SetTrace(false)
	ctx := context.Background()

	var upds *Updates
	var lastRoot []byte
	for i, b := range batches {
		plainKeys := b[0].([][]byte)
		updates := b[1].([]Update)
		require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

		if i == 0 {
			upds = WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		} else {
			hph.Reset()
			WrapKeyUpdatesInto(t, upds, plainKeys, updates)
		}
		root, err := hph.Process(ctx, upds, "", nil, WarmupConfig{})
		require.NoError(t, err)
		lastRoot = append(lastRoot[:0], root...)
	}
	if upds != nil {
		upds.Close()
	}
	return lastRoot
}

// Test_DeEmbedCommitment_Equivalence verifies that the final root hash
// computed over a sequence of updates is identical regardless of whether
// branches are stored embedded (single-blob) or de-embedded (per-child keys).
func Test_DeEmbedCommitment_Equivalence(t *testing.T) {
	type scenario struct {
		name          string
		accountKeyLen int16
		batches       [][2]any
	}

	mk := func(pairs ...[2]any) [][2]any { return pairs }
	build := func(b *UpdateBuilder) [2]any {
		keys, upds := b.Build()
		return [2]any{keys, upds}
	}

	scenarios := []scenario{
		{
			name:          "singular_updates",
			accountKeyLen: 1,
			batches: mk(
				build(NewUpdateBuilder().
					Balance("00", 4).
					Balance("01", 5).
					Balance("02", 6).
					Balance("03", 7).
					Balance("04", 8).
					Storage("04", "01", "0401").
					Storage("03", "56", "050505").
					Storage("03", "57", "060606").
					Balance("05", 9).
					Storage("05", "02", "8989").
					Storage("05", "04", "9898")),
				build(NewUpdateBuilder().Storage("03", "58", "050506")),
				build(NewUpdateBuilder().Storage("03", "58", "020807")),
			),
		},
		{
			name:          "mixed_with_deletes",
			accountKeyLen: 1,
			batches: mk(
				build(NewUpdateBuilder().
					Balance("00", 4).
					Nonce("00", 246462653).
					Balance("01", 5).
					CodeHash("03", "aaaaaaaaaaf7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a870").
					Storage("04", "01", "0401").
					Storage("03", "56", "050505")),
				build(NewUpdateBuilder().
					Delete("01").
					Balance("00", 99).
					Storage("03", "56", "0a0b0c")),
				build(NewUpdateBuilder().
					DeleteStorage("03", "56").
					Balance("02", 42)),
			),
		},
		{
			name:          "addr_len_addresses",
			accountKeyLen: length.Addr,
			batches: mk(
				build(NewUpdateBuilder().
					Balance("71562b71999873db5b286df957af199ec94617f7", 999860099).
					Nonce("71562b71999873db5b286df957af199ec94617f7", 3).
					Balance("3a220f351252089d385b29beca14e27f204c296a", 900234).
					Balance("0000000000000000000000000000000000000000", 2000000000000138901).
					Balance("1337beef00000000000000000000000000000000", 4000000000000138901)),
				build(NewUpdateBuilder().
					Balance("71562b71999873db5b286df957af199ec94617f7", 2345234560099).
					Nonce("71562b71999873db5b286df957af199ec94617f7", 4).
					Delete("1337beef00000000000000000000000000000000").
					Balance("3a220f351252089d385b29beca14e27f204c296a", 820234).
					Balance("0000000000000000000000000000000000000000", 3000000000000138901)),
			),
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			withDeEmbedCommitment(t, false)
			embeddedRoot := runScenarioHash(t, sc.accountKeyLen, sc.batches)

			withDeEmbedCommitment(t, true)
			deEmbeddedRoot := runScenarioHash(t, sc.accountKeyLen, sc.batches)

			require.Equal(t, embeddedRoot, deEmbeddedRoot,
				"root hash mismatch between embedded (%x) and de-embedded (%x) modes",
				embeddedRoot, deEmbeddedRoot)
		})
	}
}

// Test_DeEmbedCommitment_RoundTrip verifies that SplitBranchDataIntoChildren
// followed by ReassembleBranchData reproduces the original branch blob
// byte-for-byte for representative cases.
func Test_DeEmbedCommitment_RoundTrip(t *testing.T) {
	// Ensure branches are stored embedded so ms.cm holds full blobs to
	// split/reassemble. The test exercises the byte-level encoding contract,
	// independent of the storage split.
	withDeEmbedCommitment(t, false)

	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(1, ms)
	hph.SetTrace(false)

	plainKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Balance("10", 5).
		Balance("20", 6).
		Storage("20", "01", "deadbeef").
		Storage("20", "02", "cafebabe").
		Nonce("20", 7).
		Balance("30", 8).
		Delete("10").
		Build()

	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	_, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	require.NotEmpty(t, ms.cm, "expected branch entries after Process")
	for prefix, data := range ms.cm {
		if len(data) < 4 {
			continue
		}
		tMap, aMap, cells, err := SplitBranchDataIntoChildren(data)
		require.NoErrorf(t, err, "split %x", prefix)
		reassembled, err := ReassembleBranchData(tMap, aMap, cells, nil)
		require.NoErrorf(t, err, "reassemble %x", prefix)
		require.Equalf(t, []byte(data), []byte(reassembled),
			"round-trip mismatch at prefix %x", prefix)
	}
}
