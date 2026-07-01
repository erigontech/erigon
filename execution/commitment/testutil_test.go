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

// Shared helpers for commitment tests: trie processing wrappers, key generators,
// cell-row generators and common update fixtures.

package commitment

import (
	"context"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

// forEachMode runs fn as a subtest once per Updates mode, named after the mode.
func forEachMode(t *testing.T, fn func(t *testing.T, mode Mode)) {
	t.Helper()
	for _, mode := range []Mode{ModeDirect, ModeUpdate} {
		name := "ModeDirect"
		if mode == ModeUpdate {
			name = "ModeUpdate"
		}
		t.Run(name, func(t *testing.T) { fn(t, mode) })
	}
}

// processSeq feeds updates to the trie one key at a time (a fresh Process call per key),
// mirroring per-block processing, and returns the root after the last key.
func processSeq(tb testing.TB, ms *MockState, trie *HexPatriciaHashed, plainKeys [][]byte, updates []Update) []byte {
	tb.Helper()
	ctx := context.Background()
	var root []byte
	for i := range updates {
		require.NoError(tb, ms.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1]))
		upds := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])
		r, err := trie.Process(ctx, upds, "", nil, WarmupConfig{})
		upds.Close()
		require.NoError(tb, err)
		root = common.Copy(r)
	}
	return root
}

// processBatch applies all updates in a single Process call and returns the root.
func processBatch(tb testing.TB, ms *MockState, trie *HexPatriciaHashed, plainKeys [][]byte, updates []Update) []byte {
	tb.Helper()
	require.NoError(tb, ms.applyPlainUpdates(plainKeys, updates))
	upds := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	root, err := trie.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(tb, err)
	return common.Copy(root)
}

// processFreshTrie builds a fresh trie/state, applies the updates in a single ModeDirect batch and
// returns both the trie (for post-run inspection) and the root.
func processFreshTrie(t *testing.T, plainKeys [][]byte, updates []Update) (*HexPatriciaHashed, []byte) {
	t.Helper()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	root, err := hph.Process(context.Background(), toProcess, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return hph, root
}

// fixtureBaseAccounts returns a builder with a mixed set of account and storage updates shared
// by the unique-representation tests. Callers may chain further calls to vary the fixture.
func fixtureBaseAccounts() *UpdateBuilder {
	return NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
		Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
		Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
		Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
		Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
		Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
		Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
		Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898")
}

// fixtureBaseWithCode is fixtureBaseAccounts plus a balance bump and a code hash, used by the
// tests that exercise code-bearing accounts.
func fixtureBaseWithCode() *UpdateBuilder {
	return fixtureBaseAccounts().
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1237).
		CodeHash("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed")
}

// fixtureBrokenUniqueRepr is fixtureBaseAccounts with a smaller ba7a3b7b balance and no code hash,
// used by the broken-unique-representation test (its original table also repeats a couple of keys,
// which Build de-duplicates).
func fixtureBrokenUniqueRepr() *UpdateBuilder {
	return fixtureBaseAccounts().
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 100000)
}

func generateCellRow(tb testing.TB, size int) (row []*cell, bitmap uint16) {
	tb.Helper()

	row = make([]*cell, size)
	var bm uint16
	for i := 0; i < len(row); i++ {
		row[i] = new(cell)
		row[i].hashLen = 32
		n, err := rand.Read(row[i].hash[:])
		require.NoError(tb, err)
		require.Equal(tb, int(row[i].hashLen), n)

		th := rand.Intn(120)
		switch {
		case th > 70:
			n, err = rand.Read(row[i].accountAddr[:])
			require.NoError(tb, err)
			row[i].accountAddrLen = int16(n)
		case th > 20 && th <= 70:
			n, err = rand.Read(row[i].storageAddr[:])
			require.NoError(tb, err)
			row[i].storageAddrLen = int16(n)
		case th <= 20:
			n, err = rand.Read(row[i].extension[:th])
			row[i].extLen = int16(n)
			require.NoError(tb, err)
			require.Equal(tb, th, n)
		}
		bm |= uint16(1 << i)
	}
	return row, bm
}

// generateCellEncodeDataRow converts a cell row (from generateCellRow) into a [16]cellEncodeData array.
func generateCellEncodeDataRow(tb testing.TB, row []*cell, bm uint16) [16]cellEncodeData {
	tb.Helper()
	var data [16]cellEncodeData
	for bitset := bm; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		if nibble < len(row) && row[nibble] != nil {
			data[nibble] = cellEncodeDataFromCell(row[nibble])
		}
		bitset ^= bit
	}
	return data
}

// encodeCellRow generates a random cell row of the given size and returns it with its
// bitmap and the BranchData produced by encoding it (touchMap == afterMap == bitmap).
func encodeCellRow(tb testing.TB, size int) (row []*cell, bm uint16, enc BranchData) {
	tb.Helper()
	row, bm = generateCellRow(tb, size)
	be := NewBranchEncoder(1024)
	cellData := generateCellEncodeDataRow(tb, row, bm)
	enc, err := be.EncodeBranch(bm, bm, bm, &cellData)
	require.NoError(tb, err)
	return row, bm, enc
}

// testWarmuper builds a Warmuper with the constant test config (Enabled, MaxDepth 64,
// LogPrefix "test"), varying only the context factory and worker count.
func testWarmuper(ctx context.Context, factory TrieContextFactory, workers int) *Warmuper {
	return NewWarmuper(ctx, WarmupConfig{
		Enabled:    true,
		CtxFactory: factory,
		NumWorkers: workers,
		MaxDepth:   64,
		LogPrefix:  "test",
	})
}

func cellMustEqual(tb testing.TB, first, second *cell) {
	tb.Helper()
	require.Equal(tb, first.hashedExtLen, second.hashedExtLen)
	require.Equal(tb, first.hashedExtension[:first.hashedExtLen], second.hashedExtension[:second.hashedExtLen])
	require.Equal(tb, first.hashLen, second.hashLen)
	require.Equal(tb, first.hash[:first.hashLen], second.hash[:second.hashLen])
	require.Equal(tb, first.accountAddrLen, second.accountAddrLen)
	require.Equal(tb, first.storageAddrLen, second.storageAddrLen)
	require.Equal(tb, first.accountAddr[:], second.accountAddr[:])
	require.Equal(tb, first.storageAddr[:], second.storageAddr[:])
	require.Equal(tb, first.extension[:first.extLen], second.extension[:second.extLen])
	require.Equal(tb, first.stateHash[:first.stateHashLen], second.stateHash[:second.stateHashLen])

	// encode doesn't code Nonce, Balance, CodeHash and Storage, Delete fields
}

// requireDecodedCellEq compares the fields the branch decoder is responsible for
// restoring; hashedExtension and stateHash are derived separately and are skipped.
func requireDecodedCellEq(tb testing.TB, i int, orig, decoded *cell) {
	tb.Helper()
	require.Equal(tb, orig.extLen, decoded.extLen, "cell %d extLen", i)
	require.Equal(tb, orig.extension[:orig.extLen], decoded.extension[:decoded.extLen], "cell %d extension", i)
	require.Equal(tb, orig.accountAddrLen, decoded.accountAddrLen, "cell %d accountAddrLen", i)
	require.Equal(tb, orig.accountAddr[:orig.accountAddrLen], decoded.accountAddr[:decoded.accountAddrLen], "cell %d accountAddr", i)
	require.Equal(tb, orig.storageAddrLen, decoded.storageAddrLen, "cell %d storageAddrLen", i)
	require.Equal(tb, orig.storageAddr[:orig.storageAddrLen], decoded.storageAddr[:decoded.storageAddrLen], "cell %d storageAddr", i)
	require.Equal(tb, orig.hashLen, decoded.hashLen, "cell %d hashLen", i)
	require.Equal(tb, orig.hash[:orig.hashLen], decoded.hash[:decoded.hashLen], "cell %d hash", i)
}
