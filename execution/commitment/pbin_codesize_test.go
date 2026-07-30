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
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

// TestPBinBasicDataLeafCarriesCodeSize checks the BASIC_DATA leaf the engine
// builds for a code-bearing account against the reference's own packings: the
// code size has to reach the leaf value, not be forced to zero.
func TestPBinBasicDataLeafCarriesCodeSize(t *testing.T) {
	t.Parallel()
	v := pbinLoadSpecVectors(t)
	require.NotEmpty(t, v.BasicData)

	addr := pbinOracleAddr(1)
	key := pbinTreeKeyAccount(addr, pbinBasicDataLeafKey)

	for _, tc := range v.BasicData {
		bal, err := uint256.FromDecimal(tc.Balance)
		require.NoError(t, err)

		u := Update{Flags: NonceUpdate | BalanceUpdate, Nonce: tc.Nonce, Balance: *bal, CodeSize: tc.CodeSize}
		got, err := pbinLeafValue(key, &u)
		require.NoError(t, err)
		require.Equal(t, pbinMustHex(t, tc.Value), got[:],
			"BASIC_DATA leaf mismatch for code_size=%d nonce=%d balance=%s", tc.CodeSize, tc.Nonce, tc.Balance)
	}
}

// TestPBinEngineRootCarriesCodeSize drives a code-bearing account through the
// whole engine, so the size has to survive the context read, the cell merge and
// the leaf hash — not just the value encoder.
func TestPBinEngineRootCarriesCodeSize(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(9)
	code := pbinTestCode(1000)
	withCode := new(pbinTestCorpus).accountWithCodeBytes(addr, 4, 500, code)

	_, root := withCode.process(t)
	require.Equal(t, withCode.oracleRoot(t), root)

	// The same leaf set with BASIC_DATA packed at code_size 0 isolates the size:
	// every other leaf, the chunks included, stays where it was.
	sizeless, err := pbinEncodeBasicData(4, uint256.NewInt(500), 0)
	require.NoError(t, err)
	entries := withCode.entries(t)
	basicDataKey := pbinTreeKeyAccount(addr, pbinBasicDataLeafKey)
	patched := 0
	for i := range entries {
		if bytes.Equal(entries[i].key, basicDataKey) {
			entries[i].value, patched = sizeless[:], patched+1
		}
	}
	require.Equal(t, 1, patched)

	want := pbinOracleRoot(entries)
	require.NotEqual(t, want[:], root, "code_size must reach the root")
}

// TestPBinUpdateCodeSizeSurvivesCopyAndReset pins the two Update lifecycle
// hooks the engine relies on: a copied update keeps the size, a reset one drops
// it so a pooled cell cannot inherit a stale code size.
func TestPBinUpdateCodeSizeSurvivesCopyAndReset(t *testing.T) {
	t.Parallel()

	u := Update{Flags: CodeUpdate, CodeHash: common.Hash{0x01}, CodeSize: 24576}
	require.Equal(t, uint64(24576), u.Copy().CodeSize)

	u.Reset()
	require.Zero(t, u.CodeSize)
}

// TestPBinUpdateCodeSizeMergesWithCodeHash pins that the size travels with the
// hash: they describe the same code, so a merge must never leave one of them
// from the old account and the other from the new.
func TestPBinUpdateCodeSizeMergesWithCodeHash(t *testing.T) {
	t.Parallel()

	dst := Update{Flags: CodeUpdate, CodeHash: common.Hash{0x01}, CodeSize: 100}
	dst.Merge(&Update{Flags: CodeUpdate, CodeHash: common.Hash{0x02}, CodeSize: 200})
	require.Equal(t, common.Hash{0x02}, dst.CodeHash)
	require.Equal(t, uint64(200), dst.CodeSize)
}

// TestPBinPushSideNeverDeliversCode pins that Updates.TouchCode cannot feed the
// bin trie: the variant is hardwired to ModeDirect, which interns plain keys
// only and hands the trie a nil update. Everything the tree hashes comes from
// the read side, so patching the push side to carry code would be dead code.
func TestPBinPushSideNeverDeliversCode(t *testing.T) {
	t.Parallel()

	cfg := DefaultTrieConfig()
	cfg.Variant = VariantBinPatriciaTrie
	trie, upd := InitializeTrieAndUpdates(ModeUpdate, t.TempDir(), cfg)
	defer upd.Close()

	require.IsType(t, &PBinPatriciaHashed{}, trie)
	require.Equal(t, ModeDirect, upd.Mode(), "the bin variant overrides the requested mode")

	addr := pbinOracleAddr(3)
	upd.TouchPlainKey(string(addr), []byte{0x60, 0x00, 0x60, 0x00}, upd.TouchCode)

	keys := 0
	require.NoError(t, upd.HashSort(context.Background(), nil, func(treeKey, plainKey []byte, u *Update) error {
		keys++
		require.Nil(t, u, "ModeDirect delivers no update, so TouchCode cannot reach the trie")
		return nil
	}))
	require.Equal(t, 1, keys)
}
