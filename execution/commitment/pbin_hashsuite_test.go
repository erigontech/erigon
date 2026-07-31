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
	"encoding/hex"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests move the process-wide hash selection, so none of them is parallel.

func pbinRestoreHashSuite(t *testing.T) {
	t.Helper()
	prev := PBinHashSuiteName()
	t.Cleanup(func() { require.NoError(t, SetPBinHashSuite(prev)) })
}

func TestPBinSetHashSuite(t *testing.T) {
	pbinRestoreHashSuite(t)

	require.NoError(t, SetPBinHashSuite(PBinHashBlake3))
	require.Equal(t, PBinHashBlake3, PBinHashSuiteName())

	require.NoError(t, SetPBinHashSuite(PBinHashKeccak))
	require.Equal(t, PBinHashKeccak, PBinHashSuiteName())

	// An absent setting is the Keccak default, not an error.
	require.NoError(t, SetPBinHashSuite(""))
	require.Equal(t, PBinHashKeccak, PBinHashSuiteName())

	require.Error(t, SetPBinHashSuite("sha256"))
	require.Equal(t, PBinHashKeccak, PBinHashSuiteName(), "a rejected name must not change the suite")
}

// TestPBinInitializeTrieAppliesHashSuite pins that the selection reaches both
// seams through the production constructor: an engine whose node hashing and
// key derivation disagreed would build a tree no one can reproduce.
func TestPBinInitializeTrieAppliesHashSuite(t *testing.T) {
	pbinRestoreHashSuite(t)

	for _, tc := range []struct {
		name     string
		wantSame bool
	}{
		{PBinHashKeccak, false},
		{PBinHashBlake3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, SetPBinHashSuite(tc.name))
			trie, tree := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), TrieConfig{Variant: VariantBinPatriciaTrie})
			pph, ok := trie.(*PBinPatriciaHashed)
			require.True(t, ok)
			defer pph.Release()

			require.Equal(t, tc.wantSame, pph.hasher.sum != nil, "node hashing seam")
			require.Equal(t, tc.wantSame, pph.keyDigest.sum != nil, "key derivation seam")

			// The buffer's hasher has to derive the same key the engine will look for.
			addr := make([]byte, 20)
			addr[19] = 1
			require.Equal(t, pbinKeyHasherWith(pph.hasher.sum)(addr), tree.hasher(addr))
		})
	}
}

// TestPBinBlake3SuiteMatchesSpecRoots is the interop check: with BLAKE3 selected
// the way a node selects it, the engine reproduces the reference implementation's
// roots. Under the Keccak default the same vectors must NOT match — otherwise the
// selection is not reaching the engine and the test proves nothing.
func TestPBinBlake3SuiteMatchesSpecRoots(t *testing.T) {
	pbinRestoreHashSuite(t)
	v := pbinLoadRootVectors(t)
	require.NotEmpty(t, v.Trie)

	rootOf := func(t *testing.T, tc int) string {
		t.Helper()
		leaves := make([]pbinEngineLeaf, 0, len(v.Trie[tc].Entries))
		for i, e := range v.Trie[tc].Entries {
			key, err := hex.DecodeString(e.Key[2:])
			require.NoError(t, err)
			val, err := hex.DecodeString(e.Value[2:])
			require.NoError(t, err)
			l, ok := pbinLeafFromVector(key, val, i+1)
			require.True(t, ok)
			leaves = append(leaves, l)
		}
		sort.Slice(leaves, func(i, j int) bool {
			return string(leaves[i].treeKey) < string(leaves[j].treeKey)
		})

		trie, _ := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), TrieConfig{Variant: VariantBinPatriciaTrie})
		pph := trie.(*PBinPatriciaHashed)
		defer pph.Release()
		pph.ResetContext(NewMockState(t))

		for i := range leaves {
			require.NoError(t, pph.followAndUpdate(leaves[i].treeKey, leaves[i].plainKey, &leaves[i].update))
		}
		for pph.grid.activeRows > 0 {
			require.NoError(t, pph.fold())
		}
		require.NoError(t, pph.storeRoot())
		got, err := pph.RootHash()
		require.NoError(t, err)
		return hex.EncodeToString(got)
	}

	for i, tc := range v.Trie {
		t.Run(tc.Name, func(t *testing.T) {
			require.NoError(t, SetPBinHashSuite(PBinHashBlake3))
			require.Equal(t, tc.Root[2:], rootOf(t, i))

			if len(tc.Entries) == 0 {
				return // the empty tree is 32 zero bytes under any hash (eip:208)
			}
			require.NoError(t, SetPBinHashSuite(PBinHashKeccak))
			require.NotEqual(t, tc.Root[2:], rootOf(t, i), "keccak must not reproduce a blake3 reference root")
		})
	}
}
