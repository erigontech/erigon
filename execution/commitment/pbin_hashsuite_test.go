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

// The selection has to reach both seams: an engine whose node hashing and key
// derivation disagreed would build a tree no one can reproduce.
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
			require.Equal(t, tc.wantSame, pph.updateStream.keyDigest.sum != nil, "key derivation seam")

			// The buffer's hasher has to derive the same key the engine will look for.
			addr := make([]byte, 20)
			addr[19] = 1
			require.Equal(t, pbinKeyHasherWith(pph.hasher.sum)(addr), tree.hasher(addr))
		})
	}
}

// With BLAKE3 selected the way a node selects it, the engine reproduces the
// reference implementation's roots. The Keccak default must NOT match the same
// vectors — otherwise the selection never reached the engine and the positive
// half proves nothing.
func TestPBinBlake3SuiteMatchesSpecRoots(t *testing.T) {
	pbinRestoreHashSuite(t)
	v := pbinLoadSpecVectors(t)
	require.NotEmpty(t, v.Trie)

	rootOf := func(t *testing.T, tc pbinSpecTrieVector) string {
		t.Helper()
		trie, _ := InitializeTrieAndUpdates(ModeDirect, t.TempDir(), TrieConfig{Variant: VariantBinPatriciaTrie})
		pph := trie.(*PBinPatriciaHashed)
		defer pph.Release()
		pph.ResetContext(NewMockState(t))
		return pbinSpecEngineRoot(t, pph, tc)
	}

	for _, tc := range v.Trie {
		t.Run(tc.Name, func(t *testing.T) {
			require.NoError(t, SetPBinHashSuite(PBinHashBlake3))
			require.Equal(t, tc.Root[2:], rootOf(t, tc))

			if len(tc.Entries) == 0 {
				return // the empty tree is 32 zero bytes under any hash (eip:"Node merkelization")
			}
			require.NoError(t, SetPBinHashSuite(PBinHashKeccak))
			require.NotEqual(t, tc.Root[2:], rootOf(t, tc), "keccak must not reproduce a blake3 reference root")
		})
	}
}
