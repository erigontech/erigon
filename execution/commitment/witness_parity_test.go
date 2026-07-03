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

package commitment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

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

func touchUpdates(touchAccounts, touchStorage [][]byte) *Updates {
	u := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
	for _, a := range touchAccounts {
		u.TouchPlainKey(string(a), nil, u.TouchAccount)
	}
	for _, s := range touchStorage {
		u.TouchPlainKey(string(s), nil, u.TouchStorage)
	}
	return u
}

// Test_Witnesses_ExclusionAcrossFoldedExtension drives Witnesses() in legacy mode
// on the #21810 shape (absent slot diverging inside a folded storage extension)
// and asserts the captured set proves absence — the diverging branch is
// materialized during positioning.
func Test_Witnesses_ExclusionAcrossFoldedExtension(t *testing.T) {
	acctPlains, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 2, 6)
	acctPlain := acctPlains[0]
	storPlain, storHashed := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 5, 2)

	builder := NewUpdateBuilder()
	for i, a := range acctPlains {
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	for _, sk := range storPlain {
		builder.Storage(common.Bytes2Hex(acctPlain), common.Bytes2Hex(sk), common.Bytes2Hex(sk))
	}
	plainKeys, updates := builder.Build()

	shared := storHashed[0]
	absentPrefix := []byte{shared[0], shared[1], (shared[2] + 1) & 0xf}
	absentSlotPlain, _ := generateKeyWithHashedPrefix(absentPrefix, length.Hash)
	absentStorageKey := storageKey(acctPlain, absentSlotPlain)

	hph, root := processFreshTrie(t, plainKeys, updates)
	setB, _, rootW, err := hph.Witnesses(context.Background(),
		touchUpdates([][]byte{acctPlain}, [][]byte{absentStorageKey}), true, "")
	require.NoError(t, err)
	require.Equal(t, root, rootW)

	decoded, err := trie.RLPDecode(setB)
	require.NoError(t, err)
	require.Equal(t, root, decoded.Root(), "Witnesses set must reconstruct the root")

	assertPresentStrict(t, decoded, acctPlain)
	require.True(t, witnessResolvesAbsence(decoded.RootNode, KeyToHexNibbleHash(absentStorageKey), 0),
		"Witnesses must materialize the diverging branch to prove the absent slot")
}
