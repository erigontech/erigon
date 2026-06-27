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

// Test_Witnesses_ParityWithToWitnessTrie checks the on-the-fly Witnesses() against
// the legacy toWitnessTrie builder: same root, the captured node set reconstructs
// the root and strictly resolves every accessed key, and reports the node-set diff.
func Test_Witnesses_ParityWithToWitnessTrie(t *testing.T) {
	accounts := make([][]byte, 0, 8)
	builder := NewUpdateBuilder()
	for i := 0; i < 8; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		accounts = append(accounts, a)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
	}
	storer := accounts[0]
	presentSlot := common.FromHex("0000000000000000000000000000000000000000000000000000000000000001")
	absentSlot := common.FromHex("00000000000000000000000000000000000000000000000000000000000000aa")
	builder.Storage(common.Bytes2Hex(storer), common.Bytes2Hex(presentSlot), common.Bytes2Hex(presentSlot))

	plainKeys, updates := builder.Build()

	touchAccts := [][]byte{storer, accounts[3]}
	touchStor := [][]byte{storageKey(storer, presentSlot), storageKey(storer, absentSlot)}
	keyExists := map[string]bool{
		string(storer): true, string(accounts[3]): true,
		string(storageKey(storer, presentSlot)): true,
		string(storageKey(storer, absentSlot)):  false,
	}

	// Legacy path: GenerateWitness -> toWitnessTrie -> RLPEncode.
	hphA, rootA := processFreshTrie(t, plainKeys, updates)
	wt, rootWA, err := hphA.GenerateWitness(context.Background(), touchUpdates(touchAccts, touchStor), nil, "", true)
	require.NoError(t, err)
	require.Equal(t, rootA, rootWA, "legacy witness root must equal commitment root")
	setA, err := wt.RLPEncode()
	require.NoError(t, err)

	// New path: Witnesses() on-the-fly node set.
	hphB, rootB := processFreshTrie(t, plainKeys, updates)
	setB, _, rootWB, err := hphB.Witnesses(context.Background(), touchUpdates(touchAccts, touchStor), true, "")
	require.NoError(t, err)
	require.Equal(t, rootB, rootWB, "Witnesses root must equal commitment root")
	require.Equal(t, rootA, rootB, "both paths build the same trie")

	// The captured node set must reconstruct the root and strictly resolve every key.
	decoded, err := trie.RLPDecode(setB)
	require.NoError(t, err)
	require.Equal(t, rootB, decoded.Root(), "Witnesses node set must reconstruct the root")

	for _, a := range touchAccts {
		assertPresentStrict(t, decoded, a)
	}
	for _, s := range touchStor {
		if keyExists[string(s)] {
			assertPresentStrict(t, decoded, s)
		} else {
			assertAbsentStrict(t, decoded, s)
		}
	}

	// Report node-set diff vs the legacy builder (informational).
	inA := make(map[string]struct{}, len(setA))
	for _, n := range setA {
		inA[string(n)] = struct{}{}
	}
	inB := make(map[string]struct{}, len(setB))
	for _, n := range setB {
		inB[string(n)] = struct{}{}
	}
	var onlyA, onlyB int
	for k := range inA {
		if _, ok := inB[k]; !ok {
			onlyA++
		}
	}
	for k := range inB {
		if _, ok := inA[k]; !ok {
			onlyB++
		}
	}
	t.Logf("node-set sizes: legacy=%d witnesses=%d; only-in-legacy=%d only-in-witnesses=%d", len(setA), len(setB), onlyA, onlyB)
}

// Test_Witnesses_ExclusionAcrossFoldedExtension drives Witnesses() in legacy mode
// on the #21810 shape (absent slot diverging inside a folded storage extension)
// and asserts the captured set proves absence — the diverging branch is
// materialized during positioning, no toWitnessTrie walk.
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
