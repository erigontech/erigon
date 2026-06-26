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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/trie"
)

// Strict (reth-equivalent) witness oracle: root equality is necessary-not-sufficient,
// so each accessed key must also strictly resolve — present keys fully materialized,
// absent keys diverging at a materialized node, never a bare HashNode on the path.

func processAndWitness(t *testing.T, builder *UpdateBuilder, produceExclusionProofs bool,
	touchAccounts [][]byte, touchStorage [][]byte, touchHashed ...[]byte) (root []byte, witnessTrie *trie.Trie) {
	t.Helper()
	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)

	plainKeys, updates := builder.Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	root, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(t, err)

	toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
	defer toWitness.Close()
	for _, a := range touchAccounts {
		toWitness.TouchPlainKey(string(a), nil, toWitness.TouchAccount)
	}
	for _, s := range touchStorage {
		toWitness.TouchPlainKey(string(s), nil, toWitness.TouchStorage)
	}
	for _, h := range touchHashed {
		toWitness.TouchHashedKey(h)
	}

	witnessTrie, rootWitness, err := hph.GenerateWitness(ctx, toWitness, nil, "", produceExclusionProofs)
	require.NoError(t, err)
	require.NotNil(t, witnessTrie)
	require.Equal(t, root, rootWitness, "witness root must equal commitment root")
	return root, witnessTrie
}

func assertPresentStrict(t *testing.T, wt *trie.Trie, plainKey []byte) {
	t.Helper()
	require.True(t, witnessMaterializesNodeAt(wt.RootNode, KeyToHexNibbleHash(plainKey)),
		"present key %x must be materialized on-path", plainKey)
}

func assertAbsentStrict(t *testing.T, wt *trie.Trie, plainKey []byte) {
	t.Helper()
	require.True(t, witnessResolvesAbsence(wt.RootNode, KeyToHexNibbleHash(plainKey), 0),
		"absent key %x must be provable without a bare HashNode", plainKey)
}

func storageKey(account, slot []byte) []byte {
	return append(common.Copy(account), slot...)
}

// assertStrictWitness re-runs Process over builders, generates a legacy-mode witness,
// and asserts the strict oracle for every plain key (present materialized, absent proven).
func assertStrictWitness(t *testing.T, builders []*UpdateBuilder, plainKeysToWitness [][]byte, keyExists []bool, hashedKeysToWitness ...[]byte) {
	t.Helper()
	require.Equal(t, len(plainKeysToWitness), len(keyExists))
	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)

	var root []byte
	for _, b := range builders {
		plainKeys, updates := b.Build()
		require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))
		toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		r, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
		toProcess.Close()
		require.NoError(t, err)
		root = r
	}

	toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
	defer toWitness.Close()
	for _, pk := range plainKeysToWitness {
		if len(pk) == length.Addr {
			toWitness.TouchPlainKey(string(pk), nil, toWitness.TouchAccount)
		} else {
			toWitness.TouchPlainKey(string(pk), nil, toWitness.TouchStorage)
		}
	}
	for _, hk := range hashedKeysToWitness {
		toWitness.TouchHashedKey(hk)
	}

	wt, rootW, err := hph.GenerateWitness(ctx, toWitness, nil, "", true)
	require.NoError(t, err)
	require.Equal(t, root, rootW, "strict witness root must equal commitment root")
	for i, pk := range plainKeysToWitness {
		if keyExists[i] {
			assertPresentStrict(t, wt, pk)
		} else {
			assertAbsentStrict(t, wt, pk)
		}
	}
}

func Test_Witness_StrictResolution(t *testing.T) {
	t.Run("PresentAccountsAndStorage", func(t *testing.T) {
		accounts := make([][]byte, 0, 6)
		builder := NewUpdateBuilder()
		for i := 0; i < 6; i++ {
			a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
			accounts = append(accounts, a)
			builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		}
		storer := accounts[0]
		slots := [][]byte{
			common.FromHex("0000000000000000000000000000000000000000000000000000000000000001"),
			common.FromHex("00000000000000000000000000000000000000000000000000000000000000ff"),
			common.FromHex("8000000000000000000000000000000000000000000000000000000000000000"),
		}
		for _, s := range slots {
			builder.Storage(common.Bytes2Hex(storer), common.Bytes2Hex(s), common.Bytes2Hex(s))
		}

		touchStor := [][]byte{storageKey(storer, slots[0]), storageKey(storer, slots[2])}
		_, wt := processAndWitness(t, builder, true, [][]byte{storer, accounts[3]}, touchStor)

		assertPresentStrict(t, wt, storer)
		assertPresentStrict(t, wt, accounts[3])
		for _, sk := range touchStor {
			assertPresentStrict(t, wt, sk)
		}
	})

	t.Run("AccountWithoutStorage_AbsentSlot", func(t *testing.T) {
		accounts := make([][]byte, 0, 5)
		builder := NewUpdateBuilder()
		for i := 0; i < 5; i++ {
			a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
			accounts = append(accounts, a)
			builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		}
		bare := accounts[0]
		absentSlot := common.FromHex("00000000000000000000000000000000000000000000000000000000000000aa")

		_, wt := processAndWitness(t, builder, true, [][]byte{bare}, [][]byte{storageKey(bare, absentSlot)})

		assertPresentStrict(t, wt, bare)
		assertAbsentStrict(t, wt, storageKey(bare, absentSlot))
	})

	t.Run("MixedPresentAbsentAcrossExtension", func(t *testing.T) {
		extAccts, extHashed := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 3, 2)
		builder := NewUpdateBuilder()
		for i, a := range extAccts {
			builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		}
		for i := 0; i < 8; i++ {
			a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
			builder.Balance(common.Bytes2Hex(a), uint64(100+i))
		}

		shared := extHashed[0]
		absentPrefix := []byte{shared[0], shared[1], shared[2], (shared[3] + 1) & 0xf}
		absentAcct, _ := generateKeyWithHashedPrefix(absentPrefix, length.Addr)

		_, wt := processAndWitness(t, builder, true, [][]byte{extAccts[0], absentAcct}, nil)

		assertPresentStrict(t, wt, extAccts[0])
		assertAbsentStrict(t, wt, absentAcct)
	})
}

func Test_Witness_CanonicalSubsetOfLegacy(t *testing.T) {
	accounts := make([][]byte, 0, 6)
	for i := 0; i < 6; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		accounts = append(accounts, a)
	}
	storer := accounts[0]
	slots := [][]byte{
		common.FromHex("0000000000000000000000000000000000000000000000000000000000000001"),
		common.FromHex("00000000000000000000000000000000000000000000000000000000000000ff"),
	}
	mkBuilder := func() *UpdateBuilder {
		builder := NewUpdateBuilder()
		for i, a := range accounts {
			builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		}
		for _, s := range slots {
			builder.Storage(common.Bytes2Hex(storer), common.Bytes2Hex(s), common.Bytes2Hex(s))
		}
		return builder
	}
	touchStor := [][]byte{storageKey(storer, slots[0])}

	nodeSet := func(t *testing.T, wt *trie.Trie) map[string]struct{} {
		t.Helper()
		nodes, err := wt.RLPEncode()
		require.NoError(t, err)
		set := make(map[string]struct{}, len(nodes))
		for _, n := range nodes {
			set[string(n)] = struct{}{}
		}
		return set
	}

	_, legacy := processAndWitness(t, mkBuilder(), true, [][]byte{storer}, touchStor)
	_, canonical := processAndWitness(t, mkBuilder(), false, [][]byte{storer}, touchStor)

	legacySet := nodeSet(t, legacy)
	canonSet := nodeSet(t, canonical)
	for n := range canonSet {
		_, ok := legacySet[n]
		require.True(t, ok, "canonical witness node not present in legacy witness")
	}
	require.LessOrEqual(t, len(canonSet), len(legacySet))

	assertPresentStrict(t, legacy, storer)
	assertPresentStrict(t, canonical, storer)
	assertPresentStrict(t, legacy, touchStor[0])
	assertPresentStrict(t, canonical, touchStor[0])
}

func BenchmarkGenerateWitness(b *testing.B) {
	ctx := context.Background()
	ms := NewMockState(b)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)

	builder := NewUpdateBuilder()
	accounts := make([][]byte, 0, 128)
	for i := 0; i < 128; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		accounts = append(accounts, a)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		for j := 0; j < 4; j++ {
			slot := common.FromHex(fmt.Sprintf("%064x", j+1))
			builder.Storage(common.Bytes2Hex(a), common.Bytes2Hex(slot), common.Bytes2Hex(slot))
		}
	}
	plainKeys, updates := builder.Build()
	require.NoError(b, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	_, err := hph.Process(ctx, toProcess, "", nil, WarmupConfig{})
	require.NoError(b, err)

	witnessTargets := accounts[:16]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
		for _, a := range witnessTargets {
			toWitness.TouchPlainKey(string(a), nil, toWitness.TouchAccount)
		}
		_, _, err := hph.GenerateWitness(ctx, toWitness, nil, "", false)
		toWitness.Close()
		require.NoError(b, err)
	}
}

func benchWitnessTrie(b *testing.B) (*HexPatriciaHashed, [][]byte) {
	b.Helper()
	ms := NewMockState(b)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	hph.SetTrace(false)
	builder := NewUpdateBuilder()
	accounts := make([][]byte, 0, 128)
	for i := 0; i < 128; i++ {
		a, _ := generateKeyWithHashedPrefix(nil, length.Addr)
		accounts = append(accounts, a)
		builder.Balance(common.Bytes2Hex(a), uint64(i+1))
		for j := 0; j < 4; j++ {
			slot := common.FromHex(fmt.Sprintf("%064x", j+1))
			builder.Storage(common.Bytes2Hex(a), common.Bytes2Hex(slot), common.Bytes2Hex(slot))
		}
	}
	plainKeys, updates := builder.Build()
	require.NoError(b, ms.applyPlainUpdates(plainKeys, updates))
	toProcess := WrapKeyUpdates(b, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()
	_, err := hph.Process(context.Background(), toProcess, "", nil, WarmupConfig{})
	require.NoError(b, err)
	return hph, accounts[:16]
}

func BenchmarkWitnesses(b *testing.B) {
	ctx := context.Background()
	hph, targets := benchWitnessTrie(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
		for _, a := range targets {
			toWitness.TouchPlainKey(string(a), nil, toWitness.TouchAccount)
		}
		_, _, err := hph.Witnesses(ctx, toWitness, false, "")
		toWitness.Close()
		require.NoError(b, err)
	}
}
