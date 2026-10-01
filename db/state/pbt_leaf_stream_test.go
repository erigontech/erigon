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

package state_test

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const pbinLeafStreamStepSize = uint64(8)

type pbinLeafStreamAccount struct {
	address     []byte
	code        []byte
	codeWritten bool
	codeHash    common.Hash
	hasCodeHash bool
	nonce       uint64
	balance     uint64
	slots       map[string][]byte
}

func TestForEachPBinLeafFeatures(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := pbinLeafStreamDatadir(t)
	pbinLeafStreamAssertLeaves(t, pbinLeafStreamLeaves(t, db, agg, false))
	pbinLeafStreamAssertLeaves(t, pbinLeafStreamLeaves(t, db, agg, true))
}

func TestForEachPBinLeafEmptyState(t *testing.T) {
	selectPBinLeafStreamHash(t)
	_, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	var leaves []state.PBinLeaf
	require.NoError(t, state.ForEachPBinLeaf(at, nil, true, func(leaf state.PBinLeaf) error {
		leaves = append(leaves, leaf)
		return nil
	}))
	builder, err := eip8297.NewStreamRootBuilder(eip8297.SelectedHash())
	require.NoError(t, err)
	require.Empty(t, leaves)
	root, err := builder.RootHash()
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
}

func TestForEachPBinLeafEIP8297ZeroValuesAndDeletionDropsCodeWhenLastHolderIsDeleted(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	code := []byte{0x60, 0x01, 0x60, 0x00, 0x52}
	address := pbinLeafStreamAddress(0x11)
	writePBinLeafStreamRange(t, db, 0, pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: address, code: code, codeWritten: true, nonce: 1, balance: 1},
	})
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
	require.NoError(t, err)
	defer sd.Close()
	previous, _, err := sd.GetLatest(kv.AccountsDomain, tx, address)
	require.NoError(t, err)
	require.NoError(t, sd.DomainDel(kv.AccountsDomain, tx, address, pbinLeafStreamStepSize, previous))
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	writePBinLeafStreamStates(t, db, pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x22), nonce: 2, balance: 2},
	})
	writePBinLeafStreamStates(t, db, 2*pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x33), nonce: 3, balance: 3},
	})
	require.NoError(t, agg.BuildFiles(db, 2*pbinLeafStreamStepSize, unboundedFinalityCtx))
	leaves := pbinLeafStreamLeaves(t, db, agg, true)
	codeChunkKey := eip8297.TreeKeyCodeChunk(crypto.Keccak256Hash(code), 0)
	entries := make([]eip8297.Entry, 0, len(leaves))
	for _, leaf := range leaves {
		require.NotEqual(t, codeChunkKey, leaf.Key)
		entries = append(entries, eip8297.Entry{Key: leaf.Key, Value: leaf.Value})
	}
	builder, err := eip8297.NewStreamRootBuilder(eip8297.SelectedHash())
	require.NoError(t, err)
	for _, leaf := range leaves {
		require.NoError(t, builder.Add(leaf.Key, leaf.Value))
	}
	root, err := builder.RootHash()
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRootWithHash(entries, eip8297.SelectedHash()), root)
}

func TestForEachPBinLeafCombinesStamps(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	code := []byte{0x60, 0x01, 0x60, 0x00, 0x52}
	addressA := pbinLeafStreamAddress(0x11)
	addressB := pbinLeafStreamAddress(0x22)
	codeHash := crypto.Keccak256Hash(code)
	writePBinLeafStreamRange(t, db, 0, pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: addressA, code: code, codeWritten: true, nonce: 1, balance: 1},
		{address: addressB, codeHash: codeHash, hasCodeHash: true, nonce: 1, balance: 2},
	})
	require.NoError(t, agg.BuildFiles(db, pbinLeafStreamStepSize, unboundedFinalityCtx))
	writePBinLeafStreamStates(t, db, pbinLeafStreamStepSize, []pbinLeafStreamAccount{
		{address: addressA, codeHash: codeHash, hasCodeHash: true, nonce: 2, balance: 3},
		{address: addressB, code: code, codeWritten: true, nonce: 1, balance: 2},
	})

	leaves := pbinLeafStreamLeaves(t, db, agg, false)
	wantBasicA := eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey)
	wantBasicB := eip8297.TreeKeyAccount(addressB, eip8297.BasicDataLeafKey)
	wantChunk := eip8297.TreeKeyCodeChunk(codeHash, 0)
	byKey := make(map[string]state.PBinLeaf, len(leaves))
	for _, leaf := range leaves {
		byKey[string(leaf.Key)] = leaf
	}
	require.EqualValues(t, pbinLeafStreamStepSize, byKey[string(wantBasicA)].Stamp)
	require.EqualValues(t, pbinLeafStreamStepSize, byKey[string(wantBasicB)].Stamp)
	require.EqualValues(t, pbinLeafStreamStepSize, byKey[string(wantChunk)].Stamp)
}

func selectPBinLeafStreamHash(t *testing.T) {
	previous := eip8297.HashSuiteName()
	previousPBin := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		require.NoError(t, eip8297.SetHashSuite(previous))
		require.NoError(t, commitment.SetPBinHashSuite(previousPBin))
	})
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func pbinLeafStreamDatadir(t *testing.T) (kv.TemporalRwDB, *state.Aggregator) {
	db, agg := testDbAndAggregatorv3(t, pbinLeafStreamStepSize)
	writePBinLeafStreamRange(t, db, 0, 2*pbinLeafStreamStepSize, pbinLeafStreamAccounts())
	require.NoError(t, agg.BuildFiles(db, 2*pbinLeafStreamStepSize, unboundedFinalityCtx))
	return db, agg
}

func pbinLeafStreamLeaves(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, filesOnly bool) []state.PBinLeaf {
	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	var tx kv.Tx
	if !filesOnly {
		roTx, err := db.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		t.Cleanup(roTx.Rollback)
		tx = roTx
	}
	var leaves []state.PBinLeaf
	require.NoError(t, state.ForEachPBinLeaf(at, tx, filesOnly, func(leaf state.PBinLeaf) error {
		leaves = append(leaves, state.PBinLeaf{Key: bytes.Clone(leaf.Key), Value: bytes.Clone(leaf.Value), Stamp: leaf.Stamp})
		return nil
	}))
	return leaves
}

func pbinLeafStreamAssertLeaves(t *testing.T, leaves []state.PBinLeaf) {
	byKey := make(map[string]state.PBinLeaf, len(leaves))
	for _, leaf := range leaves {
		byKey[string(leaf.Key)] = leaf
	}
	accounts := pbinLeafStreamAccounts()
	sharedHash := crypto.Keccak256Hash(accounts[0].code)
	require.Contains(t, byKey, string(eip8297.TreeKeyAccount(accounts[0].address, eip8297.BasicDataLeafKey)))
	require.Contains(t, byKey, string(eip8297.TreeKeyStorage(accounts[0].address, []byte{1})))
	require.Contains(t, byKey, string(eip8297.TreeKeyAccount(accounts[2].address, eip8297.DelegationLeafKey)))
	require.NotContains(t, byKey, string(eip8297.TreeKeyAccount(accounts[2].address, eip8297.CodeHashLeafKey)))
	require.NotContains(t, byKey, string(eip8297.TreeKeyAccount(accounts[0].address, eip8297.DelegationLeafKey)))
	require.NotContains(t, byKey, string(eip8297.TreeKeyCodeChunk(crypto.Keccak256Hash(accounts[3].code), 0)))
	require.Contains(t, byKey, string(eip8297.TreeKeyCodeChunk(sharedHash, 0)))
	for i := 1; i < len(leaves); i++ {
		require.Less(t, bytes.Compare(leaves[i-1].Key, leaves[i].Key), 0)
	}
}

func pbinLeafStreamAccounts() []pbinLeafStreamAccount {
	code := []byte{0x60, 0x01, 0x60, 0x00, 0x52}
	delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{0x42}, 20)...)
	return []pbinLeafStreamAccount{
		{address: pbinLeafStreamAddress(0x11), code: code, codeWritten: true, nonce: 1, balance: 1, slots: map[string][]byte{string([]byte{1}): {1}}},
		{address: pbinLeafStreamAddress(0x22), code: code, codeWritten: true, nonce: 2, balance: 2},
		{address: pbinLeafStreamAddress(0x33), code: delegation, codeWritten: true, nonce: 3, balance: 3},
		{address: pbinLeafStreamAddress(0x44), code: make([]byte, eip8297.ChunkDataLen), codeWritten: true, nonce: 4, balance: 4},
		{address: pbinLeafStreamAddress(0x55), nonce: 5, balance: 5},
	}
}

func pbinLeafStreamAddress(seed byte) []byte { return bytes.Repeat([]byte{seed}, length.Addr) }

func writePBinLeafStreamStates(t *testing.T, db kv.TemporalRwDB, txNum uint64, states []pbinLeafStreamAccount) {
	writePBinLeafStreamRange(t, db, txNum, txNum+1, states)
}

func writePBinLeafStreamRange(t *testing.T, db kv.TemporalRwDB, fromTxNum, toTxNum uint64, states []pbinLeafStreamAccount) {
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantHexPatriciaTrie
	cfg.EnableTrieWarmup = false
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg))
	require.NoError(t, err)
	defer sd.Close()
	sd.DiscardWrites(kv.CommitmentDomain)
	for i, state := range states {
		txNum := fromTxNum + uint64(i)*(toTxNum-fromTxNum)/uint64(len(states))
		codeHash := state.codeHash
		if state.codeWritten {
			codeHash = crypto.Keccak256Hash(state.code)
		}
		account := accounts.Account{Nonce: state.nonce, Balance: *uint256.NewInt(state.balance), CodeHash: accounts.EmptyCodeHash}
		if state.codeWritten || state.hasCodeHash {
			account.CodeHash = accounts.InternCodeHash(codeHash)
		}
		encoded := accounts.SerialiseV3(&account)
		previous, _, err := sd.GetLatest(kv.AccountsDomain, tx, state.address)
		require.NoError(t, err)
		require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, state.address, encoded, txNum, previous))
		if state.codeWritten {
			previous, _, err = sd.GetLatest(kv.CodeDomain, tx, state.address)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.CodeDomain, tx, state.address, state.code, txNum, previous))
		}
		for slot, value := range state.slots {
			key := append(append([]byte(nil), state.address...), []byte(slot)...)
			previous, _, err = sd.GetLatest(kv.StorageDomain, tx, key)
			require.NoError(t, err)
			require.NoError(t, sd.DomainPut(kv.StorageDomain, tx, key, value, txNum, previous))
		}
	}
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}
