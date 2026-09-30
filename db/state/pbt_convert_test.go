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
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestConvertPBinMatchesReferenceRootAndVerification(t *testing.T) {
	selectPBinConvertHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertState(t, db)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()

	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	var leaves []state.PBinLeaf
	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	require.NoError(t, state.ForEachPBinLeaf(at, sourceTx, false, func(leaf state.PBinLeaf) error {
		leaves = append(leaves, state.PBinLeaf{Key: bytes.Clone(leaf.Key), Value: bytes.Clone(leaf.Value), Stamp: leaf.Stamp})
		return nil
	}))

	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentBinDomain,
		EndTxNum:         8,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	entries := make([]eip8297.Entry, 0, len(leaves))
	for _, leaf := range leaves {
		entries = append(entries, eip8297.Entry{Key: leaf.Key, Value: leaf.Value})
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	require.Equal(t, eip8297.StateRootWithHash(entries, eip8297.HashBytes), root)

	targetTx.Rollback()
}

func TestConvertPBinEmptyStreamHasZeroRoot(t *testing.T) {
	selectPBinConvertHash(t)
	db, agg := temporal.Open(t, 8)
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentBinDomain,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
}

func TestVerifyPBinDomainRejectsCorruptedRow(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertState(t, db)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	root, err := state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         8,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	require.NotEqual(t, eip8297.EmptyTreeHash, root)
	targetTx.Rollback()

	at := agg.BeginFilesRo()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	var rowKey, rowValue []byte
	for iter.HasNext() {
		key, value, nextErr := iter.Next()
		require.NoError(t, nextErr)
		if !commitment.IsCommitmentStateKey(key) {
			rowKey, rowValue = bytes.Clone(key), bytes.Clone(value)
			break
		}
	}
	iter.Close()
	at.Close()
	require.NotEmpty(t, rowKey)
	badTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer badTx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), badTx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	previous, _, err := domains.GetLatest(kv.CommitmentDomain, badTx, rowKey)
	require.NoError(t, err)
	corrupted := bytes.Clone(rowValue)
	corrupted[0] ^= 1
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, badTx, rowKey, corrupted, 9, previous))
	require.NoError(t, domains.Flush(t.Context(), badTx))
	require.NoError(t, badTx.Commit())
	domains.Close()
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	require.Error(t, state.VerifyPBinDomain(t.Context(), verifyTx, agg, kv.CommitmentDomain))
}

func selectPBinConvertHash(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func selectPBinBinOnlyHash(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func writePBinConvertState(t *testing.T, db kv.TemporalRwDB) {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	for i := byte(1); i <= 2; i++ {
		address := bytes.Repeat([]byte{i}, length.Addr)
		account := accounts.Account{Nonce: uint64(i), Balance: *uint256.NewInt(uint64(i)), CodeHash: accounts.EmptyCodeHash}
		previous, _, err := domains.GetLatest(kv.AccountsDomain, tx, address)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), uint64(i), previous))
		slot := append(bytes.Clone(address), bytes.Repeat([]byte{i}, 32)...)
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, slot, []byte{i}, uint64(i), nil))
	}
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}
