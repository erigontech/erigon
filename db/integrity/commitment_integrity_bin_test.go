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

package integrity_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	commitmenttemporal "github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestCheckCommitmentRootAcceptsConvertedHexBin(t *testing.T) {
	fixture := newPBinIntegrityFixture(t)
	reader := fixture.blockReader(t)
	require.NoError(t, integrity.CheckCommitmentRoot(t.Context(), fixture.db, reader, true, log.New()))
}

func TestCheckCommitmentRootRejectsDualDatadirWithoutLatestBinCheckpoint(t *testing.T) {
	fixture := newPBinIntegrityFixture(t)
	tx, err := fixture.db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	previous, _, err := domains.GetLatest(kv.CommitmentBinDomain, tx, commitment.KeyCommitmentState)
	require.NoError(t, err)
	require.NotEmpty(t, previous)
	require.NoError(t, domains.DomainDel(kv.CommitmentBinDomain, tx, commitment.KeyCommitmentState, 9, previous))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 2, 9))
	require.NoError(t, tx.Commit())
	domains.Close()
	require.NoError(t, fixture.agg.BuildFiles2(t.Context(), fixture.db, 1, 2, unboundedFinalityCtx, false))
	fixture.agg.WaitForFiles()
	require.Error(t, integrity.CheckCommitmentRoot(t.Context(), fixture.db, fixture.blockReader(t), true, log.New()))
}

func TestCheckCommitmentRootRejectsCorruptedBinRow(t *testing.T) {
	fixture := newPBinIntegrityFixture(t)
	corruptPBinIntegrityRow(t, fixture, false)
	require.Error(t, integrity.CheckCommitmentRoot(t.Context(), fixture.db, fixture.blockReader(t), true, log.New()))
}

func TestCheckCommitmentRootRejectsCorruptedBinRoot(t *testing.T) {
	fixture := newPBinIntegrityFixture(t)
	corruptPBinIntegrityRow(t, fixture, true)
	require.Error(t, integrity.CheckCommitmentRoot(t.Context(), fixture.db, fixture.blockReader(t), true, log.New()))
}

func TestCheckCommitmentRootAcceptsZeroBinRoot(t *testing.T) {
	fixture := newPBinIntegrityFixture(t)
	tx, err := fixture.db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	writer, err := state.NewPBinRangeWriter(fixture.agg, kv.CommitmentBinDomain, 7)
	require.NoError(t, err)
	_, err = writer.WriteAtBlock(t.Context(), tx, domains, func(func(state.PBinLeaf) error) error { return nil }, 1)
	require.NoError(t, err)
	domains.Close()
	tx.Rollback()
	require.NoError(t, integrity.CheckCommitmentRoot(t.Context(), fixture.db, fixture.blockReader(t), true, log.New()))
}

type pbinIntegrityFixture struct {
	db  kv.TemporalRwDB
	agg *state.Aggregator
}

func newPBinIntegrityFixture(t *testing.T) pbinIntegrityFixture {
	selectPBinIntegritySuite(t)
	db, agg := commitmenttemporal.Open(t, 8)
	probeTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer probeTx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), probeTx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	address := bytes.Repeat([]byte{0x11}, length.Addr)
	account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, probeTx, address, accounts.SerialiseV3(&account), 1, nil))
	slot := append(bytes.Clone(address), bytes.Repeat([]byte{0x22}, length.Hash)...)
	require.NoError(t, domains.DomainPut(kv.StorageDomain, probeTx, slot, []byte{1}, 1, nil))
	updates := commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	updates.TouchPlainKey(string(address), nil, func(*commitment.KeyUpdate, []byte) {})
	updates.TouchPlainKey(string(slot), nil, func(*commitment.KeyUpdate, []byte) {})
	hexCtx := domains.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	hexCtx.SetUpdates(updates)
	hexRoot, err := hexCtx.ComputeCommitment(t.Context(), probeTx, false, 1, 7, "integrity-bin-test", nil)
	require.NoError(t, err)
	domains.Close()
	probeTx.Rollback()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err = execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), 1, nil))
	require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, slot, []byte{1}, 1, nil))
	hexState, err := commitment.EncodeCommitmentV3State(hexRoot, 1, 7, nil)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, hexState, 7, nil))
	updates = commitment.NewUpdates(commitment.ModeCollect, "", commitment.KeyToHexNibbleHash)
	updates.TouchPlainKey(string(address), nil, func(*commitment.KeyUpdate, []byte) {})
	updates.TouchPlainKey(string(slot), nil, func(*commitment.KeyUpdate, []byte) {})
	hexCtx = domains.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	hexCtx.SetUpdates(updates)
	computedRoot, err := hexCtx.ComputeCommitment(t.Context(), tx, false, 1, 7, "integrity-bin-test", nil)
	require.NoError(t, err)
	require.Equal(t, hexRoot, computedRoot)
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 0))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 7))
	require.NoError(t, tx.Commit())
	domains.Close()
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentBinDomain,
		BlockNum:         1,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	targetTx.Rollback()
	sourceTx.Rollback()
	return pbinIntegrityFixture{db: db, agg: agg}
}

func (f pbinIntegrityFixture) blockReader(t *testing.T) dbservices.FullBlockReader {
	t.Helper()
	at := f.agg.BeginFilesRo()
	defer at.Close()
	value, found, _, _, err := at.DebugGetLatestFromFiles(kv.CommitmentDomain, commitment.KeyCommitmentV3State, ^uint64(0))
	require.NoError(t, err)
	require.True(t, found)
	root, _, _, err := integrity.ExtractCommitmentStateRoot(commitment.KeyCommitmentV3State, value)
	require.NoError(t, err)
	return pbinIntegrityBlockReader{root: common.BytesToHash(root)}
}

func corruptPBinIntegrityRow(t *testing.T, fixture pbinIntegrityFixture, root bool) {
	t.Helper()
	at := fixture.agg.BeginFilesRo()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentBinDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	var rowKey, rowValue []byte
	for iter.HasNext() {
		key, value, nextErr := iter.Next()
		require.NoError(t, nextErr)
		if root == bytes.Equal(key, []byte{0x08}) || !root && !commitment.IsCommitmentStateKey(key) {
			rowKey, rowValue = bytes.Clone(key), bytes.Clone(value)
			break
		}
	}
	iter.Close()
	at.Close()
	require.NotEmpty(t, rowKey)
	tx, err := fixture.db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	previous, _, err := domains.GetLatest(kv.CommitmentBinDomain, tx, rowKey)
	require.NoError(t, err)
	corrupted := bytes.Clone(rowValue)
	corrupted[len(corrupted)-1] ^= 1
	require.NoError(t, domains.DomainPut(kv.CommitmentBinDomain, tx, rowKey, corrupted, 9, previous))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	domains.Close()
}

func selectPBinIntegritySuite(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

type pbinIntegrityBlockReader struct {
	dbservices.FullBlockReader
	root common.Hash
}

func (r pbinIntegrityBlockReader) HeaderByNumber(context.Context, kv.Getter, uint64) (*types.Header, error) {
	return &types.Header{Root: r.root}, nil
}

func (r pbinIntegrityBlockReader) TxnumReader() rawdbv3.TxNumsReader {
	return rawdbv3.TxNums
}
