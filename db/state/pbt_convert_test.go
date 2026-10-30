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
	"context"
	"encoding/binary"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestConvertPBinMatchesReferenceRoot(t *testing.T) {
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
		EndTxNum:         7,
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
		EndTxNum:         7,
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
	_, verifyErr := state.VerifyPBinDomainRoot(t.Context(), verifyTx, agg, kv.CommitmentDomain)
	require.Error(t, verifyErr)
}

func TestVerifyPBinDomainAcceptsBinOnlyConversion(t *testing.T) {
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
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg,
		SourceTx:         sourceTx,
		TargetAggregator: agg,
		TargetTx:         targetTx,
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	targetTx.Rollback()
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	_, verifyErr := state.VerifyPBinDomainRoot(t.Context(), verifyTx, agg, kv.CommitmentDomain)
	require.NoError(t, verifyErr)
}

func TestVerifyPBinDomainReportsProgress(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
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
		TargetDomain:     kv.CommitmentDomain,
		EndTxNum:         7,
		Hash:             eip8297.HashBytes,
	})
	require.NoError(t, err)
	targetTx.Rollback()
	verifyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer verifyTx.Rollback()
	previousHandler := log.Root().GetHandler()
	var logs bytes.Buffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previousHandler) })
	_, err = state.VerifyPBinDomainRoot(t.Context(), verifyTx, agg, kv.CommitmentDomain)
	require.NoError(t, err)
	require.Contains(t, logs.String(), "PBT verification progress")
}

func selectPBinConvertHash(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func selectPBinBinOnlyHash(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func writePBinConvertState(t *testing.T, db kv.TemporalRwDB) {
	writePBinConvertStateN(t, db, 2)
}

func writePBinConvertStateManyStorageSlots(t *testing.T, db kv.TemporalRwDB) {
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
	address := make([]byte, length.Addr)
	account := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
	previous, _, err := domains.GetLatest(kv.AccountsDomain, tx, address)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), 1, previous))
	slotKey := func(slotIndex uint64) []byte {
		slot := make([]byte, length.Hash)
		binary.BigEndian.PutUint64(slot[length.Hash-8:], slotIndex)
		return slot
	}
	firstGroup := eip8297.TreeKeyStorage(address, slotKey(eip8297.HeaderStorageSlots))[1+length.Hash] >> 4
	secondGroup := uint64(1)
	for ; eip8297.TreeKeyStorage(address, slotKey(secondGroup*256+eip8297.HeaderStorageSlots))[1+length.Hash]>>4 == firstGroup; secondGroup++ {
	}
	put := func(slotIndex uint64) {
		slot := append(bytes.Clone(address), slotKey(slotIndex)...)
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, slot, []byte{byte(slotIndex)}, 1, nil))
	}
	for slotIndex := uint64(eip8297.HeaderStorageSlots); slotIndex < 256; slotIndex++ {
		put(slotIndex)
	}
	for slotIndex := secondGroup*256 + eip8297.HeaderStorageSlots; slotIndex < secondGroup*256+eip8297.HeaderStorageSlots+4; slotIndex++ {
		put(slotIndex)
	}
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}

func writePBinConvertStateN(t *testing.T, db kv.TemporalRwDB, count int) {
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
	for i := 1; i <= count; i++ {
		address := make([]byte, length.Addr)
		binary.BigEndian.PutUint64(address[length.Addr-8:], uint64(i))
		account := accounts.Account{Nonce: uint64(i), Balance: *uint256.NewInt(uint64(i)), CodeHash: accounts.EmptyCodeHash}
		previous, _, err := domains.GetLatest(kv.AccountsDomain, tx, address)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), 1, previous))
		slot := append(bytes.Clone(address), make([]byte, 32)...)
		binary.BigEndian.PutUint64(slot[length.Addr+24:], uint64(i))
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, slot, []byte{byte(i)}, 1, nil))
	}
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
}

type pbinVerifyPinTx struct {
	kv.TemporalTx
	begun, closed *atomic.Int32
}

func (tx *pbinVerifyPinTx) UnderlyingTx() kv.TemporalTx { return tx.TemporalTx }

func (tx *pbinVerifyPinTx) Pin() kv.TemporalFilesPin {
	pin := tx.TemporalTx.(interface{ Pin() kv.TemporalFilesPin }).Pin()
	if pin == nil {
		return nil
	}
	return &pbinVerifyPin{TemporalFilesPin: pin, begun: tx.begun, closed: tx.closed}
}

type pbinVerifyPin struct {
	kv.TemporalFilesPin
	begun, closed *atomic.Int32
}

func (p *pbinVerifyPin) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	tx, err := p.TemporalFilesPin.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, err
	}
	p.begun.Add(1)
	return &pbinVerifyWorkerTx{TemporalTx: tx, closed: p.closed}, nil
}

type pbinVerifyWorkerTx struct {
	kv.TemporalTx
	closed *atomic.Int32
}

func (tx *pbinVerifyWorkerTx) Rollback() {
	tx.closed.Add(1)
	tx.TemporalTx.Rollback()
}

func verifyPBinDomainWith(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, workers int, writer bool) (error, int32) {
	t.Helper()
	previous := dbg.PBTWorkers
	dbg.PBTWorkers = workers
	defer func() { dbg.PBTWorkers = previous }()
	var tx kv.TemporalTx
	var err error
	if writer {
		tx, err = db.BeginTemporalRw(t.Context())
	} else {
		tx, err = db.BeginTemporalRo(t.Context())
	}
	require.NoError(t, err)
	defer tx.Rollback()
	var begun, closed atomic.Int32
	_, verifyErr := state.VerifyPBinDomainRoot(t.Context(), &pbinVerifyPinTx{TemporalTx: tx, begun: &begun, closed: &closed}, agg, kv.CommitmentDomain)
	require.Equal(t, begun.Load(), closed.Load(), "worker transactions left open")
	return verifyErr, begun.Load()
}

func TestVerifyPBinDomainVerifiesChainsOnPinnedWorkerViews(t *testing.T) {
	selectPBinBinOnlyHash(t)
	db, agg := temporal.Open(t, 8)
	writePBinConvertStateN(t, db, 1024)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 1, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	sourceTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer sourceTx.Rollback()
	targetTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer targetTx.Rollback()
	_, err = state.ConvertPBin(t.Context(), state.PBinConvertOptions{
		SourceAggregator: agg, SourceTx: sourceTx, TargetAggregator: agg, TargetTx: targetTx,
		TargetDomain: kv.CommitmentDomain, EndTxNum: 7, Hash: eip8297.HashBytes,
	})
	require.NoError(t, err)
	targetTx.Rollback()

	verifyErr, begun := verifyPBinDomainWith(t, db, agg, 4, false)
	require.NoError(t, verifyErr)
	require.Greater(t, begun, int32(1), "the verify opened no pinned worker views")
	verifyErr, begun = verifyPBinDomainWith(t, db, agg, 1, false)
	require.NoError(t, verifyErr)
	require.Zero(t, begun)
	verifyErr, begun = verifyPBinDomainWith(t, db, agg, 4, true)
	require.NoError(t, verifyErr)
	require.Zero(t, begun, "a write tx caller opened worker views that cannot see its rows")

	at := agg.BeginFilesRo()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	var rowKey, rowValue []byte
	for iter.HasNext() {
		key, value, nextErr := iter.Next()
		require.NoError(t, nextErr)
		path, decodeErr := eip8297.DecodeBitPath(key)
		if decodeErr == nil && path.BitLen > 12 && path.BitLen < 264 {
			rowKey, rowValue = bytes.Clone(key), bytes.Clone(value)
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
	previousValue, _, err := domains.GetLatest(kv.CommitmentDomain, badTx, rowKey)
	require.NoError(t, err)
	corrupted := bytes.Clone(rowValue)
	corrupted[len(corrupted)-1] ^= 1
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, badTx, rowKey, corrupted, 9, previousValue))
	require.NoError(t, domains.Flush(t.Context(), badTx))
	require.NoError(t, badTx.Commit())
	domains.Close()

	sequentialErr, _ := verifyPBinDomainWith(t, db, agg, 1, false)
	require.Error(t, sequentialErr)
	parallelErr, begun := verifyPBinDomainWith(t, db, agg, 4, false)
	require.Greater(t, begun, int32(1))
	require.EqualError(t, parallelErr, sequentialErr.Error())
}
