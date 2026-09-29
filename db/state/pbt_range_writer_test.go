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
	"math"
	"path/filepath"
	"sort"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
	commitmenttemporal "github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestNewPBinRangeWriter(t *testing.T) {
	previous := statecfg.ExperimentalHexBinCommitment
	t.Cleanup(func() { statecfg.ExperimentalHexBinCommitment = previous })
	statecfg.ExperimentalHexBinCommitment = true
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()

	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 24)
	require.NoError(t, err)
	require.NotNil(t, writer)
}

func TestPBinRangeWriterStreamsLeavesIntoBinFiles(t *testing.T) {
	selectPBinRangeWriterHash(t)
	statecfg.ExperimentalHexBinCommitment = true
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 24)
	require.NoError(t, err)
	var leaves []state.PBinLeaf
	root, err := writer.Write(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		return state.ForEachPBinLeaf(at, nil, true, func(leaf state.PBinLeaf) error {
			leaves = append(leaves, leaf)
			return emit(leaf)
		})
	})
	require.NoError(t, err)
	entries := make([]eip8297.Entry, 0, len(leaves))
	for _, leaf := range leaves {
		entries = append(entries, eip8297.Entry{Key: leaf.Key, Value: leaf.Value})
	}
	require.Equal(t, eip8297.StateRootWithHash(entries, eip8297.SelectedHash()), root)
	out := agg.BeginFilesRo()
	t.Cleanup(out.Close)
	binFiles := out.Files(kv.CommitmentBinDomain)
	require.Len(t, binFiles, 3)
	stateValue, found, start, end, err := out.DebugGetLatestFromFiles(kv.CommitmentBinDomain, commitment.KeyCommitmentState, math.MaxUint64)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, stateValue)
	require.Equal(t, binFiles[len(binFiles)-1].StartRootNum(), start)
	require.Equal(t, uint64(24), end)
}

func TestPBinRangeWriterWritesEmptyTargetRange(t *testing.T) {
	selectPBinRangeWriterHash(t)
	statecfg.ExperimentalHexBinCommitment = false
	dirs := datadir.New(t.TempDir())
	raw := mdbxtest.InMem(t, mdbx.New(dbcfg.ChainDB, log.New()), dirs.Chaindata).
		GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	t.Cleanup(raw.Close)
	sourceAgg := state.NewTest(dirs).StepSize(8).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, sourceAgg.OpenFolder(raw))
	sourceDB, err := dbtemporal.New(raw, sourceAgg, nil)
	require.NoError(t, err)
	writePBinRangeWriterAccounts(t, sourceDB, 32)
	require.NoError(t, sourceAgg.BuildFiles2(t.Context(), sourceDB, 0, 3, unboundedFinalityCtx, false))
	sourceAgg.WaitForFiles()
	sourceAgg.Close()

	statecfg.ExperimentalHexBinCommitment = true
	targetAgg := state.NewTest(dirs).StepSize(8).Logger(log.New()).MustOpen(t.Context())
	t.Cleanup(targetAgg.Close)
	require.NoError(t, targetAgg.OpenFolder(raw))
	targetDB, err := dbtemporal.New(raw, targetAgg, nil)
	require.NoError(t, err)
	t.Cleanup(targetDB.Close)
	tx, err := targetDB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	writer, err := state.NewPBinRangeWriter(targetAgg, kv.CommitmentBinDomain, 24)
	require.NoError(t, err)
	entries := eip8297.EmbedState([][]eip8297.State{{
		{Address: bytes.Repeat([]byte{0x11}, length.Addr), Nonce: 1, Balance: *uint256.NewInt(1), Code: []byte{0x60, 0x01, 0x60, 0x00, 0x52}, Slots: map[string][]byte{string(bytes.Repeat([]byte{0x22}, 32)): {1}}},
	}})
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	_, err = writer.Write(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		for _, entry := range entries {
			if err := emit(state.PBinLeaf{Key: entry.Key, Value: entry.Value}); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	out := targetAgg.BeginFilesRo()
	defer out.Close()
	files := out.Files(kv.CommitmentBinDomain)
	require.Len(t, files, 3)
	emptyRows := 0
	iter, err := out.DebugRangeLatestFromFiles(kv.CommitmentBinDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer iter.Close()
	for iter.HasNext() {
		key, _, err := iter.Next()
		require.NoError(t, err)
		_, found, start, _, err := out.DebugGetLatestFromFiles(kv.CommitmentBinDomain, key, math.MaxUint64)
		require.NoError(t, err)
		require.True(t, found)
		if start == 8 {
			emptyRows++
		}
	}
	require.Zero(t, emptyRows)
}

func TestPBinRangeWriterStampsRowsByMaximumLeafAndKeepsEmptyRanges(t *testing.T) {
	selectPBinRangeWriterHash(t)
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	at := agg.BeginFilesRo()
	t.Cleanup(at.Close)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 24)
	require.NoError(t, err)
	entries := eip8297.EmbedState([][]eip8297.State{{
		{Address: bytes.Repeat([]byte{0x11}, length.Addr), Nonce: 1, Balance: *uint256.NewInt(1), Code: []byte{0x60, 0x01, 0x60, 0x00, 0x52}, Slots: map[string][]byte{string(bytes.Repeat([]byte{0x22}, 32)): {1}, string(bytes.Repeat([]byte{0x23}, 32)): {2}}},
	}})
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	var leaves []state.PBinLeaf
	for i, entry := range entries {
		stamp := uint64(16)
		if i == len(entries)-1 {
			stamp = 0
		}
		leaves = append(leaves, state.PBinLeaf{Key: entry.Key, Value: entry.Value, Stamp: stamp})
	}
	root, err := writer.Write(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		for _, leaf := range leaves {
			if err := emit(leaf); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, eip8297.StateRootWithHash(entries, eip8297.SelectedHash()), root)
	out := agg.BeginFilesRo()
	t.Cleanup(out.Close)
	files := out.Files(kv.CommitmentBinDomain)
	iter, err := out.DebugRangeLatestFromFiles(kv.CommitmentBinDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer iter.Close()
	rowCount := 0
	stateCount := 0
	for iter.HasNext() {
		key, _, err := iter.Next()
		require.NoError(t, err)
		value, found, start, end, err := out.DebugGetLatestFromFiles(kv.CommitmentBinDomain, key, math.MaxUint64)
		require.NoError(t, err)
		require.True(t, found)
		require.NotEmpty(t, value)
		if commitment.IsCommitmentStateKey(key) {
			stateCount++
			require.Equal(t, uint64(16), start)
			require.Equal(t, uint64(24), end)
			continue
		}
		wantStamp := uint64(0)
		if bytes.Equal(key, pbt.GlobalRootKey()) {
			for _, leaf := range leaves {
				wantStamp = max(wantStamp, leaf.Stamp)
			}
			require.Equal(t, uint64(16), start, "row created after leaves were folded")
		} else {
			path, err := eip8297.DecodeBitPath(key)
			require.NoError(t, err)
			for _, leaf := range leaves {
				leafPath := eip8297.PathFromBits(leaf.Key, int16(len(leaf.Key)*8))
				if leafPath.HasPrefix(&path) && leaf.Stamp > wantStamp {
					wantStamp = leaf.Stamp
				}
			}
		}
		require.Equal(t, (wantStamp/8)*8, start)
		require.Equal(t, start+8, end)
		rowCount++
	}
	require.NotZero(t, rowCount)
	require.Equal(t, 1, stateCount)
	require.Len(t, files, 3)
	for _, file := range files {
		require.Equal(t, ".kv", filepath.Ext(file.Fullpath()))
	}
	require.Equal(t, uint64(8), files[1].StartRootNum())
	require.Equal(t, uint64(16), files[1].EndRootNum())

	before := pbinRangeWriterRows(t, out)
	out.Close()
	previousNoMerge := dbg.NoMerge()
	t.Cleanup(func() { dbg.SetNoMerge(previousNoMerge) })
	dbg.SetNoMerge(false)
	require.NoError(t, agg.MergeLoop(t.Context()))
	agg.WaitForFiles()
	after := agg.BeginFilesRo()
	defer after.Close()
	require.Equal(t, before, pbinRangeWriterRows(t, after))
	require.Less(t, len(after.Files(kv.CommitmentBinDomain)), len(files))
}

func pbinRangeWriterRows(t *testing.T, at *state.AggregatorRoTx) map[string][]byte {
	t.Helper()
	iter, err := at.DebugRangeLatestFromFiles(kv.CommitmentBinDomain, nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer iter.Close()
	rows := make(map[string][]byte)
	for iter.HasNext() {
		key, value, err := iter.Next()
		require.NoError(t, err)
		rows[string(key)] = bytes.Clone(value)
	}
	return rows
}

func selectPBinRangeWriterHash(t *testing.T) {
	previousPBin := commitment.PBinHashSuiteName()
	previousEIP := eip8297.HashSuiteName()
	previousDual := statecfg.ExperimentalHexBinCommitment
	previousBinHash := statecfg.BinCommitmentHash
	t.Cleanup(func() {
		require.NoError(t, commitment.SetPBinHashSuite(previousPBin))
		require.NoError(t, eip8297.SetHashSuite(previousEIP))
		statecfg.ExperimentalHexBinCommitment = previousDual
		statecfg.BinCommitmentHash = previousBinHash
	})
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	statecfg.ExperimentalHexBinCommitment = true
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
}

func writePBinRangeWriterAccounts(t *testing.T, db kv.TemporalRwDB, endTxNum uint64) {
	t.Helper()
	for i := uint64(0); i < endTxNum; i += 8 {
		tx, err := db.BeginTemporalRw(t.Context())
		require.NoError(t, err)
		cfg := commitment.DefaultTrieConfig()
		cfg.Variant = commitment.VariantCommitmentV3
		domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentDomain))
		require.NoError(t, err)
		address := bytes.Repeat([]byte{byte(i/8 + 1)}, length.Addr)
		account := accounts.Account{Nonce: i/8 + 1, Balance: *uint256.NewInt(i/8 + 1), CodeHash: accounts.EmptyCodeHash}
		previous, _, err := domains.GetLatest(kv.AccountsDomain, tx, address)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), i, previous))
		require.NoError(t, domains.Flush(t.Context(), tx))
		domains.Close()
		require.NoError(t, tx.Commit())
	}
}
