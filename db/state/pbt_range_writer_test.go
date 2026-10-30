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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/internal/commitmenttest"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
	commitmenttemporal "github.com/erigontech/erigon/internal/commitmenttest/temporal"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestNewPBinRangeWriter(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalHexBinCommitment = true
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 22)
	require.NoError(t, err)
	require.NotNil(t, writer)
}

func TestNewPBinRangeWriterExtendsPastStateFrontier(t *testing.T) {
	commitmentflags.Restore(t)
	statecfg.ExperimentalHexBinCommitment = true
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 39)
	require.NoError(t, err, "import must create an empty range after the state-file frontier")
	require.NotNil(t, writer)
}

func TestPBinRangeWriterWithinFilesKeepsUnalignedStateOutsideFiles(t *testing.T) {
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
	writer, err := state.NewPBinRangeWriterWithinFiles(agg, kv.CommitmentBinDomain, 39)
	require.NoError(t, err)
	root, err := writer.WriteAtBlock(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		return state.ForEachPBinLeaf(at, nil, true, emit)
	}, 0)
	require.NoError(t, err)
	require.NotEqual(t, eip8297.EmptyTreeHash, root)
	require.False(t, writer.PBinCommitmentStateInFiles())
	out := agg.BeginFilesRo()
	defer out.Close()
	stateValue, found, _, _, err := out.DebugGetLatestFromFiles(kv.CommitmentBinDomain, commitment.KeyCommitmentState, math.MaxUint64)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, stateValue)
	require.Len(t, out.Files(kv.CommitmentBinDomain), 3)
	files, err := filepath.Glob(filepath.Join(agg.Dirs().SnapDomain, "*-commitment-bin.*.kv"))
	require.NoError(t, err)
	require.Len(t, files, 3)
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
	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 23)
	require.NoError(t, err)
	var leaves []state.PBinLeaf
	root, err := writer.WriteAtBlock(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		return state.ForEachPBinLeaf(at, nil, true, func(leaf state.PBinLeaf) error {
			leaves = append(leaves, leaf)
			return emit(leaf)
		})
	}, 0)
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

func TestPBinRangeWriterStreamsLeavesIntoBinFilesWithTinyBatches(t *testing.T) {
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
	writer, err := state.NewPBinRangeWriterWithLimitsForTest(agg, kv.CommitmentBinDomain, 23, 2, 1<<20)
	require.NoError(t, err)
	root, err := writer.WriteAtBlock(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		return state.ForEachPBinLeaf(at, nil, true, emit)
	}, 0)
	require.NoError(t, err)
	require.NotEqual(t, eip8297.EmptyTreeHash, root)
	out := agg.BeginFilesRo()
	defer out.Close()
	stateValue, found, _, _, err := out.DebugGetLatestFromFiles(kv.CommitmentBinDomain, commitment.KeyCommitmentState, math.MaxUint64)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, stateValue)
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
	writer, err := state.NewPBinRangeWriter(targetAgg, kv.CommitmentBinDomain, 23)
	require.NoError(t, err)
	entries := eip8297.EmbedState([][]eip8297.State{{
		{Address: bytes.Repeat([]byte{0x11}, length.Addr), Nonce: 1, Balance: *uint256.NewInt(1), Code: []byte{0x60, 0x01, 0x60, 0x00, 0x52}, Slots: map[string][]byte{string(bytes.Repeat([]byte{0x22}, 32)): {1}}},
	}})
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	_, err = writer.WriteAtBlock(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		for _, entry := range entries {
			if emitErr := emit(state.PBinLeaf{Key: entry.Key, Value: entry.Value}); emitErr != nil {
				return emitErr
			}
		}
		return nil
	}, 0)
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

func TestPBinRangeWriterWritesEmptyStateAtNewestRange(t *testing.T) {
	selectPBinRangeWriterHash(t)
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	defer domains.Close()
	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 23)
	require.NoError(t, err)
	root, err := writer.WriteAtBlock(t.Context(), tx, domains, func(func(state.PBinLeaf) error) error {
		return nil
	}, 0)
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, root)
	out := agg.BeginFilesRo()
	defer out.Close()
	stateValue, found, start, end, err := out.DebugGetLatestFromFiles(kv.CommitmentBinDomain, commitment.KeyCommitmentState, math.MaxUint64)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(16), start)
	require.Equal(t, uint64(24), end)
	txNum, blockNum := commitmentdb.DecodeTxBlockNums(stateValue)
	require.Equal(t, uint64(23), txNum)
	require.Zero(t, blockNum)
}

func TestPBinRangeWriterReleasesFinishedRows(t *testing.T) {
	selectPBinRangeWriterHash(t)
	counts := make([]int, 0, 2)
	for _, accountCount := range []byte{8, 80} {
		func() {
			db, agg := commitmenttemporal.Open(t, 8)
			writePBinRangeWriterAccounts(t, db, 32)
			require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
			agg.WaitForFiles()
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			cfg := commitment.DefaultTrieConfig()
			cfg.Variant = commitment.VariantCommitmentV3
			cfg.EnableTrieWarmup = false
			domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
			require.NoError(t, err)
			writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 23)
			require.NoError(t, err)
			entries := make([]eip8297.Entry, 0, int(accountCount))
			for i := byte(1); i <= accountCount; i++ {
				entries = append(entries, eip8297.EmbedState([][]eip8297.State{{{
					Address: bytes.Repeat([]byte{i}, length.Addr),
					Nonce:   uint64(i),
					Balance: *uint256.NewInt(uint64(i)),
				}}})...)
			}
			sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
			_, err = writer.WriteAtBlock(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
				for _, entry := range entries {
					if emitErr := emit(state.PBinLeaf{Key: entry.Key, Value: entry.Value}); emitErr != nil {
						return emitErr
					}
				}
				return nil
			}, 0)
			require.NoError(t, err)
			counts = append(counts, domains.GetMemBatch().(*state.TemporalMemBatch).DomainLen(kv.CommitmentBinDomain))
			domains.Close()
			tx.Rollback()
			agg.Close()
			db.Close()
		}()
	}
	require.Equal(t, counts[0], counts[1])
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
	writer, err := state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, 23)
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
	root, err := writer.WriteAtBlock(t.Context(), tx, domains, func(emit func(state.PBinLeaf) error) error {
		for _, leaf := range leaves {
			if emitErr := emit(leaf); emitErr != nil {
				return emitErr
			}
		}
		return nil
	}, 0)
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

func TestPBinRangeWriterBatchSizeDoesNotChangeRows(t *testing.T) {
	selectPBinRangeWriterHash(t)
	for _, corpus := range pbinRangeWriterCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			var want pbinRangeWriterSnapshot
			for _, maxOps := range []int{100000, 1000, 7, 1} {
				got := runPBinRangeWriterCorpus(t, corpus.leaves, maxOps)
				if want.state == nil {
					want = got
					continue
				}
				require.Equal(t, want, got, "maxOps=%d", maxOps)
			}
		})
	}
}

func TestPBinRangeWriterRecordsMatchSequentialTrie(t *testing.T) {
	selectPBinRangeWriterHash(t)
	for _, corpus := range pbinRangeWriterCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			ops := make([]pbt.Op, len(corpus.leaves))
			for i, leaf := range corpus.leaves {
				var value [eip8297.ValueLength]byte
				copy(value[:], leaf.Value)
				ops[i] = pbt.Op{Key: bytes.Clone(leaf.Key), Value: value}
			}
			wantContext := newPBinSequentialContext()
			wantRoot, err := pbt.NewTrie(wantContext).Process(ops)
			require.NoError(t, err)
			wantTrieState, err := pbt.NewTrie(wantContext).EncodeCurrentState(nil)
			require.NoError(t, err)
			wantState, err := commitmentdb.NewCommitmentState(23, 0, wantTrieState).Encode()
			require.NoError(t, err)
			got := runPBinRangeWriterCorpus(t, corpus.leaves, 7)
			gotRecords := make(map[string][]byte)
			for _, file := range got.files {
				for _, entry := range file.entries {
					if bytes.Equal(entry.key, commitment.KeyCommitmentState) {
						continue
					}
					require.NotContains(t, gotRecords, string(entry.key), "file=[%d,%d)", file.start, file.end)
					gotRecords[string(entry.key)] = entry.value
				}
			}
			require.Equal(t, wantRoot, got.root)
			require.Equal(t, wantContext.Records, gotRecords)
			require.Equal(t, wantState, got.state)
			if corpus.name == "bucket-heavy" {
				bucket := eip8297.PathFromBits(pbinRangeWriterSplit271Prefix(), 264)
				bucketKey, err := pbt.EncodeRowKey(&bucket)
				require.NoError(t, err)
				record, err := pbt.DecodeRecord(bucketKey, wantContext.Records[string(bucketKey)])
				require.NoError(t, err)
				require.Equal(t, pbt.ExtRoot, record.Form)
				require.Equal(t, int16(271-264), record.SelfExt.BitLen)
			}
		})
	}
}

func TestPBinRangeWriterRowsLandInLeafStampRange(t *testing.T) {
	selectPBinRangeWriterHash(t)
	for _, corpus := range pbinRangeWriterCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			for _, maxOps := range []int{100000, 7, 1} {
				requirePBinRowsInLeafStampRanges(t, corpus.leaves, runPBinRangeWriterCorpus(t, corpus.leaves, maxOps), fmt.Sprintf("maxOps=%d", maxOps))
			}
		})
	}
}

func TestPBinRangeWriterPastFrontierRange(t *testing.T) {
	selectPBinRangeWriterHash(t)
	var leaves []state.PBinLeaf
	for bucket, stamp := range []uint64{0, 33} {
		prefix := append([]byte{eip8297.StorageZone}, bytes.Repeat([]byte{byte(0x30 + bucket)}, 32)...)
		for slot := range byte(3) {
			key := make([]byte, eip8297.StorageKeyLength)
			copy(key, prefix)
			key[33] = slot << 4
			key[len(key)-1] = slot + 1
			leaves = append(leaves, state.PBinLeaf{Key: key, Value: pbinRangeWriterValue(byte(bucket*3) + slot + 1), Stamp: stamp})
		}
	}
	sort.Slice(leaves, func(i, j int) bool { return bytes.Compare(leaves[i].Key, leaves[j].Key) < 0 })
	for _, maxOps := range []int{100000, 2} {
		label := fmt.Sprintf("maxOps=%d", maxOps)
		run := openPBinRangeWriterRun(t, maxOps, 0, 39)
		started := time.Now()
		_, err := run.write(t.Context(), leaves)
		require.NoError(t, err, label)
		got := readPBinRangeWriterFilesOnDisk(t, run.agg.Dirs().SnapDomain, run.agg.StepSize(), started)
		ranges := make([][2]uint64, 0, len(got.files))
		entries := make([]int, 0, len(got.files))
		for _, file := range got.files {
			ranges = append(ranges, [2]uint64{file.start, file.end})
			entries = append(entries, len(file.entries))
		}
		require.Equal(t, [][2]uint64{{0, 8}, {8, 16}, {16, 24}, {24, 40}}, ranges, label)
		requirePBinRowsInLeafStampRanges(t, leaves, got, label)
		require.Equal(t, []int{entries[0], 0, 0, entries[3]}, entries, "%s: rows in the ranges no leaf stamp selects", label)
		require.Positive(t, entries[0], label)
		require.Positive(t, entries[3], label)
	}
}

func readPBinRangeWriterFilesOnDisk(t *testing.T, snapDomain string, stepSize uint64, writtenAfter time.Time) pbinRangeWriterSnapshot {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(snapDomain, "*-"+kv.CommitmentBinDomain.String()+".*.kv"))
	require.NoError(t, err)
	var snapshot pbinRangeWriterSnapshot
	for _, name := range names {
		var from, to uint64
		base := filepath.Base(name)
		_, err := fmt.Sscanf(base[strings.Index(base, kv.CommitmentBinDomain.String())+len(kv.CommitmentBinDomain.String()):], ".%d-%d.kv", &from, &to)
		require.NoError(t, err, base)
		info, err := os.Stat(name)
		require.NoError(t, err)
		require.False(t, info.ModTime().Before(writtenAfter), "%s was not written by this run", base)
		file := pbinRangeWriterFile{start: from * stepSize, end: to * stepSize, entries: []pbinRangeWriterEntry{}}
		decompressor, err := seg.NewDecompressor(name)
		require.NoError(t, err)
		reader := seg.NewReader(decompressor.MakeGetter(), seg.CompressNone)
		for reader.HasNext() {
			key, _ := reader.Next(nil)
			require.True(t, reader.HasNext(), "key %x has no value in %s", key, name)
			value, _ := reader.Next(nil)
			file.entries = append(file.entries, pbinRangeWriterEntry{key: bytes.Clone(key), value: bytes.Clone(value)})
		}
		decompressor.Close()
		snapshot.files = append(snapshot.files, file)
	}
	sort.Slice(snapshot.files, func(i, j int) bool { return snapshot.files[i].start < snapshot.files[j].start })
	return snapshot
}

func requirePBinRowsInLeafStampRanges(t *testing.T, leaves []state.PBinLeaf, got pbinRangeWriterSnapshot, label string) {
	t.Helper()
	var maximum uint64
	for _, leaf := range leaves {
		maximum = max(maximum, leaf.Stamp)
	}
	expected := func(key []byte) uint64 {
		if bytes.Equal(key, pbt.GlobalRootKey()) {
			return maximum
		}
		path, err := eip8297.DecodeBitPath(key)
		require.NoError(t, err, "%x", key)
		var stamp uint64
		found := false
		for _, leaf := range leaves {
			leafPath := eip8297.PathFromBits(leaf.Key, int16(len(leaf.Key)*8))
			if eip8297.CommonPrefixBitsAt(&path, 0, &leafPath) == path.BitLen {
				stamp = max(stamp, leaf.Stamp)
				found = true
			}
		}
		require.True(t, found, "row %x covers no leaf", key)
		return stamp
	}
	for _, file := range got.files {
		for _, entry := range file.entries {
			if bytes.Equal(entry.key, commitment.KeyCommitmentState) {
				continue
			}
			stamp := expected(entry.key)
			require.True(t, stamp >= file.start && stamp < file.end, "%s: row %x with leaf stamp %d is in file [%d,%d)", label, entry.key, stamp, file.start, file.end)
		}
	}
}

func TestPBinRangeWriterFactoryMatchesSequentialEngine(t *testing.T) {
	selectPBinRangeWriterHash(t)
	for _, corpus := range pbinRangeWriterCorpora() {
		t.Run(corpus.name, func(t *testing.T) {
			want := runPBinRangeWriterCorpus(t, corpus.leaves, 100000)
			for _, maxOps := range []int{100000, 1000, 7, 1} {
				require.Equal(t, want, runPBinRangeWriterCorpusWith(t, corpus.leaves, maxOps, 4), "maxOps=%d", maxOps)
			}
		})
	}
}

func pbinRandomThreeZoneLeaves(r *rand.Rand) []state.PBinLeaf {
	var leaves []state.PBinLeaf
	stamp := func() uint64 { return uint64(r.Intn(24)) }
	for range 20 + r.Intn(40) {
		address := make([]byte, 20)
		r.Read(address)
		leaves = append(
			leaves,
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: pbinRangeWriterBasicValue(byte(r.Intn(250) + 1)), Stamp: stamp()},
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.HeaderStorageOffset), Value: pbinRangeWriterHeaderValue(byte(r.Intn(250) + 1)), Stamp: stamp()},
		)
		if r.Intn(2) == 0 {
			leaves = append(leaves, state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), Value: pbinRangeWriterCodeHashValue(byte(r.Intn(250) + 1)), Stamp: stamp()})
			var codeHash [32]byte
			r.Read(codeHash[:])
			for chunk := range 1 + r.Intn(3) {
				leaves = append(leaves, state.PBinLeaf{Key: eip8297.TreeKeyCodeChunk(common.Hash(codeHash), chunk), Value: pbinRangeWriterCodeValue(byte(r.Intn(250) + 1)), Stamp: stamp()})
			}
		}
		slots := 0
		switch r.Intn(4) {
		case 0:
			slots = 1
		case 1:
			slots = 2 + r.Intn(6)
		case 2:
			slots = 100 + r.Intn(200)
		}
		for range slots {
			slot := make([]byte, 32)
			r.Read(slot)
			leaves = append(leaves, state.PBinLeaf{Key: eip8297.TreeKeyStorage(address, slot), Value: pbinRangeWriterValue(byte(r.Intn(250) + 1)), Stamp: stamp()})
		}
	}
	sort.Slice(leaves, func(i, j int) bool { return bytes.Compare(leaves[i].Key, leaves[j].Key) < 0 })
	return slices.CompactFunc(leaves, func(a, b state.PBinLeaf) bool { return bytes.Equal(a.Key, b.Key) })
}

func TestPBinRangeWriterRowStampsMatchReferenceTracker(t *testing.T) {
	selectPBinRangeWriterHash(t)
	for seed := range int64(4) {
		leaves := pbinRandomThreeZoneLeaves(rand.New(rand.NewSource(seed)))
		for _, maxOps := range []int{100000, 1000, 7, 1} {
			for _, workers := range []int{0, 4} {
				t.Run(fmt.Sprintf("seed=%d/maxOps=%d/workers=%d", seed, maxOps, workers), func(t *testing.T) {
					runPBinRangeWriterCorpusWith(t, leaves, maxOps, workers)
				})
			}
		}
	}
}

func TestPBinRangeWriterFewerOpsThanPartitions(t *testing.T) {
	selectPBinRangeWriterHash(t)
	var leaves []state.PBinLeaf
	for nibble := range byte(16) {
		for i := range byte(3) {
			key := make([]byte, eip8297.StorageKeyLength)
			key[0] = eip8297.StorageZone
			key[1] = nibble<<4 | i
			key[33] = i << 4
			key[len(key)-1] = i + 1
			leaves = append(leaves, state.PBinLeaf{Key: key, Value: pbinRangeWriterValue(nibble*3 + i + 1), Stamp: uint64(nibble+i) % 24})
		}
	}
	for nibble := range byte(4) {
		for i := range byte(2) {
			stem := make([]byte, 32)
			stem[0] = nibble<<4 | i
			leaves = append(leaves, state.PBinLeaf{Key: eip8297.TreeKey(eip8297.CodeZone, stem, i), Value: pbinRangeWriterCodeValue(nibble*2 + i + 1), Stamp: uint64(nibble*5+i) % 24})
		}
	}
	sort.Slice(leaves, func(i, j int) bool { return bytes.Compare(leaves[i].Key, leaves[j].Key) < 0 })
	partitions := make(map[[2]byte]struct{})
	for _, leaf := range leaves {
		partitions[[2]byte{leaf.Key[0], leaf.Key[1] >> 4}] = struct{}{}
	}
	require.Len(t, partitions, 20)
	want := runPBinRangeWriterCorpus(t, leaves, 100000)
	for _, maxOps := range []int{3, 1} {
		require.Equal(t, want, runPBinRangeWriterCorpus(t, leaves, maxOps), "maxOps=%d", maxOps)
	}
}

type pbinSequentialContext struct {
	commitmenttest.MapBranchStore
}

func newPBinSequentialContext() *pbinSequentialContext {
	return &pbinSequentialContext{MapBranchStore: commitmenttest.NewMapBranchStore()}
}

func (c *pbinSequentialContext) Account([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected account read")
}

func (c *pbinSequentialContext) Storage([]byte) (*commitment.Update, error) {
	return nil, fmt.Errorf("unexpected storage read")
}

var _ commitment.PatriciaContext = (*pbinSequentialContext)(nil)

type pbinRangeWriterCorpus struct {
	name   string
	leaves []state.PBinLeaf
}

type pbinRangeWriterSnapshot struct {
	files []pbinRangeWriterFile
	state []byte
	root  common.Hash
}

type pbinRangeWriterFile struct {
	start   uint64
	end     uint64
	entries []pbinRangeWriterEntry
}

type pbinRangeWriterEntry struct {
	key   []byte
	value []byte
}

func readPBinRangeWriterSnapshot(t testing.TB, agg *state.Aggregator, root common.Hash) pbinRangeWriterSnapshot {
	t.Helper()
	out := agg.BeginFilesRo()
	defer out.Close()
	snapshot := pbinRangeWriterSnapshot{root: root}
	for _, visible := range out.Files(kv.CommitmentBinDomain) {
		if !strings.HasSuffix(visible.Fullpath(), ".kv") {
			continue
		}
		file := pbinRangeWriterFile{start: visible.StartRootNum(), end: visible.EndRootNum(), entries: []pbinRangeWriterEntry{}}
		decompressor, err := seg.NewDecompressor(visible.Fullpath())
		require.NoError(t, err)
		reader := seg.NewReader(decompressor.MakeGetter(), seg.CompressNone)
		for reader.HasNext() {
			key, _ := reader.Next(nil)
			require.True(t, reader.HasNext(), "key %x has no value in %s", key, visible.Fullpath())
			value, _ := reader.Next(nil)
			if bytes.Equal(key, commitment.KeyCommitmentState) {
				require.Nil(t, snapshot.state, "second state record in %s", visible.Fullpath())
				snapshot.state = bytes.Clone(value)
			}
			file.entries = append(file.entries, pbinRangeWriterEntry{key: bytes.Clone(key), value: bytes.Clone(value)})
		}
		decompressor.Close()
		snapshot.files = append(snapshot.files, file)
	}
	require.NotEmpty(t, snapshot.files)
	require.NotNil(t, snapshot.state)
	return snapshot
}

func runPBinRangeWriterCorpus(t *testing.T, leaves []state.PBinLeaf, maxOps int) pbinRangeWriterSnapshot {
	t.Helper()
	return runPBinRangeWriterCorpusWith(t, leaves, maxOps, 0)
}

type pbinRangeWriterRun struct {
	db       kv.TemporalRwDB
	agg      *state.Aggregator
	tx       kv.TemporalRwTx
	domains  *execctx.SharedDomains
	writer   *state.PBinRangeWriter
	activity *atomic.Int32
}

func openPBinRangeWriterRun(t testing.TB, maxOps, workers int, endTxNum uint64) *pbinRangeWriterRun {
	t.Helper()
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	options := []execctx.SharedDomainOption{execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek()}
	if workers > 0 {
		cfg.WarmupNumWorkers = workers
		options = append(options, execctx.WithTrieConfig(cfg), execctx.WithParaTrieDB(db))
	}
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), options...)
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	run := &pbinRangeWriterRun{db: db, agg: agg, tx: tx, domains: domains, activity: new(atomic.Int32)}
	if workers > 0 {
		activity, ok := domains.GetCommitmentCtx().Trie().(interface{ SetCoreActivityHook(func(bool)) })
		require.True(t, ok)
		activity.SetCoreActivityHook(func(active bool) {
			if active {
				run.activity.Add(1)
			}
		})
	}
	run.writer, err = state.NewPBinRangeWriterWithLimitsForTest(agg, kv.CommitmentBinDomain, endTxNum, maxOps, 1<<30)
	require.NoError(t, err)
	return run
}

func (r *pbinRangeWriterRun) write(ctx context.Context, leaves []state.PBinLeaf) (common.Hash, error) {
	return r.writer.WriteAtBlock(ctx, r.tx, r.domains, func(emit func(state.PBinLeaf) error) error {
		for _, leaf := range leaves {
			if emitErr := emit(leaf); emitErr != nil {
				return emitErr
			}
		}
		return nil
	}, 0)
}

func runPBinRangeWriterCorpusWith(t *testing.T, leaves []state.PBinLeaf, maxOps, workers int) pbinRangeWriterSnapshot {
	t.Helper()
	run := openPBinRangeWriterRun(t, maxOps, workers, 23)
	agg := run.agg
	phaseActivity := run.activity
	stampLookups := run.writer.CheckRowStampsForTest()
	root, err := run.write(t.Context(), leaves)
	require.NoError(t, err)
	require.Positive(t, stampLookups(), "no row stamp was looked up")
	pbinReviewRewritePersistedStateKey(t, agg)
	snapshot := readPBinRangeWriterSnapshot(t, agg, root)
	if workers > 0 && maxOps > 1 {
		require.Greater(t, phaseActivity.Load(), int32(1), "maxOps=%d: the parallel round did not run", maxOps)
	}
	return snapshot
}

func pbinRangeWriterCorpora() []pbinRangeWriterCorpus {
	threeZone := make([]state.PBinLeaf, 0, 128)
	for i := byte(1); i <= 24; i++ {
		address := bytes.Repeat([]byte{i}, 20)
		basic := pbinRangeWriterBasicValue(i)
		header := pbinRangeWriterHeaderValue(i)
		threeZone = append(
			threeZone,
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: basic, Stamp: uint64(i % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.HeaderStorageOffset), Value: header, Stamp: uint64((i + 3) % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyCodeChunk(common.BytesToHash(bytes.Repeat([]byte{i}, 32)), 0), Value: pbinRangeWriterCodeValue(i), Stamp: uint64((i + 6) % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyStorage(address, []byte{i}), Value: pbinRangeWriterValue(i + 72), Stamp: uint64((i + 9) % 24)},
		)
	}
	zonePure := make([]state.PBinLeaf, 0, 96)
	for i := byte(1); i <= 24; i++ {
		address := bytes.Repeat([]byte{i}, 20)
		zonePure = append(
			zonePure,
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: pbinRangeWriterBasicValue(i), Stamp: uint64(i % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.CodeHashLeafKey), Value: pbinRangeWriterCodeHashValue(i), Stamp: uint64((int(i) + 1) % 24)},
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.HeaderStorageOffset), Value: pbinRangeWriterHeaderValue(i), Stamp: uint64((int(i) + 2) % 24)},
		)
	}
	bucketHeavy := make([]state.PBinLeaf, 0, 80)
	for i := range 8 {
		bucket := byte(i)
		prefix := append([]byte{eip8297.StorageZone}, bytes.Repeat([]byte{bucket + 1}, 32)...)
		for slotIndex := range 8 {
			slot := byte(slotIndex)
			key := make([]byte, eip8297.StorageKeyLength)
			copy(key, prefix)
			key[33] = 1 << slot
			key[len(key)-1] = slot + 1
			bucketHeavy = append(bucketHeavy, state.PBinLeaf{Key: key, Value: pbinRangeWriterValue(bucket*8 + slot + 1), Stamp: uint64((i*8 + slotIndex) % 24)})
		}
	}
	for _, first := range []byte{0x10, 0x11} {
		key := make([]byte, eip8297.StorageKeyLength)
		copy(key, pbinRangeWriterSplit271Prefix())
		key[33] = first
		key[len(key)-1] = first
		bucketHeavy = append(bucketHeavy, state.PBinLeaf{Key: key, Value: pbinRangeWriterValue(first + 0x40), Stamp: uint64(first % 24)})
	}
	for i := range 8 {
		bucket := byte(i)
		key := make([]byte, eip8297.StorageKeyLength)
		key[0] = eip8297.StorageZone
		copy(key[1:33], bytes.Repeat([]byte{0x80 + bucket}, 32))
		key[len(key)-1] = 1
		bucketHeavy = append(bucketHeavy, state.PBinLeaf{Key: key, Value: pbinRangeWriterValue(100 + bucket), Stamp: uint64(i + 16)})
	}
	for i := byte(1); i <= 8; i++ {
		address := bytes.Repeat([]byte{0xa0 + i}, 20)
		bucketHeavy = append(
			bucketHeavy,
			state.PBinLeaf{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: pbinRangeWriterBasicValue(120 + i), Stamp: uint64(i)},
			state.PBinLeaf{Key: eip8297.TreeKeyCodeChunk(common.BytesToHash(bytes.Repeat([]byte{0xb0 + i}, 32)), 0), Value: pbinRangeWriterCodeValue(140 + i), Stamp: uint64(i + 8)},
		)
	}
	for _, corpus := range []*[]state.PBinLeaf{&threeZone, &zonePure, &bucketHeavy} {
		sort.Slice(*corpus, func(i, j int) bool { return bytes.Compare((*corpus)[i].Key, (*corpus)[j].Key) < 0 })
	}
	return []pbinRangeWriterCorpus{{name: "three-zone", leaves: threeZone}, {name: "zone-pure", leaves: zonePure}, {name: "bucket-heavy", leaves: bucketHeavy}}
}

func pbinRangeWriterSplit271Prefix() []byte {
	return append([]byte{eip8297.StorageZone}, bytes.Repeat([]byte{0x71}, 32)...)
}

func pbinRangeWriterValue(seed byte) []byte {
	value := bytes.Repeat([]byte{seed}, eip8297.ValueLength)
	if value[0] == 0 {
		value[0] = 1
	}
	return value
}

func pbinRangeWriterBasicValue(seed byte) []byte {
	value, err := eip8297.EncodeBasicData(uint64(seed), uint256.NewInt(uint64(seed)), 0)
	if err != nil {
		panic(err)
	}
	return value[:]
}

func pbinRangeWriterHeaderValue(seed byte) []byte {
	value := eip8297.EncodeStorageValue([]byte{seed})
	return value[:]
}

func pbinRangeWriterCodeValue(seed byte) []byte {
	var value [eip8297.ValueLength]byte
	value[0] = 1
	value[1] = seed
	return value[:]
}

func pbinRangeWriterCodeHashValue(seed byte) []byte {
	value := eip8297.CodeHashValue(common.BytesToHash(bytes.Repeat([]byte{seed}, 32)))
	return value[:]
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

func selectPBinRangeWriterHash(t testing.TB) {
	commitmentflags.Restore(t)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	statecfg.ExperimentalHexBinCommitment = true
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	require.NoError(t, eip8297.SetHashSuite(eip8297.HashBlake3))
}

func writePBinRangeWriterAccounts(t testing.TB, db kv.TemporalRwDB, endTxNum uint64) {
	t.Helper()
	for i := uint64(0); i < endTxNum; i += 8 {
		func() {
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
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
		}()
	}
}

func TestPBinDecodeFailureStopsQueuedCompute(t *testing.T) {
	selectPBinRangeWriterHash(t)
	run := openPBinRangeWriterRun(t, 30, 0, 23)
	leaves := make([]state.PBinLeaf, 100)
	for i := range leaves {
		key := make([]byte, eip8297.StorageKeyLength)
		key[0] = eip8297.StorageZone
		binary.BigEndian.PutUint64(key[len(key)-8:], uint64(i+1))
		leaves[i] = state.PBinLeaf{Key: key, Value: pbinRangeWriterValue(1), Stamp: 1}
	}
	encoded, encodeErr := pbt.EncodeOp(pbt.Op{Key: leaves[0].Key})
	require.NoError(t, encodeErr)
	var path string
	var queuedBatchFlushes atomic.Int32
	run.writer.SetPipelineHookForTest(func(stage string, index int) error {
		if stage == "stage1" && index == 0 {
			paths, err := filepath.Glob(filepath.Join(run.agg.Dirs().Tmp, "pbin-ops-*.bin"))
			if err != nil {
				return err
			}
			if len(paths) != 1 {
				return fmt.Errorf("want one ops file, got %v", paths)
			}
			path = paths[0]
			file, err := os.OpenFile(path, os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = file.WriteAt([]byte{0, 0, 0, 0}, int64(70*(4+8+len(encoded))))
			return err
		}
		if stage == "stage2" && index == 0 {
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if _, err := os.Stat(path); os.IsNotExist(err) {
					return nil
				}
				time.Sleep(time.Millisecond)
			}
			return errors.New("decoder did not finish")
		}
		if stage == "stage3" && index == 1 {
			queuedBatchFlushes.Add(1)
		}
		return nil
	})
	_, writeErr := run.write(t.Context(), leaves)
	require.ErrorContains(t, writeErr, "operation record of 0 bytes")
	require.Zero(t, queuedBatchFlushes.Load(), "stage 1 decode failed, but queued batch 1 still completed compute and reached collect: %d", queuedBatchFlushes.Load())
}

type pbinSerialTx struct {
	kv.Tx
	busy atomic.Bool
}

func (tx *pbinSerialTx) Cursor(table string) (kv.Cursor, error) {
	if !tx.busy.CompareAndSwap(false, true) {
		return nil, errors.New("MDBX transaction used concurrently")
	}
	defer tx.busy.Store(false)
	time.Sleep(10 * time.Millisecond)
	return tx.Tx.Cursor(table)
}

func (tx *pbinSerialTx) CursorDupSort(table string) (kv.CursorDupSort, error) {
	if !tx.busy.CompareAndSwap(false, true) {
		return nil, errors.New("MDBX transaction used concurrently")
	}
	defer tx.busy.Store(false)
	time.Sleep(10 * time.Millisecond)
	return tx.Tx.CursorDupSort(table)
}

func TestPBinStampWalkUsesMDBXSerially(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, 8)
	writePBinLeafStreamStates(t, db, 8, []pbinLeafStreamAccount{{address: bytes.Repeat([]byte{0x80}, 20), nonce: 1, balance: 1}})
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := agg.BeginFilesRo()
	defer at.Close()
	guarded := &pbinSerialTx{Tx: tx}
	err = state.VerifyPBinSourceStamps(t.Context(), at, guarded, false, 15, 2, nil)
	require.NoError(t, err, "valid MDBX rows must verify without concurrent access to their transaction; got %v", err)
}

func TestPBinResumeMergeErrorReleasesAggregatorLock(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	run.writer.SetResumeSourceLayoutForTest()
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		if stage == "resume-merge" {
			return errors.New("injected resume merge error")
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.ErrorContains(t, err, "injected resume merge error")
	closed := make(chan struct{})
	go func() {
		run.agg.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("aggregator close blocked after resume merge error")
	}
}

func TestPBinResumeMergeRunsOutsideDirtyFilesLock(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	run := openPBinRangeWriterRun(t, 7, 0, 23)
	run.writer.SetResumeSourceLayoutForTest()
	lockFree := false
	stop := errors.New("stop at resume merge")
	run.writer.SetPipelineHookForTest(func(stage string, _ int) error {
		if stage == "resume-merge" {
			lockFree = run.agg.DirtyFilesLockFreeForTest()
			return stop
		}
		return nil
	})
	_, err := run.write(t.Context(), pbinRangeWriterCorpora()[0].leaves)
	require.ErrorIs(t, err, stop)
	require.True(t, lockFree, "a resume merge closes a files reader, which can take dirtyFilesLock")
}

func TestPBinRejectedEndCleansRanges(t *testing.T) {
	selectPBinRangeWriterPipeline(t)
	db, agg := commitmenttemporal.Open(t, 8)
	writePBinRangeWriterAccounts(t, db, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, 3, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	pattern := filepath.Join(agg.Dirs().Tmp, "pbin-range-*")
	before, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.Empty(t, before)
	_, err = state.NewPBinRangeWriter(agg, kv.CommitmentBinDomain, math.MaxUint64)
	require.ErrorContains(t, err, "end txNum is too large")
	after, err := filepath.Glob(pattern)
	require.NoError(t, err)
	if len(after) != 0 {
		t.Errorf("rejected writer left %d range temp directories; want 0", len(after))
	}
}
