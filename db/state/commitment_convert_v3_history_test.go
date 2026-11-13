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
	"maps"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	_ "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func testDbAggregatorWithCommitmentHistory(t *testing.T, stepSize uint64, steps int) (kv.TemporalRwDB, *state.Aggregator, map[uint64][]byte) {
	t.Helper()
	previousSchema := statecfg.Schema
	previousParallel := statecfg.ExperimentalParallelCommitment
	statecfg.EnableHistoricalCommitment()
	statecfg.ExperimentalParallelCommitment = true
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
		statecfg.ExperimentalParallelCommitment = previousParallel
	})

	db, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	ctx := t.Context()

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	defer domains.Close()

	rnd := newRnd(7)
	addrs, _ := generateInputData(t, length.Addr, 1, 40)
	slots := storageSlotsSharingFirstNibble(t)
	storageKey := func(a, j int) []byte { return append(bytes.Clone(addrs[a]), slots[j]...) }
	put, del, _ := domainWriter(t, domains, rwTx)

	roots := map[uint64][]byte{}
	txCount := stepSize * uint64(steps)
	var blockNum uint64
	for txNum := range txCount {
		for range 3 {
			a := rnd.IntN(len(addrs))
			acc := accounts.Account{Nonce: txNum, Balance: *uint256.NewInt(rnd.Uint64()), CodeHash: accounts.EmptyCodeHash}
			put(kv.AccountsDomain, addrs[a], accounts.SerialiseV3(&acc), txNum)
			j := rnd.IntN(len(slots))
			if rnd.IntN(4) == 0 {
				del(kv.StorageDomain, storageKey(a, j), txNum)
			} else {
				put(kv.StorageDomain, storageKey(a, j), []byte{byte(rnd.IntN(255) + 1)}, txNum)
			}
		}
		if rnd.IntN(8) == 0 {
			a := rnd.IntN(len(addrs))
			for j := range slots {
				del(kv.StorageDomain, storageKey(a, j), txNum)
			}
			del(kv.AccountsDomain, addrs[a], txNum)
		}
		if txNum%3 == 2 || (txNum+1)%stepSize == 0 {
			root, rootErr := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, rootErr)
			roots[txNum] = root
			blockNum++
		}
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, txCount, unboundedFinalityCtx))
	return db, agg, roots
}

func TestConvertCommitmentFiles_V3HistoryOrphanStorageRoot(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	const stepSize, steps = 4, 2
	db, agg, rwTx, domains := convertTestDomainsWithParallel(t, stepSize, true)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	ctx := t.Context()

	addrs, _ := generateInputData(t, length.Addr, 1, 4)
	owner := addrs[0]
	var next uint64
	slots := [][]byte{keyWithHashPrefix(length.Hash, []byte{1}, &next), keyWithHashPrefix(length.Hash, []byte{1}, &next), keyWithHashPrefix(length.Hash, []byte{2}, &next), keyWithHashPrefix(length.Hash, []byte{2}, &next)}
	slot := func(s []byte) []byte { return append(bytes.Clone(owner), s...) }

	roots := map[uint64][]byte{}
	for txNum := range uint64(stepSize * steps) {
		switch txNum {
		case 0:
			for _, a := range addrs {
				putAccount(a, txNum)
			}
			for _, s := range slots {
				put(kv.StorageDomain, slot(s), []byte{1}, txNum)
			}
		case 1:
			for _, s := range slots {
				del(kv.StorageDomain, slot(s), txNum)
			}
			del(kv.AccountsDomain, owner, txNum)
		case 2:
			putAccount(owner, txNum)
			put(kv.StorageDomain, slot(slots[0]), []byte{2}, txNum)
		case 3:
			put(kv.StorageDomain, slot(slots[2]), []byte{3}, txNum)
		default:
			putAccount(addrs[1+int(txNum)%3], txNum)
		}
		root, rootErr := domains.ComputeCommitment(ctx, rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, rootErr)
		roots[txNum] = root
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

	ownerPath := commitment.KeyToHexNibbleHash(owner)
	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	orphan, _, err := roTx.GetAsOf(kv.CommitmentDomain, nibbles.HexToCompact(ownerPath), 3)
	require.NoError(t, err)
	roTx.Rollback()
	require.NotEmpty(t, orphan, "legacy storage root record of the recreated account is expected to survive its deletion")

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	roTx, err = db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	latestState, _, err := roTx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, kv.GetLatestOptions{})
	require.NoError(t, err)
	roTx.Rollback()
	_, lastFrozenCommit, _, err := commitment.DecodeCommitmentV3State(latestState)
	require.NoError(t, err)
	require.GreaterOrEqual(t, lastFrozenCommit, uint64(3))
	for txNum := range lastFrozenCommit + 1 {
		checks, rootErr := state.FoldCommitmentV3(ctx, agg, []uint64{txNum + 1}, nil)
		require.NoErrorf(t, rootErr, "records as of txNum %d", txNum+1)
		require.Equalf(t, roots[txNum], checks[0].Root, "root as of txNum %d", txNum+1)
	}
}

func TestConvertCommitmentFiles_V3History(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	testConvertCommitmentHistoryRoots(t)
}

func TestConvertCommitmentFiles_V3HistorySplitChains(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	previousBatchBytes := state.SetCommitmentV3BatchBytesForTest(256)
	t.Cleanup(func() { state.SetCommitmentV3BatchBytesForTest(previousBatchBytes) })
	testConvertCommitmentHistoryRoots(t)
}

func TestConvertCommitmentFiles_V3HistoryShapeTable(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	previousMinVersions := state.SetLegacyShapeMinVersionsForTest(1)
	t.Cleanup(func() { state.SetLegacyShapeMinVersionsForTest(previousMinVersions) })
	testConvertCommitmentHistoryRoots(t)
}

func testConvertCommitmentHistoryRoots(t *testing.T) {
	t.Helper()
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg, roots := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	legacyHistory, err := filepath.Glob(filepath.Join(agg.Dirs().SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.NotEmpty(t, legacyHistory)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	converted, err := filepath.Glob(filepath.Join(agg.Dirs().SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.Len(t, converted, len(legacyHistory))
	for _, p := range converted {
		require.Truef(t, strings.HasPrefix(filepath.Base(p), "v3.0-"), "%s is not a v3.0 history file", filepath.Base(p))
	}
	requireCommitmentHistoryRoots(t, db, agg, roots)
}

func requireCommitmentHistoryRoots(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, roots map[uint64][]byte) {
	t.Helper()
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	latestState, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, kv.GetLatestOptions{})
	require.NoError(t, err)
	_, lastFrozenCommit, _, err := commitment.DecodeCommitmentV3State(latestState)
	require.NoError(t, err)
	tx.Rollback()
	var asOf []uint64
	for _, txNum := range slices.Sorted(maps.Keys(roots)) {
		if txNum > lastFrozenCommit {
			break
		}
		asOf = append(asOf, txNum+1)
	}
	require.Greater(t, len(asOf), 90)
	defer state.SetCommitmentV3FoldRangesForTest(state.SetCommitmentV3FoldRangesForTest(5))
	defer state.SetCommitmentV3FoldChunkForTest(state.SetCommitmentV3FoldChunkForTest(1))
	checks, err := state.FoldCommitmentV3(t.Context(), agg, append(asOf, math.MaxUint64), nil)
	require.NoError(t, err)
	for i, txNum := range asOf {
		require.Equalf(t, roots[txNum-1], checks[i].Root, "root as of txNum %d in one fold of %d", txNum, len(asOf))
	}
}

func TestConvertCommitmentFiles_V3HistoryContinueSkipsConvertedFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg, roots := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	legacyHistory, err := filepath.Glob(filepath.Join(agg.Dirs().SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.Greater(t, len(legacyHistory), 1)

	errInterrupted := errors.New("interrupted")
	var converted []int
	interrupt := true
	state.SetConvertHistoryAfterFileHookForTest(func(idx int) error {
		converted = append(converted, idx)
		if interrupt {
			interrupt = false
			return errInterrupted
		}
		return nil
	})
	t.Cleanup(func() { state.SetConvertHistoryAfterFileHookForTest(nil) })
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	err = state.ConvertCommitmentFiles(t.Context(), state.AggTx(tx), state.ConvertOpts{TargetV3: true}, log.New())
	tx.Rollback()
	require.ErrorIs(t, err, errInterrupted)

	converted = nil
	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true, Continue: true})
	require.NotContains(t, converted, 0, "--continue must reuse the history file converted before the interrupt")
	require.Len(t, converted, len(legacyHistory)-1)
	requireCommitmentHistoryRoots(t, db, agg, roots)
}

func TestConvertCommitmentFiles_V3HistoryRejectsAsOfOrphans(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg, _ := testDbAggregatorWithCommitmentHistory(t, 7, 2)
	state.SetConvertPhase5HookForTest(func() error {
		injectCommitmentHistoryOrphan(t, db, agg)
		return nil
	})
	t.Cleanup(func() { state.SetConvertPhase5HookForTest(nil) })

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback) //nolint:gocritic
	err = state.ConvertCommitmentFiles(t.Context(), state.AggTx(tx), state.ConvertOpts{TargetV3: true}, log.New())
	tx.Rollback()
	require.ErrorContains(t, err, "orphan records")
}

func TestVerifyCommitmentV3_RejectsOrphanInLastGroup(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg, _ := testDbAggregatorWithCommitmentHistory(t, 7, 2)
	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
	orphanTx := injectCommitmentHistoryOrphan(t, db, agg)
	clean := orphanTx + 1
	require.NoError(t, state.VerifyCommitmentV3(t.Context(), agg, []uint64{clean, clean, clean}, false, log.New()))
	err := state.VerifyCommitmentV3(t.Context(), agg, []uint64{clean, clean, clean, orphanTx}, false, log.New())
	require.ErrorContains(t, err, "orphan records")
}

func TestConvertCommitmentFiles_V3HistoryOversizedBatch(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, _, _ := testDbAggregatorWithCommitmentHistory(t, 7, 4)
	previousBatchBytes := state.SetCommitmentV3BatchBytesForTest(256)
	t.Cleanup(func() { state.SetCommitmentV3BatchBytesForTest(previousBatchBytes) })
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	err = state.ConvertCommitmentFiles(ctx, state.AggTx(tx), state.ConvertOpts{TargetV3: true}, log.New())
	tx.Rollback()
	require.NoError(t, err)
}

func TestConvertCommitmentFiles_V3HistoryPreflightLeavesNoProducer(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, _, _ := testDbAggregatorWithCommitmentHistory(t, 7, 4)
	previousBatchBytes := state.SetCommitmentV3BatchBytesForTest(256)
	t.Cleanup(func() { state.SetCommitmentV3BatchBytesForTest(previousBatchBytes) })
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })
	state.SetLegacyCommitTxNumsForTest(func(*state.DomainRoTx, uint64, uint64) ([]uint64, error) {
		return nil, errors.New("missing .ef range")
	})
	t.Cleanup(func() { state.SetLegacyCommitTxNumsForTest(nil) })

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	opt := goleak.IgnoreCurrent()
	err = state.ConvertCommitmentFiles(t.Context(), state.AggTx(tx), state.ConvertOpts{TargetV3: true}, log.New())
	tx.Rollback()
	require.ErrorContains(t, err, "missing .ef range")
	goleak.VerifyNone(t, opt)
}

func TestConvertCommitmentFiles_V3HistoryRefusesPrunedStart(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg, _ := testDbAggregatorWithCommitmentHistory(t, 10, 4)
	dirs := agg.Dirs()
	var pruned []string
	for _, d := range []string{dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		files, err := filepath.Glob(filepath.Join(d, "*-commitment.0-*"))
		require.NoError(t, err)
		pruned = append(pruned, files...)
	}
	require.NotEmpty(t, pruned)
	agg.CloseFilesNoReopen()
	for _, f := range pruned {
		require.NoError(t, dir.RemoveFile(f))
	}
	require.NoError(t, agg.ReloadFiles())

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	err = state.ConvertCommitmentFiles(t.Context(), state.AggTx(tx), state.ConvertOpts{TargetV3: true}, log.New())
	tx.Rollback()
	require.ErrorContains(t, err, "commitment history below step")
	require.NoDirExists(t, filepath.Join(dirs.Snap, "backup"))
}

type commitmentHistoryPair struct {
	key, value []byte
}

func injectCommitmentHistoryOrphan(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator) uint64 {
	t.Helper()
	dirs := agg.Dirs()
	historyPaths, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.NotEmpty(t, historyPaths)
	historyPath := historyPaths[len(historyPaths)-1]
	base := strings.TrimSuffix(filepath.Base(historyPath), ".v")
	idxPath := filepath.Join(dirs.SnapIdx, base+".ef")
	cfg := agg.Cfg(kv.CommitmentDomain).Hist

	historyPairs := readCommitmentHistoryValues(t, historyPath, cfg)
	idxPairs := readCommitmentHistoryIndex(t, idxPath, cfg)

	var sourceKey, sourceValue []byte
	var orphanTx uint64
	for _, idxPair := range idxPairs {
		if len(idxPair.key) <= 33 || idxPair.key[0] != 0x41 {
			continue
		}
		for _, historyPair := range historyPairs {
			if len(historyPair.key) < 8 || !bytes.Equal(historyPair.key[8:], idxPair.key) || len(historyPair.value) == 0 {
				continue
			}
			txNum := binary.BigEndian.Uint64(historyPair.key)
			if sourceKey == nil || txNum > orphanTx {
				sourceKey = slices.Clone(idxPair.key)
				sourceValue = slices.Clone(historyPair.value)
				orphanTx = txNum
			}
		}
	}
	require.NotEmpty(t, sourceKey)
	orphanKey := slices.Clone(sourceKey)
	for i := 1; i < 33; i++ {
		orphanKey[i] = 0xff
	}
	require.NotEqual(t, sourceKey, orphanKey)
	var sequence multiencseq.SequenceBuilder
	sequence.Reset(0, 1, orphanTx)
	sequence.AddOffset(orphanTx)
	sequence.Build()
	idxPairs = append(idxPairs, commitmentHistoryPair{key: orphanKey, value: sequence.AppendBytes(nil)})
	historyKey := binary.BigEndian.AppendUint64(nil, orphanTx)
	historyPairs = append(historyPairs, commitmentHistoryPair{key: append(historyKey, orphanKey...), value: sourceValue})
	slices.SortFunc(idxPairs, func(a, b commitmentHistoryPair) int { return bytes.Compare(a.key, b.key) })
	slices.SortFunc(historyPairs, func(a, b commitmentHistoryPair) int { return bytes.Compare(a.key, b.key) })

	agg.CloseFilesNoReopen()
	rangeBase := strings.TrimPrefix(base, "v3.0")
	for _, dirPath := range []string{dirs.SnapIdx, dirs.SnapAccessors} {
		for _, ext := range []string{".bt", ".efi", ".kvei", ".kvi", ".vi"} {
			accessors, err := filepath.Glob(filepath.Join(dirPath, "*"+rangeBase+ext))
			require.NoError(t, err)
			for _, accessor := range accessors {
				require.NoError(t, dir.RemoveFile(accessor))
			}
		}
	}
	rewriteCommitmentHistoryV3(t, dirs, historyPath, historyPairs, cfg)
	rewriteCommitmentHistoryIndex(t, dirs, idxPath, idxPairs, cfg)
	require.NoError(t, agg.ReloadFiles())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), db, 1))
	return orphanTx
}

func readCommitmentHistoryValues(t *testing.T, path string, cfg statecfg.HistCfg) []commitmentHistoryPair {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	pageValues := d.CompressedPageValuesCount()
	if d.CompressionFormatVersion() == seg.FileCompressionFormatV0 {
		pageValues = cfg.HistoryValuesOnCompressedPage
	}
	r := seg.NewPagedReader(seg.NewReader(d.MakeGetter(), cfg.Compression), pageValues, true)
	var pairs []commitmentHistoryPair
	for r.HasNext() {
		key, value, _, _ := r.Next2(nil)
		pairs = append(pairs, commitmentHistoryPair{key: slices.Clone(key), value: slices.Clone(value)})
	}
	return pairs
}

func readCommitmentHistoryIndex(t *testing.T, path string, cfg statecfg.HistCfg) []commitmentHistoryPair {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	r := seg.NewReader(d.MakeGetter(), cfg.IiCfg.Compression)
	var pairs []commitmentHistoryPair
	for r.HasNext() {
		key, _ := r.Next(nil)
		require.True(t, r.HasNext())
		value, _ := r.Next(nil)
		pairs = append(pairs, commitmentHistoryPair{key: slices.Clone(key), value: slices.Clone(value)})
	}
	return pairs
}

func dropHistoryLeafOnlyRewrites(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator) int {
	t.Helper()
	dirs, cfg := agg.Dirs(), agg.Cfg(kv.CommitmentDomain).Hist
	historyPaths, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.NotEmpty(t, historyPaths)
	agg.CloseFilesNoReopen()
	total := 0
	for _, historyPath := range historyPaths {
		base := strings.TrimSuffix(filepath.Base(historyPath), ".v")
		_, rangeBase, _ := strings.Cut(base, "-")
		idxPaths, err := filepath.Glob(filepath.Join(dirs.SnapIdx, "*-"+rangeBase+".ef"))
		require.NoError(t, err)
		require.Len(t, idxPaths, 1)
		idxPath := idxPaths[0]
		from, _ := commitmentFileRange(t, historyPath)
		baseTxNum := from * agg.StepSize()
		historyPairs := readCommitmentHistoryValues(t, historyPath, cfg)
		versions := map[string][]commitmentHistoryPair{}
		for _, pair := range historyPairs {
			if len(pair.key) > 8 {
				versions[string(pair.key[8:])] = append(versions[string(pair.key[8:])], pair)
			}
		}
		dropped := map[string]bool{}
		for key, chain := range versions {
			if commitment.IsCommitmentStateKey([]byte(key)) {
				continue
			}
			for i := 0; i+1 < len(chain); i++ {
				before, beforeOk := branchCells(chain[i].value)
				written, writtenOk := branchCells(chain[i+1].value)
				if len(chain[i].value) != 0 && beforeOk && writtenOk && before == written {
					dropped[string(chain[i].key)] = true
				}
			}
		}
		if len(dropped) == 0 {
			continue
		}
		total += len(dropped)
		historyPairs = slices.DeleteFunc(historyPairs, func(pair commitmentHistoryPair) bool { return dropped[string(pair.key)] })
		idxPairs := readCommitmentHistoryIndex(t, idxPath, cfg)
		var sequence multiencseq.SequenceBuilder
		for i, pair := range idxPairs {
			var kept []uint64
			for it := multiencseq.ReadMultiEncSeq(baseTxNum, pair.value).Iterator(0); it.HasNext(); {
				txNum, err := it.Next()
				require.NoError(t, err)
				if !dropped[string(append(binary.BigEndian.AppendUint64(nil, txNum), pair.key...))] {
					kept = append(kept, txNum)
				}
			}
			sequence.Reset(baseTxNum, uint64(len(kept)), kept[len(kept)-1])
			for _, txNum := range kept {
				sequence.AddOffset(txNum)
			}
			sequence.Build()
			idxPairs[i].value = sequence.AppendBytes(nil)
		}
		for _, dirPath := range []string{dirs.SnapIdx, dirs.SnapAccessors} {
			for _, ext := range []string{".bt", ".efi", ".kvei", ".kvi", ".vi"} {
				accessors, err := filepath.Glob(filepath.Join(dirPath, "*"+rangeBase+ext))
				require.NoError(t, err)
				for _, accessor := range accessors {
					require.NoError(t, dir.RemoveFile(accessor))
				}
			}
		}
		rewriteCommitmentHistoryV3(t, dirs, historyPath, historyPairs, cfg)
		rewriteCommitmentHistoryIndex(t, dirs, idxPath, idxPairs, cfg)
	}
	require.NoError(t, agg.ReloadFiles())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), db, 1))
	return total
}

func rewriteCommitmentHistoryV3(t *testing.T, dirs datadir.Dirs, path string, pairs []commitmentHistoryPair, cfg statecfg.HistCfg) {
	t.Helper()
	tmp := path + ".tmp"
	comp, err := seg.NewCompressor(t.Context(), "test commitment history", tmp, dirs.Tmp, cfg.CompressorCfg, log.LvlTrace, log.New())
	require.NoError(t, err)
	w := seg.NewPagedWriter(t.Context(), seg.NewWriter(comp, cfg.Compression), comp.GetValuesOnCompressedPage() > 0, cfg.CompressorCfg.Workers)
	for _, pair := range pairs {
		require.NoError(t, w.Add(pair.key, pair.value))
	}
	require.NoError(t, w.Compress())
	comp.Close()
	require.NoError(t, os.Rename(tmp, path))
}

func rewriteCommitmentHistoryIndex(t *testing.T, dirs datadir.Dirs, path string, pairs []commitmentHistoryPair, cfg statecfg.HistCfg) {
	t.Helper()
	tmp := path + ".tmp"
	comp, err := seg.NewCompressor(t.Context(), "test commitment history index", tmp, dirs.Tmp, cfg.IiCfg.CompressorCfg, log.LvlTrace, log.New())
	require.NoError(t, err)
	w := seg.NewWriter(comp, cfg.IiCfg.Compression)
	for _, pair := range pairs {
		_, err = w.Write(pair.key)
		require.NoError(t, err)
		_, err = w.Write(pair.value)
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
	comp.Close()
	require.NoError(t, os.Rename(tmp, path))
}

func TestRestoreCommitmentFiles_V3RestoresHistory(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	if testing.Short() {
		t.Skip("long-running test")
	}
	for _, resume := range []bool{false, true} {
		t.Run(fmt.Sprintf("resume=%t", resume), func(t *testing.T) {
			db, agg, _ := testDbAggregatorWithCommitmentHistory(t, 10, 32)
			dirs := agg.Dirs()
			legacyHistory, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
			require.NoError(t, err)
			require.NotEmpty(t, legacyHistory)

			runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
			agg.CloseFilesNoReopen()
			if resume {
				var converted []string
				converted, err = filepath.Glob(filepath.Join(dirs.SnapDomain, "v3.0-commitment.*"))
				require.NoError(t, err)
				for _, p := range converted {
					require.NoError(t, dir.RemoveFile(p))
				}
				domainBackup := filepath.Join(dirs.Snap, "backup", "domains")
				var backups []os.DirEntry
				backups, err = os.ReadDir(domainBackup)
				require.NoError(t, err)
				for _, e := range backups {
					require.NoError(t, os.Rename(filepath.Join(domainBackup, e.Name()), filepath.Join(dirs.SnapDomain, e.Name())))
				}
				require.NoError(t, dir.RemoveFile(domainBackup))
			}
			_, restoreErr := state.RestoreCommitmentFiles(t.Context(), dirs, log.New())
			require.NoError(t, restoreErr)

			history, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
			require.NoError(t, err)
			require.ElementsMatch(t, legacyHistory, history)
			domain, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*.kv"))
			require.NoError(t, err)
			for _, p := range domain {
				require.Falsef(t, strings.HasPrefix(filepath.Base(p), "v3.0-"), "%s left after restore", filepath.Base(p))
			}
		})
	}
}

func TestConvertCommitmentFiles_V3HistoryStaleStorageBranchOfRecreatedAccount(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })

	const stepSize, steps = 4, 3
	db, agg, _ := buildRecreatedOwnerFilesWithParallel(t, stepSize, steps, 4, true, true)
	ctx := t.Context()

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	for txNum := uint64(1); txNum <= stepSize*steps; txNum++ {
		_, err := state.FoldCommitmentV3(ctx, agg, []uint64{txNum}, nil)
		require.NoErrorf(t, err, "records as of txNum %d", txNum)
	}
}

func TestConvertCommitmentFiles_V3HistorySerialRecreatedAccountRefusesBeforeSwap(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousSchema := statecfg.Schema
	previousParallel := statecfg.ExperimentalParallelCommitment
	statecfg.EnableHistoricalCommitment()
	statecfg.ExperimentalParallelCommitment = false
	t.Cleanup(func() {
		statecfg.Schema = previousSchema
		statecfg.ExperimentalParallelCommitment = previousParallel
	})

	db, agg, _ := buildRecreatedOwnerFilesWithParallel(t, 4, 3, 4, true, false)
	beforeDomain := snapshotCommitmentFiles(t, agg.Dirs().SnapDomain)
	beforeHistory := snapshotCommitmentFiles(t, agg.Dirs().SnapHistory)
	beforeIdx := snapshotCommitmentFiles(t, agg.Dirs().SnapIdx)
	beforeAccessors := snapshotCommitmentFiles(t, agg.Dirs().SnapAccessors)
	require.NotEmpty(t, beforeIdx)
	require.NotEmpty(t, beforeAccessors)
	backupRoot := filepath.Join(agg.Dirs().Snap, "backup")
	_, err := os.Stat(backupRoot)
	require.ErrorIs(t, err, os.ErrNotExist)

	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	err = state.ConvertCommitmentFiles(t.Context(), state.AggTx(tx), state.ConvertOpts{TargetV3: true}, log.New())
	tx.Rollback()
	require.ErrorContains(t, err, "[commitment_convert] phase 1 history:")

	require.Equal(t, beforeDomain, snapshotCommitmentFiles(t, agg.Dirs().SnapDomain))
	require.Equal(t, beforeHistory, snapshotCommitmentFiles(t, agg.Dirs().SnapHistory))
	require.Equal(t, beforeIdx, snapshotCommitmentFiles(t, agg.Dirs().SnapIdx))
	require.Equal(t, beforeAccessors, snapshotCommitmentFiles(t, agg.Dirs().SnapAccessors))
	_, err = os.Stat(backupRoot)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestConvertCommitmentFiles_V3HistoryExtensionRoot(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	const stepSize, steps = 4, 8
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	_, _, putAccount := domainWriter(t, domains, rwTx)
	next := uint64(1)
	addrs := [][]byte{keyWithHashPrefix(length.Addr, []byte{4}, &next), keyWithHashPrefix(length.Addr, []byte{4}, &next)}
	roots := map[uint64][]byte{}
	for txNum := range uint64(stepSize * steps) {
		putAccount(addrs[txNum%2], txNum)
		if txNum == 0 {
			putAccount(addrs[1], txNum)
		}
		root, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
		roots[txNum] = root
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	for _, txNum := range slices.Sorted(maps.Keys(roots)) {
		if txNum >= stepSize*(steps-1) {
			break
		}
		checks, err := state.FoldCommitmentV3(t.Context(), agg, []uint64{txNum + 1}, nil)
		require.NoErrorf(t, err, "records as of txNum %d", txNum+1)
		require.Equalf(t, roots[txNum], checks[0].Root, "root as of txNum %d", txNum+1)
	}
}

func TestConvertCommitmentFiles_V3HistoryRootWithOnlyDirectLeaves(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	const stepSize, steps = 4, 8
	previousSchema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = previousSchema })
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	_, _, putAccount := domainWriter(t, domains, rwTx)
	next := uint64(1)
	addrs := [][]byte{keyWithHashPrefix(length.Addr, []byte{4}, &next), keyWithHashPrefix(length.Addr, []byte{9}, &next)}
	roots := map[uint64][]byte{}
	for txNum := range uint64(stepSize * steps) {
		putAccount(addrs[txNum%2], txNum)
		if txNum == 0 {
			putAccount(addrs[1], txNum)
		}
		root, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
		roots[txNum] = root
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))
	require.Positive(t, dropHistoryLeafOnlyRewrites(t, db, agg))

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	for _, txNum := range slices.Sorted(maps.Keys(roots)) {
		if txNum >= stepSize*(steps-1) {
			break
		}
		checks, err := state.FoldCommitmentV3(t.Context(), agg, []uint64{txNum + 1}, nil)
		require.NoErrorf(t, err, "records as of txNum %d", txNum+1)
		require.Equalf(t, roots[txNum], checks[0].Root, "root as of txNum %d", txNum+1)
	}
}

func TestConvertCommitmentFiles_HistoryBackupFailureNamesRestore(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	_, agg, _ := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	at := agg.BeginFilesRo()
	defer at.Close()
	err := state.ConvertCommitmentFiles(t.Context(), at, state.ConvertOpts{
		TargetV3: true,
		BeforeSwap: func() error {
			backupRoot := filepath.Join(agg.Dirs().Snap, "backup")
			if err := os.MkdirAll(backupRoot, 0o755); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(backupRoot, "history"), nil, 0o644)
		},
	}, log.New())
	require.ErrorContains(t, err, "phase 3 history backup")
	require.ErrorContains(t, err, "restore with `integration commitment convert --restore`")
	files, err := filepath.Glob(filepath.Join(agg.Dirs().SnapDomain, "v3.0-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
}
