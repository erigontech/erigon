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
	"cmp"
	"encoding/binary"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestConvertCommitmentFiles_V3(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true})
	legacyTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer legacyTx.Rollback()
	legacyState, found, _, _, err := state.AggTx(legacyTx).DebugGetLatestFromFiles(kv.CommitmentDomain, commitment.KeyCommitmentState, math.MaxUint64)
	require.NoError(t, err)
	require.True(t, found)
	legacyConverted, err := v3.ConvertLegacyState(legacyState)
	legacyTx.Rollback()
	require.NoError(t, err)
	wantBlock, wantTxNum, wantRoot, err := commitment.DecodeCommitmentV3State(legacyConverted)
	require.NoError(t, err)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	snapDir := agg.Dirs().SnapDomain
	kvs, err := filepath.Glob(filepath.Join(snapDir, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.Greater(t, len(kvs), 1)
	for _, p := range kvs {
		keys, _ := readKVFile(t, agg, p)
		has := slices.ContainsFunc(keys, func(k []byte) bool { return bytes.Equal(k, commitment.KeyCommitmentV3State) })
		require.Truef(t, has, "%s has no v3 state record", filepath.Base(p))
		name := filepath.Base(p)
		require.Truef(t, strings.HasPrefix(name, "v3.0-"), "%s is not a v3.0 file", name)
		rangeName := strings.TrimSuffix(strings.TrimPrefix(name, "v3.0-"), ".kv")
		for ext, want := range map[string]int{".bt": 1, ".kvei": 1, ".kvi": 0} {
			siblings, globErr := filepath.Glob(filepath.Join(snapDir, "*-"+rangeName+ext))
			require.NoError(t, globErr)
			require.Lenf(t, siblings, want, "%s siblings of %s", ext, name)
		}
	}

	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	stateValue, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, kv.GetLatestOptions{})
	require.NoError(t, err)
	blockNum, txNum, root, err := commitment.DecodeCommitmentV3State(stateValue)
	require.NoError(t, err)
	require.Equal(t, wantRoot, root)
	require.Equal(t, wantBlock, blockNum)
	require.Equal(t, wantTxNum, txNum)
}

func commitmentFileRange(t *testing.T, path string) (from, to uint64) {
	t.Helper()
	info, _, ok := snaptype.ParseFileName("", path)
	require.Truef(t, ok, "bad commitment file name %s", path)
	return info.From, info.To
}

func branchCells(value []byte) (string, bool) {
	var sb strings.Builder
	err := commitment.BranchData(value).ForEachCell(func(nib int, c commitment.BranchCell) error {
		fmt.Fprintf(&sb, "%x:%x/%x/%x/%x;", nib, c.Extension, c.AccountAddr, c.StorageAddr, c.Hash)
		return nil
	})
	return sb.String(), err == nil
}

func dropLeafOnlyRewrites(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator) int {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(agg.Dirs().SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(paths), 2)
	slices.SortFunc(paths, func(a, b string) int {
		af, _ := commitmentFileRange(t, a)
		bf, _ := commitmentFileRange(t, b)
		return cmp.Compare(af, bf)
	})
	older := map[string][]byte{}
	for _, p := range paths[:len(paths)-1] {
		keys, vals := readKVFile(t, agg, p)
		for i, k := range keys {
			older[string(k)] = vals[i]
		}
	}
	newest := paths[len(paths)-1]
	keys, vals := readKVFile(t, agg, newest)
	tmp := newest + ".tmp"
	comp, err := seg.NewCompressor(t.Context(), "drop-leaf-only-rewrites", tmp, agg.Dirs().Tmp, seg.DefaultCfg, log.LvlTrace, log.New())
	require.NoError(t, err)
	defer comp.Close()
	w := seg.NewWriter(comp, agg.Cfg(kv.CommitmentDomain).Compression)
	dropped := 0
	for i, k := range keys {
		prev, ok := older[string(k)]
		if ok && !bytes.Equal(prev, vals[i]) && !commitment.IsCommitmentStateKey(k) {
			prevCells, prevOk := branchCells(prev)
			cells, cellsOk := branchCells(vals[i])
			if prevOk && cellsOk && prevCells == cells {
				dropped++
				continue
			}
		}
		_, err = w.Write(k)
		require.NoError(t, err)
		_, err = w.Write(vals[i])
		require.NoError(t, err)
	}
	require.NoError(t, comp.Compress())
	comp.Close()
	from, to := commitmentFileRange(t, newest)
	agg.CloseFilesNoReopen()
	accessors, err := filepath.Glob(filepath.Join(agg.Dirs().SnapDomain, fmt.Sprintf("*-commitment.%d-%d.*", from, to)))
	require.NoError(t, err)
	for _, p := range accessors {
		switch filepath.Ext(p) {
		case ".kvi", ".bt", ".kvei":
			require.NoError(t, dir.RemoveFile(p))
		}
	}
	require.NoError(t, os.Rename(tmp, newest))
	require.NoError(t, agg.ReloadFiles())
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), db, 2))
	return dropped
}

func TestConvertCommitmentFiles_V3BranchNotRewrittenAfterLeafChange(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true})
	require.Positive(t, dropLeafOnlyRewrites(t, db, agg))

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
}

func storageSlotWithHashPrefix(tb testing.TB, prefix []byte, next *uint64) []byte {
	tb.Helper()
	return keyWithHashPrefix(length.Hash, prefix, next)
}

func addressWithHashPrefix(tb testing.TB, prefix []byte, next *uint64) []byte {
	tb.Helper()
	return keyWithHashPrefix(length.Addr, prefix, next)
}

func keyWithHashPrefix(size int, prefix []byte, next *uint64) []byte {
	for ; ; *next++ {
		key := make([]byte, size)
		binary.BigEndian.PutUint64(key[size-8:], *next)
		path := make([]byte, 2*length.Hash)
		nibbles.Expand(crypto.Keccak256(key), path)
		if bytes.HasPrefix(path, prefix) {
			*next++
			return key
		}
	}
}

func convertTestDomains(t *testing.T, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator, kv.TemporalRwTx, *execctx.SharedDomains) {
	return convertTestDomainsWithParallel(t, stepSize, true)
}

func convertTestDomainsWithParallel(t *testing.T, stepSize uint64, parallel bool) (kv.TemporalRwDB, *state.Aggregator, kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
	previousParallel := statecfg.ExperimentalParallelCommitment
	statecfg.ExperimentalParallelCommitment = parallel
	t.Cleanup(func() { statecfg.ExperimentalParallelCommitment = previousParallel })
	db, agg := testDbAndAggregatorv3(t, stepSize)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback) //nolint:gocritic
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New(), execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	return db, agg, rwTx, domains
}

func domainWriter(t *testing.T, domains *execctx.SharedDomains, tx kv.TemporalRwTx) (put func(d kv.Domain, k, v []byte, txNum uint64), del func(d kv.Domain, k []byte, txNum uint64), putAccount func(addr []byte, txNum uint64)) {
	put = func(d kv.Domain, k, v []byte, txNum uint64) {
		prev, _, err := domains.GetLatest(d, tx, k)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(d, tx, k, v, txNum, prev))
	}
	del = func(d kv.Domain, k []byte, txNum uint64) {
		prev, _, err := domains.GetLatest(d, tx, k)
		require.NoError(t, err)
		if len(prev) != 0 {
			require.NoError(t, domains.DomainDel(d, tx, k, txNum, prev))
		}
	}
	putAccount = func(addr []byte, txNum uint64) {
		acc := accounts.Account{Nonce: txNum + 1, Balance: *uint256.NewInt(txNum + 1), CodeHash: accounts.EmptyCodeHash}
		put(kv.AccountsDomain, addr, accounts.SerialiseV3(&acc), txNum)
	}
	return put, del, putAccount
}

func TestConvertCommitmentFiles_V3CollapsedStorageBranch(t *testing.T) {
	for _, keysV2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("keysV2=%t", keysV2), func(t *testing.T) {
			testConvertCommitmentFilesV3CollapsedStorageBranch(t, keysV2)
		})
	}
}

func buildRecreatedOwnerFiles(t *testing.T, stepSize, steps uint64, accounts int, laterSlot bool) (kv.TemporalRwDB, *state.Aggregator, []byte) {
	return buildRecreatedOwnerFilesWithParallel(t, stepSize, steps, accounts, laterSlot, true)
}

func buildRecreatedOwnerFilesWithParallel(t *testing.T, stepSize, steps uint64, accounts int, laterSlot, parallel bool) (kv.TemporalRwDB, *state.Aggregator, []byte) {
	t.Helper()
	db, agg, rwTx, domains := convertTestDomainsWithParallel(t, stepSize, parallel)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	ctx := t.Context()

	addrs, _ := generateInputData(t, length.Addr, 1, accounts)
	owner := addrs[0]
	next := uint64(1)
	live := [][]byte{
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x2}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x5}, &next),
		storageSlotWithHashPrefix(t, []byte{0x3}, &next),
	}
	old := [][]byte{
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x0}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x9}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0xe, 0x5}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0xe, 0xa}, &next),
	}
	var later []byte
	if laterSlot {
		later = storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x3}, &next)
	}
	slot := func(s []byte) []byte { return append(bytes.Clone(owner), s...) }
	for txNum := range stepSize * steps {
		switch {
		case txNum == 0:
			for _, a := range addrs {
				putAccount(a, txNum)
			}
			for _, s := range append(slices.Clone(live), old...) {
				put(kv.StorageDomain, slot(s), []byte{1}, txNum)
			}
		case txNum == 1:
			for _, s := range append(slices.Clone(live), old...) {
				del(kv.StorageDomain, slot(s), txNum)
			}
			del(kv.AccountsDomain, owner, txNum)
		case txNum == 2:
			putAccount(owner, txNum)
			for _, s := range live {
				put(kv.StorageDomain, slot(s), []byte{2}, txNum)
			}
		case txNum == 4 && laterSlot:
			put(kv.StorageDomain, slot(later), []byte{3}, txNum)
		default:
			putAccount(addrs[1], txNum)
		}
		_, err := domains.ComputeCommitment(ctx, rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))
	return db, agg, owner
}

func testConvertCommitmentFilesV3CollapsedStorageBranch(t *testing.T, keysV2 bool) {
	db, agg, owner := buildRecreatedOwnerFiles(t, 4, 2, 2, false)
	ctx := t.Context()

	roTx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	collapsed, _, _, _, err := state.AggTx(roTx).DebugGetLatestFromFiles(kv.CommitmentDomain, nibbles.HexToCompact(append(commitment.KeyToHexNibbleHash(owner), 0xe, 0x1)), math.MaxUint64)
	require.NoError(t, err)
	roTx.Rollback()
	require.GreaterOrEqual(t, len(collapsed), 4)
	require.NotZero(t, binary.BigEndian.Uint16(collapsed[2:4]), "legacy keeps the branch record left over from the deleted incarnation")

	if keysV2 {
		runOrchestrator(t, db, state.ConvertOpts{TargetNibblesV2: true})
		require.NoError(t, dir.RemoveAll(filepath.Join(agg.Dirs().Snap, "backup", "domains")))
	}
	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	c, err := state.FoldCommitmentV3(ctx, agg, math.MaxUint64)
	require.NoError(t, err)
	require.Zero(t, c.Orphans)
}

func TestConvertCommitmentFiles_V3StorageRootSurvivesBranchCollapseAcrossSteps(t *testing.T) {
	const stepSize, steps = 1, 4
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	ctx := t.Context()

	nextA := uint64(1)
	a := addressWithHashPrefix(t, []byte{0x1, 0x1}, &nextA)
	b := addressWithHashPrefix(t, []byte{0x1, 0x2}, &nextA)
	c := addressWithHashPrefix(t, []byte{0x5}, &nextA)
	nextS := uint64(1)
	slots := [][]byte{
		storageSlotWithHashPrefix(t, []byte{0x1}, &nextS),
		storageSlotWithHashPrefix(t, []byte{0x9}, &nextS),
		storageSlotWithHashPrefix(t, []byte{0xc}, &nextS),
	}
	for txNum := range uint64(stepSize * steps) {
		switch txNum {
		case 0:
			for _, x := range [][]byte{a, b, c} {
				putAccount(x, txNum)
			}
			for _, s := range slots {
				put(kv.StorageDomain, append(bytes.Clone(a), s...), []byte{1}, txNum)
			}
		case 1:
			del(kv.AccountsDomain, b, txNum)
		default:
			putAccount(c, txNum)
		}
		_, err := domains.ComputeCommitment(ctx, rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles2(ctx, db, 0, steps-1, unboundedFinalityCtx, false))
	require.Eventually(t, func() bool { return agg.EndTxNumMinimax() >= stepSize*(steps-1) }, 30*time.Second, 50*time.Millisecond)

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
	cnt, err := state.FoldCommitmentV3(ctx, agg, math.MaxUint64)
	require.NoError(t, err)
	require.Zero(t, cnt.Orphans)
}

func commitmentFilesByEnd(t *testing.T, snapDomain string) []string {
	t.Helper()
	kvs, err := filepath.Glob(filepath.Join(snapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	rng := func(p string) snaptype.FileInfo {
		info, _, ok := snaptype.ParseFileName("", p)
		require.Truef(t, ok, "bad commitment file name %s", p)
		return info
	}
	slices.SortFunc(kvs, func(a, b string) int {
		ai, bi := rng(a), rng(b)
		return cmp.Or(cmp.Compare(ai.To, bi.To), cmp.Compare(ai.From, bi.From))
	})
	return kvs
}

func convertFixture(t *testing.T, db kv.TemporalRwDB, agg *state.Aggregator, mode string) {
	t.Helper()
	switch mode {
	case "native":
		return
	case "converted_keysV2":
		runOrchestrator(t, db, state.ConvertOpts{TargetNibblesV2: true})
		require.NoError(t, dir.RemoveAll(filepath.Join(agg.Dirs().Snap, "backup", "domains")))
	}
	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
}

func buildRandomWriteFiles(t *testing.T, mode string) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	const stepSize, steps = 4, 8
	useCommitmentSchema(t, mode == "native")
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	_, _, putAccount := domainWriter(t, domains, rwTx)
	addrs, _ := generateInputData(t, length.Addr, 1, 64)
	rnd := newRnd(7)
	for txNum := range uint64(stepSize * steps) {
		if txNum == 0 {
			for _, a := range addrs {
				putAccount(a, txNum)
			}
		}
		putAccount(addrs[rnd.IntN(len(addrs))], txNum)
		if txNum%stepSize == stepSize-1 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))
	convertFixture(t, db, agg, mode)
	return db, agg
}

func TestConvertCommitmentFiles_V3PrefixFoldMatchesStateRecord(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	fixtures := map[string]func(t *testing.T, mode string) (kv.TemporalRwDB, *state.Aggregator){
		"random_writes":                buildRandomWriteFiles,
		"storage_deleted":              wipedStorage(wipeCase{}),
		"storage_touched_then_deleted": wipedStorage(wipeCase{touchFirst: true}),
		"storage_recreated":            wipedStorage(wipeCase{recreateAt: 4, incarnation: 2, slots: recreatedSlots}),
		"recreated_same_incarnation":   wipedStorage(wipeCase{recreateAt: 4, incarnation: 1, slots: recreatedSlots}),
		"recreated_identical_later":    wipedStorage(wipeCase{recreateAt: 8, incarnation: 2, slots: oldSlots}),
		"recreated_subtree_later":      wipedStorage(wipeCase{recreateAt: 8, incarnation: 2, slots: append(oldSlots[:3:3], []byte{0x5, 0x7})}),
	}
	for _, name := range slices.Sorted(maps.Keys(fixtures)) {
		for _, mode := range []string{"converted", "converted_keysV2", "native"} {
			t.Run(name+"/"+mode, func(t *testing.T) {
				testPrefixFoldMatchesStateRecord(t, fixtures[name], mode)
			})
		}
	}
}

func testPrefixFoldMatchesStateRecord(t *testing.T, build func(t *testing.T, mode string) (kv.TemporalRwDB, *state.Aggregator), mode string) {
	db, agg := build(t, mode)
	latest, err := state.FoldCommitmentV3(t.Context(), agg, math.MaxUint64)
	require.NoError(t, err)
	require.Zero(t, latest.Orphans)
	kvs := commitmentFilesByEnd(t, agg.Dirs().SnapDomain)
	require.Greater(t, len(kvs), 1)
	type result struct {
		name, folded, recorded string
		err                    error
	}
	var results []result
	for _, path := range slices.Backward(kvs) {
		keys, vals := readKVFile(t, agg, path)
		idx := slices.IndexFunc(keys, func(k []byte) bool { return bytes.Equal(k, commitment.KeyCommitmentV3State) })
		require.GreaterOrEqual(t, idx, 0, "%s has no v3 state record", filepath.Base(path))
		_, _, recorded, err := commitment.DecodeCommitmentV3State(vals[idx])
		require.NoError(t, err)
		c, foldErr := state.FoldCommitmentV3(t.Context(), agg, math.MaxUint64)
		results = append(results, result{filepath.Base(path), fmt.Sprintf("%x", c.Root), fmt.Sprintf("%x", recorded), foldErr})

		base := strings.TrimSuffix(path, ".kv")
		for _, ext := range []string{".kv", ".bt", ".kvei", ".kvi"} {
			if rmErr := dir.RemoveFile(base + ext); rmErr != nil {
				require.ErrorIs(t, rmErr, os.ErrNotExist)
			}
		}
		require.NoError(t, agg.OpenFolder(db))
	}
	var failed []string
	for _, r := range slices.Backward(results) {
		t.Logf("file=%s folded=%s record=%s err=%v", r.name, r.folded, r.recorded, r.err)
		if r.err != nil || r.folded != r.recorded {
			failed = append(failed, r.name)
		}
	}
	require.Empty(t, failed, "files whose prefix fold does not match their own v3 state record")
}

var (
	oldSlots       = [][]byte{{0xa, 0x1, 0x2}, {0xa, 0x1, 0x5}, {0xa, 0x3}, {0x4, 0x2}, {0x4, 0x9}}
	recreatedSlots = [][]byte{{0x5, 0x1}, {0x5, 0x2}, {0xc}}
)

type wipeCase struct {
	recreateAt, incarnation uint64
	slots                   [][]byte
	touchFirst              bool
}

func wipedStorage(c wipeCase) func(t *testing.T, mode string) (kv.TemporalRwDB, *state.Aggregator) {
	return func(t *testing.T, mode string) (kv.TemporalRwDB, *state.Aggregator) {
		return buildWipedStorageFiles(t, mode, c)
	}
}

func buildWipedStorageFiles(t *testing.T, mode string, c wipeCase) (kv.TemporalRwDB, *state.Aggregator) {
	t.Helper()
	const stepSize, steps, wipeTx = 4, 8, 17
	useCommitmentSchema(t, mode == "native")
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	addrs, _ := generateInputData(t, length.Addr, 1, 16)
	next := uint64(1)
	owner := addressWithHashPrefix(t, []byte{0x7}, &next)
	slotsOf := func(prefixes ...[]byte) [][]byte {
		var slots [][]byte
		for _, p := range prefixes {
			slots = append(slots, append(bytes.Clone(owner), storageSlotWithHashPrefix(t, p, &next)...))
		}
		return slots
	}
	old := slotsOf(oldSlots...)
	recreated := slotsOf(c.slots...)
	for i, p := range c.slots {
		if j := slices.IndexFunc(oldSlots, func(o []byte) bool { return bytes.Equal(o, p) }); j >= 0 {
			recreated[i] = old[j]
		}
	}
	putOwner := func(incarnation, txNum uint64) {
		acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(txNum + 1), CodeHash: accounts.EmptyCodeHash, Incarnation: incarnation}
		put(kv.AccountsDomain, owner, accounts.SerialiseV3(&acc), txNum)
	}
	deleteTx := uint64(wipeTx)
	if c.touchFirst {
		deleteTx += stepSize
	}
	rnd := newRnd(3)
	for txNum := range uint64(stepSize * steps) {
		switch {
		case txNum == 0:
			putOwner(1, txNum)
			for _, s := range old {
				put(kv.StorageDomain, s, []byte{1}, txNum)
			}
		case c.touchFirst && txNum == wipeTx:
			put(kv.StorageDomain, old[0], []byte{3}, txNum)
		case txNum == deleteTx:
			for _, s := range old {
				del(kv.StorageDomain, s, txNum)
			}
			del(kv.AccountsDomain, owner, txNum)
		case c.recreateAt != 0 && txNum == wipeTx+c.recreateAt:
			putOwner(c.incarnation, txNum)
			for _, s := range recreated {
				put(kv.StorageDomain, s, []byte{1}, txNum)
			}
		}
		putAccount(addrs[rnd.IntN(len(addrs))], txNum)
		if txNum%stepSize == stepSize-1 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))
	convertFixture(t, db, agg, mode)
	return db, agg
}

func TestConvertCommitmentFiles_V3ExtensionRoot(t *testing.T) {
	const stepSize, steps = 4, 2
	useCommitmentSchema(t, false)
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	_, _, putAccount := domainWriter(t, domains, rwTx)
	next := uint64(1)
	addrs := [][]byte{addressWithHashPrefix(t, []byte{4}, &next), addressWithHashPrefix(t, []byte{4}, &next)}
	for txNum := range uint64(stepSize * steps) {
		for _, a := range addrs {
			putAccount(a, txNum)
		}
		if txNum%stepSize == stepSize-1 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
	c, err := state.FoldCommitmentV3(t.Context(), agg, math.MaxUint64)
	require.NoError(t, err)
	kvs := commitmentFilesByEnd(t, agg.Dirs().SnapDomain)
	keys, vals := readKVFile(t, agg, kvs[len(kvs)-1])
	idx := slices.IndexFunc(keys, func(k []byte) bool { return bytes.Equal(k, commitment.KeyCommitmentV3State) })
	require.GreaterOrEqual(t, idx, 0)
	_, _, recorded, err := commitment.DecodeCommitmentV3State(vals[idx])
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%x", recorded), fmt.Sprintf("%x", c.Root))
}

func TestConvertCommitmentFiles_V3LeafRootFailsBeforeSwap(t *testing.T) {
	const stepSize, steps = 4, 2
	useCommitmentSchema(t, false)
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	_, _, putAccount := domainWriter(t, domains, rwTx)
	next := uint64(1)
	addrs := [][]byte{addressWithHashPrefix(t, []byte{4}, &next)}
	for txNum := range uint64(stepSize * steps) {
		for _, a := range addrs {
			putAccount(a, txNum)
		}
		if txNum%stepSize == stepSize-1 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

	kvs := commitmentFilesByEnd(t, agg.Dirs().SnapDomain)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback) //nolint:gocritic
	_, err = state.ConvertCommitmentFiles(t.Context(), state.AggTx(tx), state.ConvertOpts{TargetV3: true}, log.New())
	tx.Rollback()
	require.Error(t, err)
	require.Equal(t, kvs, commitmentFilesByEnd(t, agg.Dirs().SnapDomain))
}

func TestConvertCommitmentFiles_V3ExtensionRootReplacedLater(t *testing.T) {
	for _, name := range []string{"branch", "empty"} {
		t.Run(name, func(t *testing.T) {
			const stepSize, steps = 1, 4
			useCommitmentSchema(t, false)
			db, agg, rwTx, domains := convertTestDomains(t, stepSize)
			_, del, putAccount := domainWriter(t, domains, rwTx)
			next := uint64(1)
			addrs := [][]byte{addressWithHashPrefix(t, []byte{4}, &next), addressWithHashPrefix(t, []byte{4}, &next)}
			other := addressWithHashPrefix(t, []byte{9}, &next)
			for txNum := range uint64(stepSize * steps) {
				switch {
				case txNum < stepSize*2:
					for _, a := range addrs {
						putAccount(a, txNum)
					}
				case name == "branch":
					putAccount(other, txNum)
				case txNum == stepSize*2:
					for _, a := range addrs {
						del(kv.AccountsDomain, a, txNum)
					}
				}
				_, err := domains.ComputeCommitment(t.Context(), rwTx, true, txNum, txNum, "", nil)
				require.NoError(t, err)
			}
			require.NoError(t, domains.Flush(t.Context(), rwTx))
			require.NoError(t, rwTx.Commit())
			require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, steps-1, unboundedFinalityCtx, false))
			require.Eventually(t, func() bool { return agg.EndTxNumMinimax() >= stepSize*(steps-1) }, 30*time.Second, 50*time.Millisecond)

			runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})
			c, err := state.FoldCommitmentV3(t.Context(), agg, math.MaxUint64)
			require.NoError(t, err)
			kvs := commitmentFilesByEnd(t, agg.Dirs().SnapDomain)
			keys, vals := readKVFile(t, agg, kvs[len(kvs)-1])
			idx := slices.IndexFunc(keys, func(k []byte) bool { return bytes.Equal(k, commitment.KeyCommitmentV3State) })
			require.GreaterOrEqual(t, idx, 0)
			_, _, recorded, err := commitment.DecodeCommitmentV3State(vals[idx])
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%x", recorded), fmt.Sprintf("%x", c.Root))
			require.Greater(t, len(kvs), 1)
		})
	}
}
