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
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
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
	require.NotEmpty(t, kvs)
	for _, p := range kvs {
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
	name := filepath.Base(path)
	_, err := fmt.Sscanf(name[strings.Index(name, "commitment.")+len("commitment."):], "%d-%d.kv", &from, &to)
	require.NoError(t, err)
	return from, to
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
		return int(af) - int(bf)
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
	for ; ; *next++ {
		slot := make([]byte, length.Hash)
		binary.BigEndian.PutUint64(slot[length.Hash-8:], *next)
		path := make([]byte, 2*length.Hash)
		nibbles.Expand(crypto.Keccak256(slot), path)
		if bytes.HasPrefix(path, prefix) {
			*next++
			return slot
		}
	}
}

func convertTestDomains(t *testing.T, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator, kv.TemporalRwTx, *execctx.SharedDomains) {
	t.Helper()
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

func testConvertCommitmentFilesV3CollapsedStorageBranch(t *testing.T, keysV2 bool) {
	const stepSize, steps = 4, 2
	db, agg, rwTx, domains := convertTestDomains(t, stepSize)
	put, del, putAccount := domainWriter(t, domains, rwTx)
	ctx := t.Context()

	addrs, _ := generateInputData(t, length.Addr, 1, 2)
	owner := addrs[0]
	next := uint64(1)
	live := [][]byte{
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x2}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x5}, &next),
		storageSlotWithHashPrefix(t, []byte{0x3}, &next),
	}
	collapsing := [][]byte{
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x0}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0x7, 0x9}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0xe, 0x5}, &next),
		storageSlotWithHashPrefix(t, []byte{0xe, 0x1, 0xe, 0xa}, &next),
	}
	slot := func(s []byte) []byte { return append(bytes.Clone(owner), s...) }
	for txNum := range uint64(stepSize * steps) {
		switch txNum {
		case 0:
			for _, a := range addrs {
				putAccount(a, txNum)
			}
			for _, s := range append(slices.Clone(live), collapsing...) {
				put(kv.StorageDomain, slot(s), []byte{1}, txNum)
			}
		case 1:
			for _, s := range append(slices.Clone(live), collapsing...) {
				del(kv.StorageDomain, slot(s), txNum)
			}
			del(kv.AccountsDomain, owner, txNum)
		case 2:
			putAccount(owner, txNum)
			for _, s := range live {
				put(kv.StorageDomain, slot(s), []byte{2}, txNum)
			}
		default:
			putAccount(addrs[1], txNum)
		}
		_, err := domains.ComputeCommitment(ctx, rwTx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(db, stepSize*steps, unboundedFinalityCtx))

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

func TestConvertCommitmentFiles_V3StateRecordInEveryFile(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	db, agg := testDbAggregatorWithFiles(t, &testAggConfig{stepSize: 10, disableCommitmentBranchTransform: true})
	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	kvs, err := filepath.Glob(filepath.Join(agg.Dirs().SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.Greater(t, len(kvs), 1)
	for _, p := range kvs {
		keys, _ := readKVFile(t, agg, p)
		has := slices.ContainsFunc(keys, func(k []byte) bool { return bytes.Equal(k, commitment.KeyCommitmentV3State) })
		require.Truef(t, has, "%s has no v3 state record", filepath.Base(p))
	}
}
