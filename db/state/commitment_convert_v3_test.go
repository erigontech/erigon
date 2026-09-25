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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	v3 "github.com/erigontech/erigon/execution/commitment/v3"
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
