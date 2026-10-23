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

package app

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/internal/commitmenttest/commitmentflags"
)

func TestStepRebaseIncreaseKeepsDualCommitment(t *testing.T) {
	commitmentflags.Restore(t)
	const stepSize, stepsInFrozenFile, factor = uint64(1), uint64(64), uint64(10)
	const newStepSize = stepSize * factor
	logger := log.New()
	fixture, err := execmoduletester.NewPBTAcceptanceChainWithStepSizeAndFrozenBlocks(t, false, true, stepSize, stepsInFrozenFile, 54)
	require.NoError(t, err)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
	dirs := fixture.Tester.Dirs
	top := fixture.Chain.TopBlock.NumberU64()
	want := make(map[uint64][]byte, top)
	require.NoError(t, fixture.Tester.DB.View(t.Context(), func(tx kv.Tx) error {
		for block := uint64(1); block <= top; block++ {
			root, err := rawdb.ReadShadowStateRoot(tx, fixture.Chain.Blocks[block-1].Hash(), block)
			if err != nil {
				return err
			}
			want[block] = root
		}
		return nil
	}))
	require.NotNil(t, want[top])
	var lastTxNum uint64
	wantState := make(map[uint64]map[string][]byte)
	require.NoError(t, fixture.Tester.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		var err error
		if _, lastTxNum, err = rawdbv3.TxNums.Last(tx); err != nil {
			return err
		}
		for txNum := uint64(0); txNum <= lastTxNum; txNum++ {
			wantState[txNum] = collectStateAt(t, tx, txNum)
		}
		return nil
	}))
	buildMergedStateFiles(t, fixture)

	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	var end uint64
	require.NoError(t, withStepRebaseAgg(t.Context(), dirs, true, logger, func(agg *state.Aggregator) (err error) {
		end, err = agg.StepAlignedEnd(newStepSize)
		return err
	}))
	require.NotZero(t, end)
	require.Positive(t, countMisalignedFiles(t, dirs.SnapDomain, end/stepSize, factor), "files below the aligned end must need a merge")
	plan, err := buildStepRebasePlan(dirs, settings, newStepSize, true, end, logger)
	require.NoError(t, err)
	tail := filesPastStep(t, dirs.SnapDomain, end/stepSize)
	require.NotEmpty(t, tail)
	for _, path := range tail {
		require.Contains(t, plan.deletions, path)
	}

	stray := filepath.Join(dirs.SnapDomain, "v1.0-strayfile.33-35.kv")
	writeTestFile(t, stray)
	require.ErrorContains(t, applyStepRebasePlan(t.Context(), dirs, plan, logger), "does not start and end at a multiple of 10 steps")
	for _, path := range tail {
		require.FileExists(t, path)
	}
	untouched, err := state.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	require.Equal(t, stepSize, untouched.StepSize)
	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	require.NoError(t, chainDB.View(t.Context(), func(tx kv.Tx) error {
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		require.NotZero(t, progress)
		return err
	}))
	chainDB.Close()
	require.NoError(t, dir.RemoveFile(stray))
	late := filepath.Join(dirs.SnapDomain, "v1.0-strayfile.161-162.kv")
	writeTestFile(t, late)
	require.NotContains(t, plan.deletions, late)
	require.NoError(t, applyStepRebasePlan(t.Context(), dirs, plan, logger))
	require.NoFileExists(t, late)

	rebased, err := state.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	require.Equal(t, newStepSize, rebased.StepSize)
	require.Equal(t, state.TrieVariantHexBin, rebased.TrieVariantName())
	require.Equal(t, settings.TrieHash, rebased.TrieHash)
	for _, root := range stateSnapDirs(dirs) {
		entries, err := os.ReadDir(root)
		require.NoError(t, err)
		for _, entry := range entries {
			matches := stepFileRe.FindStringSubmatch(entry.Name())
			if matches == nil {
				continue
			}
			to, err := strconv.ParseUint(matches[4], 10, 64)
			require.NoError(t, err)
			require.LessOrEqualf(t, to, end/newStepSize, "%s past the aligned end", entry.Name())
		}
	}

	chainDB = dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	require.NoError(t, chainDB.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.ClearTable(kv.ShadowStateRoot)
	}))
	chainDB.Close()
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(dirs),
		execmoduletester.WithGenesisSpec(fixture.Genesis),
		execmoduletester.WithKey(fixture.Key),
		execmoduletester.WithStepSize(newStepSize),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	defer reopened.Close()
	require.NoError(t, reopened.ReExecuteTo(t.Context(), top))
	reexecuted := 0
	require.NoError(t, reopened.DB.View(t.Context(), func(tx kv.Tx) error {
		for block := uint64(1); block <= top; block++ {
			root, err := rawdb.ReadShadowStateRoot(tx, fixture.Chain.Blocks[block-1].Hash(), block)
			if err != nil {
				return err
			}
			if root == nil {
				continue
			}
			reexecuted++
			require.Equalf(t, want[block], root, "shadow root at block %d", block)
		}
		return nil
	}))
	require.NotZero(t, reexecuted)
	require.NoError(t, reopened.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		for txNum := uint64(0); txNum <= lastTxNum; txNum++ {
			require.Equalf(t, wantState[txNum], collectStateAt(t, tx, txNum), "state as of txNum %d", txNum)
		}
		return nil
	}))
}

func collectStateAt(t *testing.T, tx kv.TemporalTx, txNum uint64) map[string][]byte {
	t.Helper()
	values := make(map[string][]byte)
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		it, err := tx.RangeAsOf(domain, nil, nil, txNum+1, order.Asc, kv.Unlim)
		require.NoError(t, err)
		for it.HasNext() {
			key, value, err := it.Next()
			require.NoError(t, err)
			if len(value) == 0 {
				value = []byte{}
			}
			values[domain.String()+"\x00"+string(key)] = bytes.Clone(value)
		}
		it.Close()
	}
	return values
}

func filesPastStep(t *testing.T, root string, endStep uint64) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	var past []string
	for _, entry := range entries {
		matches := stepFileRe.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		to, err := strconv.ParseUint(matches[4], 10, 64)
		require.NoError(t, err)
		if to > endStep {
			past = append(past, filepath.Join(root, entry.Name()))
		}
	}
	return past
}

func countMisalignedFiles(t *testing.T, root string, endStep, factor uint64) int {
	t.Helper()
	entries, err := os.ReadDir(root)
	require.NoError(t, err)
	n := 0
	for _, entry := range entries {
		matches := stepFileRe.FindStringSubmatch(entry.Name())
		if matches == nil {
			continue
		}
		to, err := strconv.ParseUint(matches[4], 10, 64)
		require.NoError(t, err)
		if to <= endStep && to%factor != 0 {
			n++
		}
	}
	return n
}

func buildMergedStateFiles(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain) {
	t.Helper()
	fixture.Tester.Close()
	dirs := fixture.Tester.Dirs
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	settings, err := state.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	agg := state.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	var lastTxNum uint64
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		lastTxNum, err = rawdbv3.TxNums.Max(t.Context(), tx, fixture.Chain.TopBlock.NumberU64())
		return err
	}))
	toStep := kv.Step(lastTxNum/settings.StepSize + 1)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, toStep, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), true))
	agg.WaitForFiles()
	db.Close()
	agg.Close()
	rawDB.Close()
}
