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
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestStepRebaseIncreaseMergesStateFiles(t *testing.T) {
	const stepSize, factor, steps = uint64(10), uint64(10), uint64(23)
	const newStepSize = stepSize * factor
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, stepSize, 64)
	_, err := state.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(t, err)
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)

	db, closeDB := openTestTemporalDB(t, dirs, settings)
	writeAccountSteps(t, db, steps*stepSize)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, kv.Step(steps), execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), true))
	agg.WaitForFiles()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 7)
	}))
	closeDB()

	var end uint64
	require.NoError(t, withStepRebaseAgg(t.Context(), dirs, true, logger, func(agg *state.Aggregator) (err error) {
		end, err = agg.StepAlignedEnd(newStepSize)
		return err
	}))
	require.Equal(t, 20*stepSize, end)
	require.Positive(t, countMisalignedFiles(t, dirs.SnapDomain, end/stepSize, factor), "files below the aligned end must need a merge")
	db, closeDB = openTestTemporalDB(t, dirs, settings)
	want := make(map[uint64]map[string][]byte, end)
	require.NoError(t, db.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		for txNum := uint64(0); txNum < end; txNum++ {
			want[txNum] = collectStateAt(t, tx, txNum)
		}
		return nil
	}))
	closeDB()
	require.NotEmpty(t, want[end-1])

	plan, err := buildStepRebasePlan(dirs, settings, newStepSize, true, end, logger)
	require.NoError(t, err)
	tail := filesPastStep(t, dirs.SnapDomain, end/stepSize)
	require.NotEmpty(t, tail)
	for _, path := range tail {
		require.Contains(t, plan.deletions, path)
	}

	stray := filepath.Join(dirs.SnapDomain, "v1.0-strayfile.3-5.kv")
	writeTestFile(t, stray)
	require.ErrorContains(t, applyStepRebasePlan(t.Context(), dirs, plan, logger), "does not start and end at a multiple of 10 steps")
	for _, path := range tail {
		require.FileExists(t, path)
	}
	untouched, err := state.ResolveErigonDBSettings(dirs, logger, true)
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
	late := filepath.Join(dirs.SnapDomain, "v1.0-strayfile.21-22.kv")
	writeTestFile(t, late)
	require.NotContains(t, plan.deletions, late)
	require.NoError(t, applyStepRebasePlan(t.Context(), dirs, plan, logger))
	require.NoFileExists(t, late)

	rebased, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	require.Equal(t, newStepSize, rebased.StepSize)
	require.Empty(t, filesPastStep(t, dirs.SnapDomain, end/newStepSize))
	db, closeDB = openTestTemporalDB(t, dirs, rebased)
	defer closeDB()
	require.NoError(t, db.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		for txNum := uint64(0); txNum < end; txNum++ {
			require.Equalf(t, want[txNum], collectStateAt(t, tx, txNum), "state as of txNum %d", txNum)
		}
		return nil
	}))
}

func TestStepRebaseIncreaseMergesReceiptCacheFiles(t *testing.T) {
	const stepSize, factor, steps = uint64(10), uint64(10), uint64(23)
	const newStepSize = stepSize * factor
	saved := statecfg.Schema.RCacheDomain
	t.Cleanup(func() { statecfg.Schema.RCacheDomain = saved })
	statecfg.EnableHistoricalRCache()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, stepSize, 64)
	_, err := state.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(t, err)
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)

	db, closeDB := openTestTemporalDB(t, dirs, settings)
	writeAccountSteps(t, db, steps*stepSize)
	writeReceiptCacheSteps(t, db, steps*stepSize)
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, kv.Step(steps), execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), true))
	agg.WaitForFiles()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 7)
	}))
	closeDB()
	statecfg.Schema.RCacheDomain = saved
	unmerged, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-rcache.16-20.v"))
	require.NoError(t, err)
	require.Len(t, unmerged, 1)

	var end uint64
	require.NoError(t, withStepRebaseAgg(t.Context(), dirs, true, logger, func(agg *state.Aggregator) (err error) {
		end, err = agg.StepAlignedEnd(newStepSize)
		return err
	}))
	require.Equal(t, 20*stepSize, end)
	plan, err := buildStepRebasePlan(dirs, settings, newStepSize, true, end, logger)
	require.NoError(t, err)
	require.NoError(t, applyStepRebasePlan(t.Context(), dirs, plan, logger))
	merged, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-rcache.0-2.v"))
	require.NoError(t, err)
	require.Len(t, merged, 1)
}

func writeReceiptCacheSteps(t *testing.T, db kv.TemporalRwDB, txNums uint64) {
	t.Helper()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	var prev []byte
	for txNum := range txNums {
		value := binary.BigEndian.AppendUint64(nil, txNum+1)
		require.NoError(t, domains.DomainPut(kv.RCacheDomain, rwTx, []byte("receipt"), value, txNum, prev))
		prev = value
	}
	require.NoError(t, kvcfg.PersistReceipts.ForceWrite(rwTx, true))
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
}

func openTestTemporalDB(t *testing.T, dirs datadir.Dirs, settings *state.ErigonDBSettings) (kv.TemporalRwDB, func()) {
	t.Helper()
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	agg := state.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := temporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	return db, func() {
		db.Close()
		agg.Close()
		rawDB.Close()
	}
}

func writeAccountSteps(t *testing.T, db kv.TemporalRwDB, txNums uint64) {
	t.Helper()
	stepSize := db.(state.HasAgg).Agg().(*state.Aggregator).StepSize()
	rwTx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	for txNum := range txNums {
		for j := range uint64(8) {
			key := make([]byte, 20)
			key[0], key[19] = 0x10, byte(j)
			acc := accounts.Account{Nonce: txNum + 1, Balance: *uint256.NewInt(txNum*100_000 + j), CodeHash: accounts.EmptyCodeHash}
			prev, _, err := domains.GetLatest(kv.AccountsDomain, rwTx, key)
			require.NoError(t, err)
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, key, accounts.SerialiseV3(&acc), txNum, prev))
		}
		if (txNum+1)%stepSize == 0 {
			_, err := domains.ComputeCommitment(t.Context(), rwTx, true, 0, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	require.NoError(t, domains.Flush(t.Context(), rwTx))
	require.NoError(t, rwTx.Commit())
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
