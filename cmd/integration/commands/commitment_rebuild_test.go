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

package commands

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

func dbWithState(t *testing.T, buildFiles bool) (kv.TemporalRwDB, datadir.Dirs, func() uint64) {
	t.Helper()
	const stepSize, txs, filed = 16, 64, 48
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(stepSize))
	agg, _ := fillAccountState(t, db, txs)
	ctx := t.Context()
	if buildFiles {
		require.NoError(t, agg.BuildFiles(db, filed, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))
	}

	accountTables := db.Debug().DomainTables(kv.AccountsDomain)
	countTail := func() (n uint64) {
		roTx, err := db.BeginRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		for _, table := range accountTables {
			c, err := roTx.Count(table)
			require.NoError(t, err)
			n += c
		}
		return n
	}
	require.NotZero(t, countTail(), "the accounts domain keeps txNums past the files in the DB")
	return db, dirs, countTail
}

func fillAccountState(t *testing.T, db kv.TemporalRwDB, txs uint64) (*state.Aggregator, []common.Hash) {
	t.Helper()
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	tx, domains := temporaltest.NewTestTxSD(t, db)
	roots := make([]common.Hash, txs)
	for txNum := range txs {
		addr := make([]byte, 20)
		binary.BigEndian.PutUint64(addr[12:], txNum%5+1)
		acc := accounts.Account{Nonce: txNum + 1, Balance: *uint256.NewInt(txNum + 1), CodeHash: accounts.EmptyCodeHash}
		prev, _, err := domains.GetLatest(kv.AccountsDomain, tx, addr)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addr, accounts.SerialiseV3(&acc), txNum, prev))
		root, err := domains.ComputeCommitment(t.Context(), tx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
		roots[txNum] = common.Hash(root)
		require.NoError(t, rawdbv3.TxNums.Append(tx, txNum, txNum))
	}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, txs-1))
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	return agg, roots
}

func requireExecutionReset(t *testing.T, db kv.TemporalRwDB, countTail func() uint64) {
	t.Helper()
	require.Zero(t, countTail(), "the files-only rewrite covers only the files, so the DB tail past them must be gone")
	roTx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	progress, err := stages.GetStageProgress(roTx, stages.Execution)
	require.NoError(t, err)
	require.Zero(t, progress)
}

func TestCommitmentRebuildWithoutHistoryDropsTheDBTail(t *testing.T) {
	db, dirs, countTail := dbWithState(t, true)
	snapCfg := ethconfig.Defaults.Snapshot
	snapCfg.ChainName = networkname.Test
	blockSnaps := blocksnapshots.NewRoSnapshots(snapCfg, dirs.Snap, log.Root())
	t.Cleanup(blockSnaps.Close)
	openBlockReaderOnce.Do(func() {
		_blockReaderSingleton, _blockWriterSingleton = freezeblocks.NewBlockReader(blockSnaps), blockio.NewBlockWriter()
	})
	prevDatadir, prevNoHistory, prevYes := datadirCli, noHistory, yes
	t.Cleanup(func() { datadirCli, noHistory, yes = prevDatadir, prevNoHistory, prevYes })
	datadirCli, noHistory, yes = dirs.DataDir, true, true

	require.NoError(t, commitmentRebuild(db, t.Context(), log.New()))
	requireExecutionReset(t, db, countTail)
}

func TestCommitmentConvertV3DropsTheDBTail(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, _, countTail := dbWithState(t, true)
	require.NoError(t, commitmentConvert(db, t.Context(), log.New(), state.ConvertOpts{TargetV3: true}))
	requireExecutionReset(t, db, countTail)
}

func TestCommitmentConvertV3RefusedKeepsTheDBTail(t *testing.T) {
	db, dirs, countTail := dbWithState(t, true)
	before := countTail()
	backup := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backup, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(backup, "stale"), []byte{1}, 0o644))
	require.Error(t, commitmentConvert(db, t.Context(), log.New(), state.ConvertOpts{TargetV3: true}))
	require.Equal(t, before, countTail())
}

func TestCommitmentConvertV3WithoutFilesKeepsTheDBTail(t *testing.T) {
	db, _, countTail := dbWithState(t, false)
	before := countTail()
	require.NoError(t, commitmentConvert(db, t.Context(), log.New(), state.ConvertOpts{TargetV3: true}))
	require.Equal(t, before, countTail())
}

func TestCommitmentClearKeepsTheDBTail(t *testing.T) {
	db, dirs, countTail := dbWithState(t, true)
	snapCfg := ethconfig.Defaults.Snapshot
	snapCfg.ChainName = networkname.Test
	blockSnaps := blocksnapshots.NewRoSnapshots(snapCfg, dirs.Snap, log.Root())
	t.Cleanup(blockSnaps.Close)
	openBlockReaderOnce.Do(func() {
		_blockReaderSingleton, _blockWriterSingleton = freezeblocks.NewBlockReader(blockSnaps), blockio.NewBlockWriter()
	})
	prevDatadir, prevClear, prevYes := datadirCli, clearCommitment, yes
	t.Cleanup(func() { datadirCli, clearCommitment, yes = prevDatadir, prevClear, prevYes })
	datadirCli, clearCommitment, yes = dirs.DataDir, true, true

	tail := countTail()
	require.NoError(t, commitmentRebuild(db, t.Context(), log.New()))
	require.Equal(t, tail, countTail(), "--clear-commitment removes commitment only")
}

func TestCommitmentRebuildWithoutHistoryAfterPrunedHistoryRefusal(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	statecfg.EnableHistoricalCommitment()
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(16))
	agg, roots := fillAccountState(t, db, 56)
	require.NoError(t, agg.BuildFiles(db, 56, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))
	agg.CloseFilesNoReopen()
	for _, d := range []string{dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		for _, domain := range []string{"commitment", "accounts", "storage", "code"} {
			files, err := filepath.Glob(filepath.Join(d, "*-"+domain+".0-*"))
			require.NoError(t, err)
			for _, f := range files {
				require.NoError(t, dir.RemoveFile(f))
			}
		}
	}
	require.NoError(t, agg.ReloadFiles())

	err := commitmentConvert(db, t.Context(), log.New(), state.ConvertOpts{TargetV3: true})
	require.ErrorContains(t, err, "then run `integration commitment rebuild --yes --no-history` with ERIGON_COMMITMENT_V3=true")

	agg.CloseFilesNoReopen()
	require.NoError(t, app.DeleteStateSnapshots(app.DeleteStateSnapshotsArgs{Dirs: dirs, DomainNames: []string{"commitment"}}))
	require.NoError(t, agg.ReloadFiles())
	snapCfg := ethconfig.Defaults.Snapshot
	snapCfg.ChainName = networkname.Test
	blockSnaps := blocksnapshots.NewRoSnapshots(snapCfg, dirs.Snap, log.Root())
	t.Cleanup(blockSnaps.Close)
	openBlockReaderOnce.Do(func() {
		_blockReaderSingleton, _blockWriterSingleton = freezeblocks.NewBlockReader(blockSnaps), blockio.NewBlockWriter()
	})
	prevDatadir, prevNoHistory, prevYes := datadirCli, noHistory, yes
	t.Cleanup(func() { datadirCli, noHistory, yes = prevDatadir, prevNoHistory, prevYes })
	datadirCli, noHistory, yes = dirs.DataDir, true, true
	t.Cleanup(statecfg.SetCommitmentV3(true))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for block, root := range roots {
			header := &types.Header{Number: *uint256.NewInt(uint64(block)), Root: root}
			if err := rawdb.WriteHeader(tx, header); err != nil {
				return err
			}
			if err := rawdb.WriteCanonicalHash(tx, header.Hash(), uint64(block)); err != nil {
				return err
			}
		}
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))

	require.NoError(t, commitmentRebuild(db, t.Context(), log.New()))
	commitmentHistory, err := filepath.Glob(filepath.Join(dirs.SnapHistory, "*-commitment.*.v"))
	require.NoError(t, err)
	require.Empty(t, commitmentHistory, "--no-history rebuilds the domain only")
	checks, err := state.FoldCommitmentV3(t.Context(), agg, []uint64{math.MaxUint64}, nil)
	require.NoError(t, err)
	c := checks[0]
	require.Equal(t, uint64(47), c.TxNum)
	require.Equal(t, roots[c.TxNum], common.Hash(c.Root))
}
