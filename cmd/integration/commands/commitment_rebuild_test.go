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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

func dbWithStateTail(t *testing.T) (kv.TemporalRwDB, datadir.Dirs, func() uint64) {
	t.Helper()
	const stepSize, txs, filed = 16, 64, 48
	dirs := datadir.New(t.TempDir())
	db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(stepSize))
	agg := db.(state.HasAgg).Agg().(*state.Aggregator)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	ctx := t.Context()

	tx, domains := temporaltest.NewTestTxSD(t, db)
	for txNum := range uint64(txs) {
		addr := make([]byte, 20)
		binary.BigEndian.PutUint64(addr[12:], txNum%5+1)
		acc := accounts.Account{Nonce: txNum + 1, Balance: *uint256.NewInt(txNum + 1), CodeHash: accounts.EmptyCodeHash}
		prev, _, err := domains.GetLatest(kv.AccountsDomain, tx, addr)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addr, accounts.SerialiseV3(&acc), txNum, prev))
		_, err = domains.ComputeCommitment(ctx, tx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
		require.NoError(t, rawdbv3.TxNums.Append(tx, txNum, txNum))
	}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, txs-1))
	require.NoError(t, domains.Flush(ctx, tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	require.NoError(t, agg.BuildFiles(db, filed, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))

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
	selectPBTHexCommandSuite(t)
	db, dirs, countTail := dbWithStateTail(t)
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

	require.NoError(t, commitmentRebuild(db, t.Context(), log.New(), hexTarget(t), nil))
	requireExecutionReset(t, db, countTail)
}

func TestCommitmentConvertV3DropsTheDBTail(t *testing.T) {
	db, _, countTail := dbWithStateTail(t)
	require.NoError(t, commitmentConvert(db, t.Context(), log.New(), state.ConvertOpts{TargetV3: true}))
	requireExecutionReset(t, db, countTail)
}
