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
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	chaincfg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestCommitmentConvertReturnsErrors(t *testing.T) {
	enabled := statecfg.ExperimentalCommitmentV3
	t.Cleanup(func() {
		convertRestore, convertContinue, convertV3, statecfg.ExperimentalCommitmentV3 = false, false, false, enabled
	})
	root := RootCommand()
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet", "--restore", "--continue"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "--continue is mutually exclusive with --restore")

	convertRestore, convertContinue = false, false
	statecfg.ExperimentalCommitmentV3 = true
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet", "--v3"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "without COMMITMENT_V3=true")
}

func TestCommitmentVerifyRejectsAsOfWithoutHistory(t *testing.T) {
	enabled := statecfg.ExperimentalCommitmentV3
	previousDatadir, previousChaindata, previousVerifyAsOf := datadirCli, chaindata, verifyAsOf
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = enabled
		datadirCli, chaindata, verifyAsOf = previousDatadir, previousChaindata, previousVerifyAsOf
	})
	statecfg.ExperimentalCommitmentV3 = true
	db, dirs := dbWithV3State(t)
	dbPath := db.Path()
	db.Close()
	datadirCli = dirs.DataDir
	chaindata = dbPath
	verifyAsOf = []uint{0}
	require.ErrorContains(t, commitmentVerify(t.Context(), log.New()), "history is disabled")
}

func TestNewTemporalDBRejectsV3FilesWhenV3IsDisabled(t *testing.T) {
	previous := statecfg.ExperimentalCommitmentV3
	statecfg.ExperimentalCommitmentV3 = false
	t.Cleanup(func() { statecfg.ExperimentalCommitmentV3 = previous })

	db, dirs := dbWithV3State(t)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v3.0-commitment.0-1.kv"), nil, 0o644))
	previousDatadir := datadirCli
	datadirCli = dirs.DataDir
	t.Cleanup(func() { datadirCli = previousDatadir })

	require.ErrorContains(t, func() error {
		_, err := newTemporalDB(t.Context(), db, log.New())
		return err
	}(), "requires commitment v3")
}

func dbWithV3State(t *testing.T) (kv.TemporalRwDB, datadir.Dirs) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	t.Cleanup(rawDB.Close)
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, common.Hash{}, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, common.Hash{}, chaincfg.TestChainOsakaConfig)
	}))
	snapCfg := ethconfig.Defaults.Snapshot
	snapCfg.ChainName = networkname.Test
	blockSnapshots := blocksnapshots.NewRoSnapshots(snapCfg, dirs.Snap, log.Root())
	require.NoError(t, blockSnapshots.OpenFolder())
	t.Cleanup(blockSnapshots.Close)
	stateSnapshots := state.NewTest(dirs).StepSize(16).MustOpen(t.Context())
	t.Cleanup(stateSnapshots.Close)
	salt := []byte{0, 0, 0, 0}
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-blocks.txt"), salt, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), salt, 0o644))
	db, err := temporal.New(rawDB, stateSnapshots, blockSnapshots)
	require.NoError(t, err)
	require.NoError(t, db.OpenStateSnapshots(t.Context()))
	t.Cleanup(db.Close)
	agg := db.Agg().(*state.Aggregator)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	tx, domains := temporaltest.NewTestTxSD(t, db)
	for txNum := range uint64(64) {
		addr := make([]byte, 20)
		binary.BigEndian.PutUint64(addr[12:], txNum%5+1)
		acc := accounts.Account{Nonce: txNum + 1, Balance: *uint256.NewInt(txNum + 1), CodeHash: accounts.EmptyCodeHash}
		prev, _, err := domains.GetLatest(kv.AccountsDomain, tx, addr)
		require.NoError(t, err)
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, addr, accounts.SerialiseV3(&acc), txNum, prev))
		_, err = domains.ComputeCommitment(t.Context(), tx, true, txNum, txNum, "", nil)
		require.NoError(t, err)
		require.NoError(t, rawdbv3.TxNums.Append(tx, txNum, txNum))
	}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 63))
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	require.NoError(t, agg.BuildFiles(db, 48, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))
	return db, dirs
}
