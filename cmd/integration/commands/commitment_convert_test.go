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
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	chaincfg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

var integrationRoot = sync.OnceValue(RootCommand)

func TestCommitmentConvertReturnsErrors(t *testing.T) {
	enabled := statecfg.ExperimentalCommitmentV3
	t.Cleanup(func() {
		convertSqueeze, convertNibblesV2, convertRestore, convertContinue, convertV3, statecfg.ExperimentalCommitmentV3 = false, false, false, false, false, enabled
		cmdCommitmentConvert.Flags().Lookup("squeeze").Changed = false
		cmdCommitmentConvert.Flags().Lookup("nibbles.v2").Changed = false
	})
	root := integrationRoot()
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet", "--restore", "--continue"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "--continue is mutually exclusive with --restore")

	convertRestore, convertContinue = false, false
	statecfg.ExperimentalCommitmentV3 = true
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet", "--v3"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "without ERIGON_COMMITMENT_V3=true")

	convertV3 = false
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet", "--restore"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "without ERIGON_COMMITMENT_V3=true")

	convertRestore = false
	statecfg.ExperimentalCommitmentV3 = false
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(filepath.Join(dirs.Snap, "backup"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "backup", ".restore_v3_reset_pending"), nil, 0o644))
	root.SetArgs([]string{"--log.dir.disable", "commitment", "convert", "--datadir", dirs.DataDir, "--chain", "mainnet", "--v3"})
	require.ErrorContains(t, root.ExecuteContext(t.Context()), "convert --restore")

	convertV3 = false
	statecfg.ExperimentalCommitmentV3 = true
	for _, mode := range [][]string{nil, {"--squeeze=true"}, {"--nibbles.v2=true"}} {
		root.SetArgs(append([]string{"--log.dir.disable", "commitment", "convert", "--datadir", t.TempDir(), "--chain", "mainnet"}, mode...))
		require.ErrorContains(t, root.ExecuteContext(t.Context()), "without ERIGON_COMMITMENT_V3=true", mode)
	}
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
	openSnapshotOnce = sync.Once{}
	require.ErrorContains(t, commitmentVerify(t.Context(), log.New()), "history is disabled")
}

func TestCommitmentVerifyRejectsAsOfBelowHistory(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	statecfg.EnableHistoricalCommitment()
	previousDatadir, previousChaindata, previousVerifyAsOf := datadirCli, chaindata, verifyAsOf
	t.Cleanup(func() { datadirCli, chaindata, verifyAsOf = previousDatadir, previousChaindata, previousVerifyAsOf })

	db, dirs := dbWithV3State(t)
	latest, err := state.FoldCommitmentV3(t.Context(), db.(state.HasAgg).Agg().(*state.Aggregator), math.MaxUint64)
	require.NoError(t, err)
	db.Close()
	var pruned []string
	for _, d := range []string{dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		files, err := filepath.Glob(filepath.Join(d, "*-commitment.*"))
		require.NoError(t, err)
		pruned = append(pruned, files...)
	}
	require.NotEmpty(t, pruned)
	for _, f := range pruned {
		require.NoError(t, dir.RemoveFile(f))
	}

	datadirCli, chaindata = dirs.DataDir, dirs.Chaindata
	for _, asOf := range []uint64{1, latest.TxNum} {
		verifyAsOf = []uint{uint(asOf)}
		openSnapshotOnce = sync.Once{}
		require.ErrorContains(t, commitmentVerify(t.Context(), log.New()), fmt.Sprintf("no commitment history covers txNum %d", asOf))
	}
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

func TestCommitmentConvertLegacyRestoreKeepsExecution(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousDatadir, previousChaindata := datadirCli, chaindata
	t.Cleanup(func() { datadirCli, chaindata = previousDatadir, previousChaindata })

	db, dirs := dbWithV3State(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 100)
	}))
	require.NoError(t, commitmentConvert(db, t.Context(), log.New(), state.ConvertOpts{TargetSqueeze: true}))
	db.Close()

	datadirCli, chaindata = dirs.DataDir, dirs.Chaindata
	require.NoError(t, commitmentRestore(t.Context(), log.New()))

	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	defer rawDB.Close()
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, err)
		require.Equal(t, uint64(100), progress)
		return nil
	}))
}

func TestCommitmentConvertRestoreResetsExecution(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousDatadir, previousChaindata := datadirCli, chaindata
	t.Cleanup(func() { datadirCli, chaindata = previousDatadir, previousChaindata })

	db, dirs := dbWithV3State(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 100)
	}))
	db.Close()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.NoError(t, os.Rename(f, filepath.Join(backupDir, filepath.Base(f))))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v3.0-commitment.0-2.kv"), nil, 0o644))

	datadirCli, chaindata = dirs.DataDir, dirs.Chaindata
	require.NoError(t, commitmentRestore(t.Context(), log.New()))

	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	defer rawDB.Close()
	var progress uint64
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		progress, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	}))
	require.Zero(t, progress)
}

func TestCommitmentConvertRestoreRetryResetsExecution(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousDatadir, previousChaindata := datadirCli, chaindata
	t.Cleanup(func() { datadirCli, chaindata = previousDatadir, previousChaindata })

	db, dirs := dbWithV3State(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 100)
	}))
	db.Close()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.NoError(t, os.Rename(f, filepath.Join(backupDir, filepath.Base(f))))
	}
	v3File := filepath.Join(dirs.SnapDomain, "v3.0-commitment.0-2.kv")
	require.NoError(t, os.WriteFile(v3File, nil, 0o644))
	blocked := filepath.Join(dirs.SnapDomain, filepath.Base(files[len(files)-1]))
	require.NoError(t, os.Mkdir(blocked, 0o755))
	blocker := filepath.Join(blocked, "blocker")
	require.NoError(t, os.WriteFile(blocker, nil, 0o644))

	datadirCli, chaindata = dirs.DataDir, dirs.Chaindata
	require.ErrorContains(t, commitmentRestore(t.Context(), log.New()), "restore mv")
	require.NoFileExists(t, v3File)
	require.FileExists(t, filepath.Join(backupDir, filepath.Base(blocked)))
	require.NoError(t, dir.RemoveFile(blocker))
	require.NoError(t, dir.RemoveFile(blocked))
	require.NoError(t, commitmentRestore(t.Context(), log.New()))

	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	defer rawDB.Close()
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, err)
		require.Zero(t, progress)
		return nil
	}))
}

func TestCommitmentConvertRestoreRetryAfterFilesResetsExecution(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	previousDatadir, previousChaindata := datadirCli, chaindata
	t.Cleanup(func() { datadirCli, chaindata = previousDatadir, previousChaindata })

	db, dirs := dbWithV3State(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, 100)
	}))
	db.Close()
	backupDir := filepath.Join(dirs.Snap, "backup", "domains")
	require.NoError(t, os.MkdirAll(backupDir, 0o755))
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment.*"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	for _, f := range files {
		require.NoError(t, os.Rename(f, filepath.Join(backupDir, filepath.Base(f))))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v3.0-commitment.0-2.kv"), nil, 0o644))
	restoredV3, err := state.RestoreCommitmentFiles(t.Context(), dirs, log.New())
	require.NoError(t, err)
	require.True(t, restoredV3)
	require.NoDirExists(t, backupDir)

	datadirCli, chaindata = dirs.DataDir, dirs.Chaindata
	require.NoError(t, commitmentRestore(t.Context(), log.New()))

	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	defer rawDB.Close()
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, err)
		require.Zero(t, progress)
		return nil
	}))
	require.NoFileExists(t, filepath.Join(dirs.Snap, "backup", ".restore_v3_reset_pending"))
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
	agg := fillAccountState(t, db, 64)
	require.NoError(t, agg.BuildFiles(db, 48, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)))
	return db, dirs
}
