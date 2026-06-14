package app

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func TestStepRebaseKeepBlocksResetsExecStateAndKeepsChaindata(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, 16, 4)
	_, err := state.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(t, err)
	writeTestChaindata(t, dirs)
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	plan, err := buildStepRebasePlan(dirs, settings, 8, true, logger)
	require.NoError(t, err)
	require.True(t, plan.resetExec)
	require.NotContains(t, plan.deletions, dirs.Chaindata)
	require.NoError(t, applyStepRebasePlan(context.Background(), dirs, plan, logger))
	require.DirExists(t, dirs.Chaindata)
	tx, err := openTestChaindata(t, dirs).BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	headerVal, err := tx.GetOne(kv.Headers, []byte("header-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("header-val"), headerVal)
	accVal, err := tx.GetOne(kv.TblAccountVals, []byte("acc-key"))
	require.NoError(t, err)
	require.Nil(t, accVal)
	csVal, err := tx.GetOne(kv.ChangeSets3, []byte("cs-key"))
	require.NoError(t, err)
	require.Nil(t, csVal)
	ppVal, err := tx.GetOne(kv.TblPruningProgress, []byte("pp-key"))
	require.NoError(t, err)
	require.Nil(t, ppVal)
	execProgress, err := tx.GetOne(kv.SyncStageProgress, []byte(stages.Execution))
	require.NoError(t, err)
	require.Nil(t, execProgress)
	sendersProgress, err := stages.GetStageProgress(tx, stages.Senders)
	require.NoError(t, err)
	require.Equal(t, uint64(9), sendersProgress)
	rebased, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	require.Equal(t, uint64(8), rebased.StepSize)
	require.Equal(t, uint64(8), rebased.StepsInFrozenFile)
}

func TestStepRebaseDeletesChaindataByDefault(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, 16, 4)
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-2.kv"))
	writeTestFile(t, filepath.Join(dirs.SnapHistory, "v1.0-accounts.0-2.v"))
	writeTestFile(t, filepath.Join(dirs.SnapAccessors, "v1.0-accounts.0-2.vi"))
	writeTestFile(t, filepath.Join(dirs.SnapIdx, "v1.0-accounts.0-2.ef"))
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-2.kv.torrent"))
	writeTestFile(t, filepath.Join(dirs.Snap, "erigondb.toml.torrent"))
	writeTestFile(t, filepath.Join(dirs.Chaindata, "mdbx.dat"))
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	plan, err := buildStepRebasePlan(dirs, settings, 8, false, logger)
	require.NoError(t, err)
	require.False(t, plan.resetExec)
	require.Contains(t, plan.deletions, dirs.Chaindata)
	require.NoError(t, applyStepRebasePlan(context.Background(), dirs, plan, logger))
	require.FileExists(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-4.kv"))
	require.FileExists(t, filepath.Join(dirs.SnapHistory, "v1.0-accounts.0-4.v"))
	require.FileExists(t, filepath.Join(dirs.SnapAccessors, "v1.0-accounts.0-4.vi"))
	require.FileExists(t, filepath.Join(dirs.SnapIdx, "v1.0-accounts.0-4.ef"))
	require.NoFileExists(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-2.kv"))
	require.NoFileExists(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-2.kv.torrent"))
	require.NoFileExists(t, filepath.Join(dirs.Snap, "erigondb.toml.torrent"))
	require.NoDirExists(t, dirs.Chaindata)
	rebased, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	require.Equal(t, uint64(8), rebased.StepSize)
	require.Equal(t, uint64(8), rebased.StepsInFrozenFile)
}

func TestStepRebaseKeepBlocksFailsFastWhenDatadirInUse(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, 16, 4)
	_, err := state.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(t, err)
	writeTestChaindata(t, dirs)
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	plan, err := buildStepRebasePlan(dirs, settings, 8, true, logger)
	require.NoError(t, err)
	unlock, err := dirs.TryFlock()
	require.NoError(t, err)
	defer unlock()
	err = applyStepRebasePlan(context.Background(), dirs, plan, logger)
	require.ErrorIs(t, err, datadir.ErrDataDirLocked)
	tx, err := openTestChaindata(t, dirs).BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	accVal, err := tx.GetOne(kv.TblAccountVals, []byte("acc-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("acc-val"), accVal)
	untouched, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	require.Equal(t, uint64(16), untouched.StepSize)
}

func TestStepRebaseKeepBlocksFailsWithoutChaindata(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, 16, 4)
	_, err := state.GetStateIndicesSalt(dirs, true, logger)
	require.NoError(t, err)
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	plan, err := buildStepRebasePlan(dirs, settings, 8, true, logger)
	require.NoError(t, err)
	err = applyStepRebasePlan(context.Background(), dirs, plan, logger)
	require.ErrorContains(t, err, "chaindata")
}

func writeTestErigonDBSettings(t *testing.T, dirs datadir.Dirs, stepSize, stepsInFrozenFile uint64) {
	t.Helper()
	content := fmt.Sprintf("step_size = %d\nsteps_in_frozen_file = %d\n", stepSize, stepsInFrozenFile)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte(content), 0644))
}

func writeTestChaindata(t *testing.T, dirs datadir.Dirs) {
	t.Helper()
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	defer db.Close()
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.Put(kv.Headers, []byte("header-key"), []byte("header-val")))
	require.NoError(t, tx.Put(kv.TblAccountVals, []byte("acc-key"), []byte("acc-val")))
	require.NoError(t, tx.Put(kv.ChangeSets3, []byte("cs-key"), []byte("cs-val")))
	require.NoError(t, tx.Put(kv.TblPruningProgress, []byte("pp-key"), []byte("pp-val")))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 7))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Senders, 9))
	require.NoError(t, tx.Commit())
}

func openTestChaindata(t *testing.T, dirs datadir.Dirs) kv.RwDB {
	t.Helper()
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)
	return db
}

func writeTestFile(t *testing.T, path string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte("x"), 0644))
}
