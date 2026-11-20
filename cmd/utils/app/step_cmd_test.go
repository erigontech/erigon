package app

import (
	"context"
	"fmt"
	"math"
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
	plan, err := buildStepRebasePlan(dirs, settings, 8, true, 0, logger)
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
	plan, err := buildStepRebasePlan(dirs, settings, 8, false, 0, logger)
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
	plan, err := buildStepRebasePlan(dirs, settings, 8, true, 0, logger)
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
	plan, err := buildStepRebasePlan(dirs, settings, 8, true, 0, logger)
	require.NoError(t, err)
	err = applyStepRebasePlan(context.Background(), dirs, plan, logger)
	require.ErrorContains(t, err, "chaindata")
}

func TestStepRebaseKeepsDatadirSettings(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	refs := true
	want := state.ErigonDBSettings{StepSize: 16, StepsInFrozenFile: 4, ReferencesInCommitmentBranches: &refs}
	writeTestSettingsFile(t, dirs, "step_size = 16\nsteps_in_frozen_file = 4\nreferences_in_commitment_branches = true\n")
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	plan, err := buildStepRebasePlan(dirs, settings, 8, false, 0, logger)
	require.NoError(t, err)
	require.NoError(t, applyStepRebasePlan(context.Background(), dirs, plan, logger))
	rebased, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	want.StepSize = 8
	want.StepsInFrozenFile = 8
	require.Equal(t, want, *rebased)
}

func TestStepRebaseIncreaseRefusesMisalignedFile(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-20.kv"))
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.20-24.kv"))
	_, err := collectRenameList(dirs.SnapDomain, false, 10, math.MaxUint64)
	require.ErrorContains(t, err, "v1.0-accounts.20-24.kv does not start and end at a multiple of 10 steps")
}

func TestStepRebaseIncreaseRefusesReferencedCommitmentReadAsPlain(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.0-16.kv"))
	_, err := collectRenameList(dirs.SnapDomain, false, 16, math.MaxUint64)
	require.ErrorContains(t, err, "v2.0-commitment.0-16.kv: referenced commitment branches would be read as plain")

	dirs = datadir.New(t.TempDir())
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v3.0-commitment.0-16.kv"))
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment.16-48.kv"))
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v2.0-commitment-bin.48-64.kv"))
	_, err = collectRenameList(dirs.SnapDomain, false, 16, math.MaxUint64)
	require.NoError(t, err)

	dirs = datadir.New(t.TempDir())
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v2.1-commitment.0-1.kv"))
	_, err = collectRenameList(dirs.SnapDomain, true, 4, math.MaxUint64)
	require.NoError(t, err)
}

func TestStepRebaseIncreaseDropsFilesPastAlignedEnd(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, 16, 40)
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-20.kv"))
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.20-22.kv"))
	writeTestFile(t, filepath.Join(dirs.SnapIdx, "v1.0-accounts.20-22.ef"))
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	plan, err := buildStepRebasePlan(dirs, settings, 160, true, 20*16, logger)
	require.NoError(t, err)
	require.Contains(t, plan.deletions, filepath.Join(dirs.SnapDomain, "v1.0-accounts.20-22.kv"))
	require.Contains(t, plan.deletions, filepath.Join(dirs.SnapIdx, "v1.0-accounts.20-22.ef"))
	require.NotContains(t, plan.deletions, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-20.kv"))
	require.Equal(t, uint64(10), plan.alignFactor)
	require.Equal(t, uint64(4), plan.settings.StepsInFrozenFile)
}

func TestStepRebaseIncreaseRefusesReferencesInCommitmentBranches(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestSettingsFile(t, dirs, "step_size = 16\nsteps_in_frozen_file = 40\nreferences_in_commitment_branches = true\n")
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	_, err = buildStepRebasePlan(dirs, settings, 160, true, 320, logger)
	require.ErrorContains(t, err, "references_in_commitment_branches")
}

func TestStepRebaseApplyRefusesStepSizeChangedSincePlan(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, 16, 40)
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	plan, err := buildStepRebasePlan(dirs, settings, 160, false, 0, logger)
	require.NoError(t, err)
	writeTestErigonDBSettings(t, dirs, 32, 20)
	require.ErrorContains(t, applyStepRebasePlan(context.Background(), dirs, plan, logger), "step size is 32, the plan was made at 16")
}

func TestStepRebaseIncreaseRefusesWithoutAlignedBoundary(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeTestErigonDBSettings(t, dirs, 16, 4)
	writeTestFile(t, filepath.Join(dirs.SnapDomain, "v1.0-accounts.0-16.kv"))
	settings, err := state.ResolveErigonDBSettings(dirs, logger, true)
	require.NoError(t, err)
	_, err = buildStepRebasePlan(dirs, settings, 160, true, 0, logger)
	require.ErrorContains(t, err, "nothing would be kept")
}

func writeTestErigonDBSettings(t *testing.T, dirs datadir.Dirs, stepSize, stepsInFrozenFile uint64) {
	t.Helper()
	writeTestSettingsFile(t, dirs, fmt.Sprintf("step_size = %d\nsteps_in_frozen_file = %d\n", stepSize, stepsInFrozenFile))
}

func writeTestSettingsFile(t *testing.T, dirs datadir.Dirs, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, state.ERIGONDB_SETTINGS_FILE), []byte(content), 0o644))
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
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
}
