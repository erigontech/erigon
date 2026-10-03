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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	app "github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestImportPBTUsesOnlySnapshotInput(t *testing.T) {
	require.NotNil(t, cmdCommitmentImportPBT.Flags().Lookup("snapshot"))
	require.Nil(t, cmdCommitmentImportPBT.Flags().Lookup("preimages"), "import-pbt must not require preimages")
	require.Nil(t, cmdCommitmentImportPBT.Flags().Lookup("block"), "import-pbt must not require a block hash")
}

func TestImportPBTReadsHexCheckpointFromDatabaseWhenFilesLag(t *testing.T) {
	selectPBTHexCommandSuite(t)
	fixture, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain.Slice(0, 2)))
	buildPBTAcceptanceFilesAt(t, fixture, 6)
	settings, err := dbstate.ReadErigonDBSettings(fixture.Tester.Dirs)
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, fixture.Tester.Dirs.Chaindata).MustOpen()
	defer rawDB.Close()
	block, txNum, err := readPBTImportHexCheckpoint(t.Context(), fixture.Tester.Dirs, rawDB, settings, log.New())
	require.NoError(t, err)
	require.Equal(t, uint64(2), block)
	require.Equal(t, uint64(7), txNum)
}

func TestImportPBTEndToEndWhenFilesReachCheckpoint(t *testing.T) {
	fixture := newPBTImportFixture(t)
	before := snapshotTree(t, fixture.dataDir)
	require.NoError(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()))
	block, txNum := readPBTImportCheckpoint(t, fixture.dataDir)
	require.Equal(t, uint64(2), block)
	require.Equal(t, uint64(7), txNum)
	after := snapshotTree(t, fixture.dataDir)
	for path, value := range before {
		if strings.Contains(path, "accounts") || strings.Contains(path, "storage") || strings.Contains(path, "code") {
			require.Equal(t, value, after[path], "import must not rewrite state-domain file %s", path)
		}
	}
}

func TestImportPBTAllowsDevChainWithEmptyStoredChainName(t *testing.T) {
	fixture := newPBTImportFixture(t)
	setPBTImportChainName(t, fixture.dataDir, "")
	require.NoError(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "dev", log.New()))
}

func TestPBTImportUnknownChainOmitsStageExecRemedy(t *testing.T) {
	t.Run("dev", func(t *testing.T) {
		err := pbtImportProgressError(4, pbtImportMeta{Block: 3}, "/node", "dev")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "stage_exec")
		require.Contains(t, err.Error(), "cannot be loaded by integration")
	})
	t.Run("mainnet", func(t *testing.T) {
		err := pbtImportProgressError(4, pbtImportMeta{Block: 3}, "/node", "mainnet")
		require.Error(t, err)
		require.Contains(t, err.Error(), "stage_exec --datadir=/node --block=4 --chain=mainnet")
	})
}

func TestImportPBTUsesDatadirScratch(t *testing.T) {
	fixture := newPBTImportFixture(t)
	tmpFile := filepath.Join(t.TempDir(), "tmp-file")
	require.NoError(t, os.WriteFile(tmpFile, nil, 0o644))
	t.Setenv("TMPDIR", tmpFile)
	require.NoError(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()))
}

func TestImportPBTRecoveryRejectsMissingMovedFile(t *testing.T) {
	fixture := newPBTImportFixture(t)
	hook := func(step string) error {
		if step == "settings-written" {
			panic("interrupt after settings")
		}
		return nil
	}
	require.Panics(t, func() {
		_ = importPBTWithHook(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New(), hook)
	})
	dirs := datadir.Open(fixture.dataDir)
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment-bin.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	require.NoError(t, dir.RemoveFile(files[0]))
	err = importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New())
	require.ErrorContains(t, err, "rerun import-pbt")
	require.NoError(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
	remaining, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment-bin.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, remaining)
}

func TestImportPBTRecoveryRejectsCorruptMarker(t *testing.T) {
	fixture := newPBTImportFixture(t)
	setPBTImportChainName(t, fixture.dataDir, "mainnet")
	hook := func(step string) error {
		if step == "settings-written" {
			panic("interrupt after settings")
		}
		return nil
	}
	require.Panics(t, func() {
		_ = importPBTWithHook(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New(), hook)
	})
	dirs := datadir.Open(fixture.dataDir)
	require.NoError(t, os.WriteFile(dbstate.PBTImportMarkerPath(dirs), []byte("{"), 0o644))
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestImportPBTCommandHelperProcess$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_IMPORT_PBT_COMMAND_HELPER=1",
		"IMPORT_PBT_DATADIR="+fixture.dataDir,
		"IMPORT_PBT_SNAPSHOT="+fixture.snapshot,
	)
	output, err := command.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "--chain=mainnet")
	_, err = os.Stat(dbstate.PBTImportMarkerPath(dirs))
	require.ErrorIs(t, err, os.ErrNotExist)
	command = exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestImportPBTCommandHelperProcess$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_IMPORT_PBT_COMMAND_HELPER=1",
		"IMPORT_PBT_DATADIR="+fixture.dataDir,
		"IMPORT_PBT_SNAPSHOT="+fixture.snapshot,
	)
	output, err = command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func TestImportPBTRecoveryRemovesMovedFilesFromCorruptMarker(t *testing.T) {
	fixture := newPBTImportFixture(t)
	setPBTImportChainName(t, fixture.dataDir, "mainnet")
	hook := func(step string) error {
		if step == "files-moved" {
			panic("interrupt after files")
		}
		return nil
	}
	require.Panics(t, func() {
		_ = importPBTWithHook(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New(), hook)
	})
	dirs := datadir.Open(fixture.dataDir)
	binFiles := make([]string, 0)
	require.NoError(t, filepath.WalkDir(dirs.Snap, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), "commitment-bin") {
			binFiles = append(binFiles, path)
		}
		return nil
	}))
	require.NotEmpty(t, binFiles)
	require.NoError(t, os.WriteFile(dbstate.PBTImportMarkerPath(dirs), []byte("{"), 0o644))
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestImportPBTCommandHelperProcess$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_IMPORT_PBT_COMMAND_HELPER=1",
		"IMPORT_PBT_DATADIR="+fixture.dataDir,
		"IMPORT_PBT_SNAPSHOT="+fixture.snapshot,
	)
	var output []byte
	_, err := command.CombinedOutput()
	require.Error(t, err)
	remaining := make([]string, 0)
	require.NoError(t, filepath.WalkDir(dirs.Snap, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() && strings.Contains(entry.Name(), "commitment-bin") {
			remaining = append(remaining, path)
		}
		return nil
	}))
	require.Empty(t, remaining)
	_, err = os.Stat(dbstate.PBTImportMarkerPath(dirs))
	require.ErrorIs(t, err, os.ErrNotExist)
	command = exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestImportPBTCommandHelperProcess$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_IMPORT_PBT_COMMAND_HELPER=1",
		"IMPORT_PBT_DATADIR="+fixture.dataDir,
		"IMPORT_PBT_SNAPSHOT="+fixture.snapshot,
	)
	output, err = command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func TestImportPBTRecoveryKeepsFilesWhenSettingsAreUnreadable(t *testing.T) {
	fixture := newPBTImportFixture(t)
	require.NoError(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()))
	dirs := datadir.Open(fixture.dataDir)
	markerPath := dbstate.PBTImportMarkerPath(dirs)
	require.NoError(t, os.WriteFile(markerPath, []byte("{"), 0o644))
	settingsPath := filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE)
	backupPath := settingsPath + ".backup"
	require.NoError(t, os.Rename(settingsPath, backupPath))
	require.NoError(t, os.Mkdir(settingsPath, 0o755))
	t.Cleanup(func() {
		_ = dir.RemoveAll(settingsPath)
		_ = os.Rename(backupPath, settingsPath)
	})
	before := snapshotTree(t, fixture.dataDir)
	err := importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New())
	require.Error(t, err)
	require.Equal(t, before, snapshotTree(t, fixture.dataDir))
	_, err = os.Stat(markerPath)
	require.NoError(t, err)
	binFiles, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment-bin.*"))
	require.NoError(t, err)
	require.Len(t, binFiles, 16)
}

func TestImportPBTRecoveryKeepsMarkerWhenFileCleanupFails(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))
	file := filepath.Join(dirs.SnapDomain, "v3.0-commitment-bin.0-1.kv")
	require.NoError(t, os.WriteFile(file, nil, 0o644))
	variant := dbstate.TrieVariantHexBin
	previousVariant := dbstate.TrieVariantHex
	hash := commitment.PBinHashBlake3
	marker := &dbstate.PBTImportMarker{
		SnapshotPath:     "/tmp/snapshot",
		SnapshotHash:     "digest",
		Files:            []string{"domain/v3.0-commitment-bin.0-1.kv"},
		Settings:         &dbstate.ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash},
		PreviousSettings: &dbstate.ErigonDBSettings{TrieVariant: &previousVariant},
	}
	require.NoError(t, dbstate.WritePBTImportMarker(dirs, marker))
	require.NoError(t, os.Chmod(dirs.SnapDomain, 0o500))
	t.Cleanup(func() { _ = os.Chmod(dirs.SnapDomain, 0o755) })
	require.Error(t, recoverPBTImportSettings(dirs, marker.Settings, marker))
	got, err := dbstate.ReadPBTImportMarker(dirs)
	require.NoError(t, err)
	require.Equal(t, marker, got)
}

func TestImportPBTMainFlowKeepsMarkerWhenFileCleanupFails(t *testing.T) {
	fixture := newPBTImportFixture(t)
	dirs := datadir.Open(fixture.dataDir)
	hook := func(step string) error {
		if step != "files-moved" {
			return nil
		}
		files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment-bin.*.kv"))
		require.NoError(t, err)
		require.NotEmpty(t, files)
		require.NoError(t, dir.RemoveFile(files[0]))
		require.NoError(t, os.Mkdir(files[0], 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(files[0], "keep"), nil, 0o644))
		return errors.New("injected files-moved failure")
	}
	require.Error(t, importPBTWithHook(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New(), hook))
	_, err := os.Stat(dbstate.PBTImportMarkerPath(dirs))
	require.NoError(t, err)
}

func TestImportPBTMainFlowKeepsMarkerWhenMoveFails(t *testing.T) {
	fixture := newPBTImportFixture(t)
	moveCount := 0
	move := func(source, destination string) error {
		moveCount++
		if moveCount == 2 {
			return errors.New("injected move failure")
		}
		return os.Rename(source, destination)
	}
	err := importPBTWithHooks(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New(), nil, move)
	require.ErrorContains(t, err, "injected move failure")
	require.GreaterOrEqual(t, moveCount, 2)
	_, err = os.Stat(dbstate.PBTImportMarkerPath(datadir.Open(fixture.dataDir)))
	require.NoError(t, err)
}

func setPBTImportChainName(t *testing.T, dataDir, chainName string) {
	t.Helper()
	dirs := datadir.Open(dataDir)
	db := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer db.Close()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		config, err := rawdb.ReadChainConfig(tx, genesisHash)
		if err != nil {
			return err
		}
		config.ChainName = chainName
		return rawdb.WriteChainConfig(tx, genesisHash, config)
	}))
}

func TestImportPBTRecoveryRemedyRunsInFreshProcess(t *testing.T) {
	fixture := newPBTImportFixture(t)
	hook := func(step string) error {
		if step == "settings-written" {
			panic("interrupt after settings")
		}
		return nil
	}
	require.Panics(t, func() {
		_ = importPBTWithHook(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New(), hook)
	})
	dirs := datadir.Open(fixture.dataDir)
	files, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "*-commitment-bin.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	require.NoError(t, dir.RemoveFile(files[0]))
	err = importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New())
	require.ErrorContains(t, err, "integration commitment import-pbt --datadir=")
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestImportPBTHelperProcess$", "-test.v")
	command.Env = append(os.Environ(),
		"GO_WANT_IMPORT_PBT_HELPER=1",
		"IMPORT_PBT_DATADIR="+fixture.dataDir,
		"IMPORT_PBT_SNAPSHOT="+fixture.snapshot,
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

func TestImportPBTHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_IMPORT_PBT_HELPER") != "1" {
		return
	}
	err := importPBT(t.Context(), os.Getenv("IMPORT_PBT_DATADIR"), os.Getenv("IMPORT_PBT_SNAPSHOT"), "", log.New())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func TestImportPBTCommandHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_IMPORT_PBT_COMMAND_HELPER") != "1" {
		return
	}
	root := RootCommand()
	root.SetArgs([]string{
		"commitment", "import-pbt",
		"--datadir", os.Getenv("IMPORT_PBT_DATADIR"),
		"--chain", "mainnet",
		"--snapshot", os.Getenv("IMPORT_PBT_SNAPSHOT"),
	})
	if err := root.ExecuteContext(t.Context()); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func TestImportPBTHexCheckpointMismatchRefusesProductionPath(t *testing.T) {
	fixture := newPBTImportFixture(t)
	overwritePBTImportHexCheckpoint(t, fixture.dataDir, 1, 6, 8)
	before := snapshotTree(t, fixture.dataDir)
	err := importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New())
	require.ErrorContains(t, err, "hex commitment checkpoint")
	require.Equal(t, before, snapshotTree(t, fixture.dataDir))
}

func TestImportPBTReplacesConvertAndAttachWithFilesBeforeCheckpoint(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFilesAt(t, source, 6)
	sourceDB := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(source.Tester.Dirs),
		execmoduletester.WithGenesisSpec(source.Genesis),
		execmoduletester.WithKey(source.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := sourceDB.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()
	sourceDB.Close()
	converted := filepath.Join(t.TempDir(), "converted")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, converted, true, "", log.New()))
	convertedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(converted))
	require.NoError(t, err)
	_, convertedTx, convertedPoint, err := convertedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, convertedPoint)
	require.Less(t, convertedTx, uint64(7), "convert-pbt must use the file frontier below the export checkpoint")
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	target, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, target.Tester.InsertChain(target.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), target.Tester.DB))
	require.NoError(t, target.Tester.ReExecuteTo(t.Context(), 2))
	buildPBTAcceptanceFilesAt(t, target, 6)
	convertedTarget, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, convertedTarget.Tester.InsertChain(convertedTarget.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), convertedTarget.Tester.DB))
	require.NoError(t, convertedTarget.Tester.ReExecuteTo(t.Context(), 2))
	copyPBTStateSalt(t, source, convertedTarget)
	buildPBTAcceptanceFilesAt(t, convertedTarget, 6)
	resetPBTAcceptanceExecution(t, convertedTarget)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	require.NoError(t, attachPBT(t.Context(), convertedTarget.Tester.Dirs.DataDir, converted, "", log.New()))
	selectPBTCommandSuite(t)
	convertedReopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(convertedTarget.Tester.Dirs),
		execmoduletester.WithGenesisSpec(convertedTarget.Genesis),
		execmoduletester.WithKey(convertedTarget.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	require.NoError(t, convertedReopened.ReExecuteTo(t.Context(), convertedTarget.Chain.TopBlock.NumberU64()))
	convertedRaw := convertedReopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	t.Cleanup(convertedReopened.Close)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	before := snapshotTree(t, target.Tester.Dirs.DataDir)
	target.Tester.Close()
	swapHook := func(step string) error {
		if step == "staging-built" {
			return errors.New("test staging failure")
		}
		return nil
	}
	require.ErrorContains(t, importPBTWithHook(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New(), swapHook), "test staging failure")
	require.Equal(t, before, snapshotTree(t, target.Tester.Dirs.DataDir), "staging failure must not change the target")
	swapHook = func(step string) error {
		if step == "files-moved" {
			return errors.New("test move failure")
		}
		return nil
	}
	settingsBeforeMoveFailure := snapshotTree(t, target.Tester.Dirs.DataDir)
	require.ErrorContains(t, importPBTWithHook(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New(), swapHook), "test move failure")
	require.Equal(t, settingsBeforeMoveFailure, snapshotTree(t, target.Tester.Dirs.DataDir), "move failure must not change the target")
	swapHook = func(step string) error {
		if step == "before-settings" {
			return errors.New("test settings failure")
		}
		return nil
	}
	settingsBeforeWriteFailure := snapshotTree(t, target.Tester.Dirs.Snap)
	rowsBeforeWriteFailure := countPBTImportRows(t, target.Tester.Dirs.Chaindata, kv.TblCommitmentBinVals)
	require.ErrorContains(t, importPBTWithHook(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New(), swapHook), "test settings failure")
	require.True(t, reflect.DeepEqual(settingsBeforeWriteFailure, snapshotTree(t, target.Tester.Dirs.Snap)), "settings failure must not change snapshot files")
	require.Equal(t, rowsBeforeWriteFailure, countPBTImportRows(t, target.Tester.Dirs.Chaindata, kv.TblCommitmentBinVals), "settings failure must remove the checkpoint row")
	swapHook = func(step string) error {
		if step == "files-moved" {
			panic("test interrupted after move")
		}
		return nil
	}
	require.Panics(t, func() {
		_ = importPBTWithHook(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New(), swapHook)
	})
	require.FileExists(t, dbstate.PBTImportMarkerPath(datadir.Open(target.Tester.Dirs.DataDir)))
	require.NoError(t, importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New()))
	after := snapshotTree(t, target.Tester.Dirs.DataDir)
	require.NotEqual(t, before, after)
	for path, value := range before {
		if strings.Contains(path, "accounts") || strings.Contains(path, "storage") || strings.Contains(path, "code") {
			require.Equal(t, value, after[path], "import must not rewrite state-domain file %s", path)
		}
	}
	settings, err := dbstate.ReadErigonDBSettings(target.Tester.Dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, settings.TrieHashName())
	gotBlock, gotTx := readPBTImportCheckpoint(t, target.Tester.Dirs.DataDir)
	require.Equal(t, uint64(2), gotBlock, "import must write the bin checkpoint block")
	require.Equal(t, uint64(7), gotTx, "import must write the bin checkpoint txNum")
	binFiles, err := filepath.Glob(filepath.Join(target.Tester.Dirs.SnapDomain, "*-commitment-bin.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, binFiles, "import must write commitment-bin files")
	require.Equal(t, readPBTFilesRoot(t, converted), readPBTFilesRoot(t, target.Tester.Dirs.DataDir), "import rows must equal convert-pbt rows")

	selectPBTCommandSuite(t)
	dual, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(target.Tester.Dirs),
		execmoduletester.WithGenesisSpec(target.Genesis),
		execmoduletester.WithKey(target.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	importedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var importedAtExport, dualAtExport []byte
	require.NoError(t, importedRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		importedAtExport, err = rawdb.ReadShadowStateRoot(tx, target.Chain.Blocks[1].Hash(), 2)
		return err
	}))
	require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		dualAtExport, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[1].Hash(), 2)
		return err
	}))
	require.Equal(t, dualAtExport, importedAtExport, "imported bin shadow at the export block")
	for block := uint64(3); block <= target.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		var importedRoot, dualRoot []byte
		require.NoError(t, importedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			importedRoot, err = rawdb.ReadShadowStateRoot(tx, target.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, importedRoot, "imported bin shadow at block %d", block)
		var convertedRoot []byte
		require.NoError(t, convertedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			convertedRoot, err = rawdb.ReadShadowStateRoot(tx, convertedTarget.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, convertedRoot, "converted and attached bin shadow at block %d", block)
	}
	reopened.Close()
}

func TestImportPBTMatchesConvertAndAttachWithStepSizedFiles(t *testing.T) {
	selectPBTHexCommandSuite(t)
	txCounts := []int{1, 1, 1, 1, 1, 1, 2, 0, 1, 1, 1, 1, 1, 1}
	source, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, false, false, 8, 4, txCounts)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), source.Tester.DB))
	require.NoError(t, source.Tester.ReExecuteTo(t.Context(), 7))
	buildPBTAcceptanceFilesAt(t, source, 23)
	sourceDB := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(source.Tester.Dirs),
		execmoduletester.WithGenesisSpec(source.Genesis),
		execmoduletester.WithKey(source.Key),
		execmoduletester.WithStepSize(8),
		execmoduletester.WithoutGenesisCommit(),
	)
	require.NoError(t, sourceDB.ReExecuteTo(t.Context(), 9))
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	exportDir := filepath.Join(t.TempDir(), "export")
	tx, err := sourceDB.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, exportDir, log.New()))
	tx.Rollback()
	sourceDB.Close()
	convertedDir := filepath.Join(t.TempDir(), "converted")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, convertedDir, true, "", log.New()))
	convertedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(convertedDir))
	require.NoError(t, err)
	conversionBlock, conversionTx, ok, err := convertedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(8), convertedSettings.StepSize)
	require.Equal(t, uint64(7), conversionBlock)
	require.Equal(t, uint64(23), conversionTx)

	selectPBTHexCommandSuite(t)
	convertedTarget, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, false, false, 8, 4, txCounts)
	require.NoError(t, err)
	require.NoError(t, convertedTarget.Tester.InsertChain(convertedTarget.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), convertedTarget.Tester.DB))
	require.NoError(t, convertedTarget.Tester.ReExecuteTo(t.Context(), 7))
	copyPBTStateSalt(t, source, convertedTarget)
	buildPBTAcceptanceFilesAt(t, convertedTarget, 23)
	resetPBTAcceptanceExecution(t, convertedTarget)
	convertedTarget.Tester.Close()
	selectPBTCommandSuite(t)
	require.NoError(t, attachPBT(t.Context(), convertedTarget.Tester.Dirs.DataDir, convertedDir, "", log.New()))
	attachedSettings, attachedErr := dbstate.ReadErigonDBSettings(convertedTarget.Tester.Dirs)
	require.NoError(t, attachedErr)
	require.Equal(t, dbstate.TrieVariantHexBin, attachedSettings.TrieVariantName())
	convertedReopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(convertedTarget.Tester.Dirs),
		execmoduletester.WithGenesisSpec(convertedTarget.Genesis),
		execmoduletester.WithKey(convertedTarget.Key),
		execmoduletester.WithStepSize(8),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	require.NoError(t, convertedReopened.ReExecuteTo(t.Context(), 14))
	defer convertedReopened.Close()

	selectPBTHexCommandSuite(t)
	imported, err := execmoduletester.NewPBTAcceptanceChainWithTxCounts(t, false, false, 8, 4, txCounts)
	require.NoError(t, err)
	require.NoError(t, imported.Tester.InsertChain(imported.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), imported.Tester.DB))
	require.NoError(t, imported.Tester.ReExecuteTo(t.Context(), 7))
	buildPBTAcceptanceFilesAt(t, imported, 23)
	importedAtX := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(imported.Tester.Dirs),
		execmoduletester.WithGenesisSpec(imported.Genesis),
		execmoduletester.WithKey(imported.Key),
		execmoduletester.WithStepSize(8),
		execmoduletester.WithoutGenesisCommit(),
	)
	require.NoError(t, importedAtX.ReExecuteTo(t.Context(), 9))
	importedAtX.Close()
	imported.Tester.Close()
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	require.NoError(t, importPBT(t.Context(), imported.Tester.Dirs.DataDir, filepath.Join(exportDir, "pbt-snapshot.bin"), "", log.New()))
	selectPBTCommandSuite(t)
	importedReopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(imported.Tester.Dirs),
		execmoduletester.WithGenesisSpec(imported.Genesis),
		execmoduletester.WithKey(imported.Key),
		execmoduletester.WithStepSize(8),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	defer importedReopened.Close()
	convertedRaw := convertedReopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	importedRaw := importedReopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	for block := uint64(9); block <= 14; block++ {
		if block > 9 {
			require.NoError(t, importedReopened.ReExecuteTo(t.Context(), block))
		}
		var convertedRoot, importedRoot []byte
		require.NoError(t, convertedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			convertedRoot, err = rawdb.ReadShadowStateRoot(tx, convertedTarget.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.NoError(t, importedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			importedRoot, err = rawdb.ReadShadowStateRoot(tx, imported.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, convertedRoot, importedRoot, "step-sized import shadow at block %d", block)
	}
}

func TestImportPBTRefusalsLeaveDatadirUnchanged(t *testing.T) {
	selectPBTHexCommandSuite(t)
	fixture := newPBTImportFixture(t)
	originalMeta, err := os.ReadFile(fixture.metaPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, os.WriteFile(fixture.metaPath, originalMeta, 0o644)) }()

	attempt := func(name string, mutate func(*pbtImportMeta), want string) {
		t.Run(name, func(t *testing.T) {
			var meta pbtImportMeta
			require.NoError(t, json.Unmarshal(originalMeta, &meta))
			mutate(&meta)
			data, marshalErr := json.Marshal(meta)
			require.NoError(t, marshalErr)
			require.NoError(t, os.WriteFile(fixture.metaPath, data, 0o644))
			before := snapshotTree(t, fixture.dataDir)
			err := importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New())
			if want == "" {
				require.Error(t, err)
			} else {
				require.ErrorContains(t, err, want)
			}
			require.Equal(t, before, snapshotTree(t, fixture.dataDir), "refusal must leave the datadir unchanged")
			require.NoError(t, os.WriteFile(fixture.metaPath, originalMeta, 0o644))
		})
	}

	attempt("moved past block", func(meta *pbtImportMeta) {
		setImportExecutionProgress(t, fixture.dataDir, meta.Block+1)
	}, "target is at block")
	setImportExecutionProgress(t, fixture.dataDir, 2)
	attempt("mid-block checkpoint", func(meta *pbtImportMeta) { meta.TxNum-- }, "not the block end")
	attempt("wrong block hash", func(meta *pbtImportMeta) { meta.BlockHash = common.Hash{0xaa}.Hex() }, "")
	attempt("wrong txNum", func(meta *pbtImportMeta) { meta.TxNum++ }, "not the block end")
	attempt("wrong chain id", func(meta *pbtImportMeta) { meta.ChainID = "999999" }, "chain id")
	attempt("digest mismatch", func(meta *pbtImportMeta) { meta.SnapshotDigest = common.Hash{0xbb}.Hex() }, "snapshot digest")

	t.Run("leaf root mismatch", func(t *testing.T) {
		originalSnapshot, readErr := os.ReadFile(fixture.snapshot)
		require.NoError(t, readErr)
		data := append([]byte(nil), originalSnapshot...)
		data[len(data)-34] ^= 1
		require.NoError(t, os.WriteFile(fixture.snapshot, data, 0o644))
		hash := keccak.NewFastKeccak()
		_, requireErr := hash.Write(data)
		require.NoError(t, requireErr)
		var meta pbtImportMeta
		require.NoError(t, json.Unmarshal(originalMeta, &meta))
		meta.SnapshotDigest = common.BytesToHash(hash.Sum(nil)).Hex()
		metaData, marshalErr := json.Marshal(meta)
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(fixture.metaPath, metaData, 0o644))
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "root")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		require.NoError(t, os.WriteFile(fixture.snapshot, originalSnapshot, 0o644))
		require.NoError(t, os.WriteFile(fixture.metaPath, originalMeta, 0o644))
	})

	t.Run("target already hex and bin", func(t *testing.T) {
		dirs := datadir.Open(fixture.dataDir)
		settings, settingsErr := dbstate.ReadErigonDBSettings(dirs)
		require.NoError(t, settingsErr)
		originalSettings, readErr := os.ReadFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE))
		require.NoError(t, readErr)
		variant := dbstate.TrieVariantHexBin
		settings.TrieVariant = &variant
		require.NoError(t, dbstate.WriteErigonDBSettings(dirs, settings))
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "hex-only")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE), originalSettings, 0o644))
	})

	t.Run("hash suite mismatch", func(t *testing.T) {
		statecfg.BinCommitmentHash = commitment.PBinHashKeccak
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "hash suite")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	})

	t.Run("frozen target", func(t *testing.T) {
		dirs := datadir.Open(fixture.dataDir)
		settings, settingsErr := dbstate.ReadErigonDBSettings(dirs)
		require.NoError(t, settingsErr)
		originalSettings, readErr := os.ReadFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE))
		require.NoError(t, readErr)
		settings.FrozenAtTxNum = map[string]uint64{kv.CommitmentDomain.String(): 7}
		require.NoError(t, dbstate.WriteErigonDBSettings(dirs, settings))
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "frozen")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE), originalSettings, 0o644))
	})
}

func TestImportPBTUsesFrozenBlockFiles(t *testing.T) {
	selectPBTHexCommandSuite(t)
	chain, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, chain.Tester.InsertChain(chain.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, chain)
	sourceDB := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(chain.Tester.Dirs),
		execmoduletester.WithGenesisSpec(chain.Genesis),
		execmoduletester.WithKey(chain.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := sourceDB.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return chain.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()

	config := snapcfg.KnownCfgOrDevnet(chain.Tester.ChainConfig.ChainName)
	sourceDB.Close()
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	archive, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, archive.Tester.InsertChain(archive.Chain))
	require.NoError(t, freezeblocks.DumpBlocks(t.Context(), 0, 3, archive.Tester.ChainConfig, archive.Tester.Dirs.Tmp, archive.Tester.Dirs.Snap, archive.Tester.DB, 1, log.LvlInfo, log.New(), archive.Tester.BlockReader, config, nil))
	archive.Tester.Close()
	copyPBTBlockSnapshotFiles(t, archive.Tester.Dirs, chain.Tester.Dirs)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	block := chain.Chain.Blocks[1]
	rawDB := dbCfg(dbcfg.ChainDB, chain.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		rawdb.DeleteHeader(tx, block.Hash(), block.NumberU64())
		rawdb.DeleteBody(tx, block.Hash(), block.NumberU64())
		if err := rawdb.TruncateCanonicalHash(tx, block.NumberU64(), false); err != nil {
			return err
		}
		return rawdbv3.TxNums.Truncate(tx, block.NumberU64())
	}))
	rawDB.Close()
	setImportExecutionProgress(t, chain.Tester.Dirs.DataDir, block.NumberU64())
	require.NoError(t, importPBT(t.Context(), chain.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(chain.Tester.Dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
}

func copyPBTBlockSnapshotFiles(t *testing.T, source, target datadir.Dirs) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(source.Snap, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(source.Snap, path)
		if err != nil {
			return err
		}
		if filepath.Dir(rel) != "." || entry.Name() == dbstate.ERIGONDB_SETTINGS_FILE || strings.HasPrefix(entry.Name(), "salt-") {
			return nil
		}
		return os.Link(path, filepath.Join(target.Snap, entry.Name()))
	}))
}

type pbtImportFixture struct {
	dataDir  string
	snapshot string
	metaPath string
}

func newPBTImportFixture(t *testing.T) pbtImportFixture {
	t.Helper()
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	sourceDB := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(source.Tester.Dirs),
		execmoduletester.WithGenesisSpec(source.Genesis),
		execmoduletester.WithKey(source.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := sourceDB.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()
	sourceDB.Close()
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))

	target, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, target.Tester.InsertChain(target.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), target.Tester.DB))
	require.NoError(t, target.Tester.ReExecuteTo(t.Context(), 2))
	buildPBTAcceptanceFilesAt(t, target, 7)
	target.Tester.Close()
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	return pbtImportFixture{
		dataDir:  target.Tester.Dirs.DataDir,
		snapshot: filepath.Join(output, "pbt-snapshot.bin"),
		metaPath: filepath.Join(output, "pbt-snapshot.meta.json"),
	}
}

func setImportExecutionProgress(t *testing.T, dataDir string, progress uint64) {
	t.Helper()
	dirs := datadir.Open(dataDir)
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, progress)
	}))
	rawDB.Close()
}

func overwritePBTImportHexCheckpoint(t *testing.T, dataDir string, blockNum, checkpointTx, storageTx uint64) {
	t.Helper()
	dirs := datadir.Open(dataDir)
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	previous, _, err := domains.GetLatest(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State)
	require.NoError(t, err)
	_, _, previousRoot, err := commitment.DecodeCommitmentV3State(previous)
	require.NoError(t, err)
	state, err := commitment.EncodeCommitmentV3State(previousRoot, blockNum, checkpointTx, nil)
	require.NoError(t, err)
	require.NoError(t, domains.DomainPut(kv.CommitmentDomain, tx, commitment.KeyCommitmentV3State, state, storageTx, previous))
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	db.Close()
	agg.Close()
	rawDB.Close()
}

func readPBTImportCheckpoint(t *testing.T, dataDir string) (uint64, uint64) {
	t.Helper()
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		_ = commitment.SetPBinHashSuite(previousSuite)
	})
	dirs := datadir.Open(dataDir)
	resolved, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(resolved).Logger(log.New()).MustOpen(t.Context())
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer rawDB.Close()
	require.NoError(t, agg.OpenFolder(rawDB))
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	readTx, err := rawDB.BeginRo(t.Context())
	require.NoError(t, err)
	defer readTx.Rollback()
	value, _, _, err := at.GetLatest(kv.CommitmentBinDomain, commitment.KeyCommitmentState, readTx, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, value, "import must write the bin checkpoint")
	tx, block := commitmentdb.DecodeTxBlockNums(value)
	return block, tx
}

func countPBTImportRows(t *testing.T, chaindata, table string) uint64 {
	t.Helper()
	db := dbCfg(dbcfg.ChainDB, chaindata).MustOpen()
	defer db.Close()
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	count, err := tx.Count(table)
	require.NoError(t, err)
	return count
}
