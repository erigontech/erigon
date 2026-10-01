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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	app "github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestPBTImportAcceptanceAndReexecute(t *testing.T) {
	selectPBTBinaryCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, false)
	require.NoError(t, err)
	conversion := source.Chain.Slice(0, 2)
	require.NoError(t, source.Tester.InsertChain(conversion))
	require.NoError(t, os.WriteFile(filepath.Join(source.Tester.Dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(source.Tester.Dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := source.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()

	target, err := execmoduletester.NewPBTAcceptanceChain(t, true, false)
	require.NoError(t, err)
	require.NoError(t, target.Tester.InsertChain(target.Chain))
	require.NoError(t, os.WriteFile(filepath.Join(target.Tester.Dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(target.Tester.Dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	target.Tester.Close()
	resetPBTImportProcessGlobals(t)
	previousDatadir, previousChaindata := datadirCli, chaindata
	t.Cleanup(func() { datadirCli, chaindata = previousDatadir, previousChaindata })
	datadirCli, chaindata = target.Tester.Dirs.DataDir, target.Tester.Dirs.Chaindata
	require.NoError(t, importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), filepath.Join(output, "framed.bin"), target.Chain.Blocks[1].Hash().Hex(), "", log.New()))
	rawDB := dbCfg(dbcfg.ChainDB, target.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		progress, progressErr := stages.GetStageProgress(tx, stages.Execution)
		require.NoError(t, progressErr)
		require.Equal(t, conversion.TopBlock.NumberU64(), progress)
		return nil
	}))
	rawDB.Close()
	selectPBTBinaryCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(target.Tester.Dirs),
		execmoduletester.WithGenesisSpec(target.Genesis),
		execmoduletester.WithKey(target.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	for block := conversion.TopBlock.NumberU64() + 1; block <= target.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		require.Equal(t, source.Chain.Headers[block-1].Root, readCurrentPBTStateRoot(t, reopened))
	}
	reopened.Close()
	require.Equal(t, target.Chain.TopBlock.NumberU64(), readExecutionStageProgress(t, target.Tester.Dirs.Chaindata))
}

func TestPBTImportIntoHexBinTargetUsesBinaryState(t *testing.T) {
	selectPBTBinaryCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, false)
	require.NoError(t, err)
	conversion := source.Chain.Slice(0, 2)
	require.NoError(t, source.Tester.InsertChain(conversion))
	require.NoError(t, os.WriteFile(filepath.Join(source.Tester.Dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(source.Tester.Dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := source.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()

	target, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, target.Tester.InsertChain(target.Chain))
	require.NoError(t, os.WriteFile(filepath.Join(target.Tester.Dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(target.Tester.Dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	target.Tester.Close()
	resetPBTImportProcessGlobals(t)
	previousDatadir, previousChaindata := datadirCli, chaindata
	t.Cleanup(func() { datadirCli, chaindata = previousDatadir, previousChaindata })
	datadirCli, chaindata = target.Tester.Dirs.DataDir, target.Tester.Dirs.Chaindata
	require.NoError(t, importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), filepath.Join(output, "framed.bin"), target.Chain.Blocks[1].Hash().Hex(), "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(target.Tester.Dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantBin, settings.TrieVariantName())
	selectPBTBinaryCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(target.Tester.Dirs),
		execmoduletester.WithGenesisSpec(target.Genesis),
		execmoduletester.WithKey(target.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	for block := conversion.TopBlock.NumberU64() + 1; block <= target.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		require.Equal(t, source.Chain.Headers[block-1].Root, readCurrentPBTStateRoot(t, reopened))
	}
	reopened.Close()
}

func TestPBTAttachAcceptanceAtConversionPoint(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	publishedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionBlock, conversionTx, ok, err := publishedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))
	require.Zero(t, readExecutionStageProgress(t, node.Tester.Dirs.Chaindata))
	require.Equal(t, readPBTFilesRoot(t, published), readPBTFilesRoot(t, node.Tester.Dirs.DataDir))
	dual, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	selectPBTCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	attachedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var attachedAtConversion, dualAtConversion []byte
	require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		attachedAtConversion, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[conversionBlock-1].Hash(), conversionBlock)
		return err
	}))
	require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		dualAtConversion, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[conversionBlock-1].Hash(), conversionBlock)
		return err
	}))
	require.Equal(t, dualAtConversion, attachedAtConversion)
	assertPBTAttachHistory(t, node.Sender, node.Contract, reopened, dual.Tester, conversionTx)
	for block := conversionBlock + 1; block <= node.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		var attachedRoot []byte
		require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			attachedRoot, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		var dualRoot []byte
		require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, attachedRoot)
	}
	assertPBTAttachHistory(t, node.Sender, node.Contract, reopened, dual.Tester, conversionTx)
	reopened.Close()
	buildPBTAcceptanceFilesAtWithMerge(t, node, pbtAcceptanceLastTxNum(t, dual.Tester), true)
	selectPBTCommandSuite(t)
	merged := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	assertPBTAttachHistory(t, node.Sender, node.Contract, merged, dual.Tester, conversionTx)
	merged.Close()
}

func TestPBTAttachPostForkBlockEndShadowRoot(t *testing.T) {
	selectPBTCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 7)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()

	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	blockNum, txNum, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))

	dual, err := execmoduletester.NewPBTAcceptanceChain(t, true, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	attached := dbCfg(dbcfg.ChainDB, node.Tester.Dirs.Chaindata).MustOpen()
	defer attached.Close()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var attachedRoot, dualRoot []byte
	require.NoError(t, attached.View(t.Context(), func(tx kv.Tx) error {
		var err error
		attachedRoot, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[blockNum-1].Hash(), blockNum)
		return err
	}))
	require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
		var err error
		dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[blockNum-1].Hash(), blockNum)
		return err
	}))
	require.NotEmpty(t, attachedRoot)
	require.Equal(t, dualRoot, attachedRoot)
	require.Equal(t, uint64(7), txNum)
}

func TestPBTAttachAcceptanceAtMidBlockConversionPoint(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFilesAt(t, node, 9)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain))
	buildPBTAcceptanceFilesAt(t, source, 9)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	publishedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionBlock, conversionTx, ok, err := publishedSettings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(3), conversionBlock)
	require.Equal(t, uint64(9), conversionTx)
	publishedFiles, err := pbtAttachFiles(datadir.Open(published))
	require.NoError(t, err)
	for _, file := range publishedFiles {
		require.LessOrEqual(t, file.from*publishedSettings.StepSize, conversionTx)
	}
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))

	dual, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	selectPBTCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	for block := conversionBlock; block <= node.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		attachedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
		var attachedRoot []byte
		require.NoError(t, attachedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			attachedRoot, err = rawdb.ReadShadowStateRoot(tx, node.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
		var dualRoot []byte
		require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, attachedRoot)
	}
	reopened.Close()
}

func TestPBTReplayMatchesConvertedState(t *testing.T) {
	selectPBTHexCommandSuite(t)
	converted, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, converted.Tester.InsertChain(converted.Chain))
	buildPBTAcceptanceFiles(t, converted)
	convertedOutput := filepath.Join(t.TempDir(), "converted")
	selectPBTCommandSuite(t)
	require.NoError(t, convertPBT(t.Context(), converted.Tester.Dirs.DataDir, convertedOutput, true, "", log.New()))

	convertedRaw := dbCfg(dbcfg.ChainDB, converted.Tester.Dirs.Chaindata).MustOpen()
	convertedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(convertedOutput))
	require.NoError(t, err)
	convertedAgg := dbstate.New(datadir.Open(convertedOutput)).WithErigonDBSettings(convertedSettings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, convertedAgg.OpenFolder(convertedRaw))
	convertedDB, err := dbtemporal.New(convertedRaw, convertedAgg, nil)
	require.NoError(t, err)
	convertedTx, err := convertedDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer convertedTx.Rollback()
	convertedExport := filepath.Join(t.TempDir(), "converted-export")
	require.NoError(t, app.RunExportPBT(t.Context(), convertedTx, func(block uint64) (*types.Header, error) {
		return converted.Chain.Headers[block-1], nil
	}, convertedExport, log.New()))
	convertedTx.Rollback()
	convertedDB.Close()
	convertedAgg.Close()
	convertedRaw.Close()

	replayed, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, replayed.Tester.InsertChain(replayed.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), replayed.Tester.DB))
	require.NoError(t, replayed.Tester.ReExecuteTo(t.Context(), replayed.Chain.TopBlock.NumberU64()))
	replayedTx, err := replayed.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer replayedTx.Rollback()
	replayedExport := filepath.Join(t.TempDir(), "replayed-export")
	require.NoError(t, app.RunExportPBT(t.Context(), replayedTx, func(block uint64) (*types.Header, error) {
		return replayed.Chain.Headers[block-1], nil
	}, replayedExport, log.New()))
	replayedTx.Rollback()

	convertedMeta := make(map[string]any)
	replayedMeta := make(map[string]any)
	convertedMetaBytes, err := os.ReadFile(filepath.Join(convertedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	replayedMetaBytes, err := os.ReadFile(filepath.Join(replayedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(convertedMetaBytes, &convertedMeta))
	require.NoError(t, json.Unmarshal(replayedMetaBytes, &replayedMeta))
	require.Equal(t, convertedMeta["snapshotDigest"], replayedMeta["snapshotDigest"])
}

func TestPBTAttachedReplayMatchesConvertedState(t *testing.T) {
	selectPBTHexCommandSuite(t)
	node, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, node.Tester.InsertChain(node.Chain))
	buildPBTAcceptanceFiles(t, node)
	node.Tester.Close()

	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	copyPBTStateSalt(t, node, source)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 3)))
	buildPBTAcceptanceFiles(t, source)
	source.Tester.Close()
	selectPBTCommandSuite(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, published, true, "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionBlock, _, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, attachPBT(t.Context(), node.Tester.Dirs.DataDir, published, "", log.New()))

	selectPBTCommandSuite(t)
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(datadir.Open(node.Tester.Dirs.DataDir)),
		execmoduletester.WithGenesisSpec(node.Genesis),
		execmoduletester.WithKey(node.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	for block := conversionBlock + 1; block <= node.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
	}
	attachedExport := filepath.Join(t.TempDir(), "attached-export")
	attachedTx, err := reopened.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(attachedTx.Rollback)
	require.NoError(t, app.RunExportPBT(t.Context(), attachedTx, func(block uint64) (*types.Header, error) {
		return node.Chain.Headers[block-1], nil
	}, attachedExport, log.New()))
	attachedTx.Rollback()
	reopened.Close()

	selectPBTHexCommandSuite(t)
	converted, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, converted.Tester.InsertChain(converted.Chain))
	buildPBTAcceptanceFiles(t, converted)
	converted.Tester.Close()
	selectPBTCommandSuite(t)
	convertedOutput := filepath.Join(t.TempDir(), "converted")
	require.NoError(t, convertPBT(t.Context(), converted.Tester.Dirs.DataDir, convertedOutput, true, "", log.New()))
	convertedDigest := exportConvertedPBTDigest(t, convertedOutput, converted.Tester.Dirs.Chaindata, converted)
	attachedMetaBytes, err := os.ReadFile(filepath.Join(attachedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	var attachedMeta map[string]any
	require.NoError(t, json.Unmarshal(attachedMetaBytes, &attachedMeta))
	require.Equal(t, convertedDigest, attachedMeta["snapshotDigest"])
}

func exportConvertedPBTDigest(t *testing.T, output, rawPath string, fixture *execmoduletester.PBTAcceptanceChain) any {
	t.Helper()
	convertedRaw := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	convertedSettings, err := dbstate.ReadErigonDBSettings(datadir.Open(output))
	require.NoError(t, err)
	convertedAgg := dbstate.New(datadir.Open(output)).WithErigonDBSettings(convertedSettings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, convertedAgg.OpenFolder(convertedRaw))
	convertedDB, err := dbtemporal.New(convertedRaw, convertedAgg, nil)
	require.NoError(t, err)
	convertedTx, err := convertedDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer convertedTx.Rollback()
	convertedExport := filepath.Join(t.TempDir(), "converted-export")
	require.NoError(t, app.RunExportPBT(t.Context(), convertedTx, func(block uint64) (*types.Header, error) {
		return fixture.Chain.Headers[block-1], nil
	}, convertedExport, log.New()))
	convertedTx.Rollback()
	convertedDB.Close()
	convertedAgg.Close()
	convertedRaw.Close()
	metaBytes, err := os.ReadFile(filepath.Join(convertedExport, "pbt-snapshot.meta.json"))
	require.NoError(t, err)
	var meta map[string]any
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	return meta["snapshotDigest"]
}

func copyPBTStateSalt(t *testing.T, node, source *execmoduletester.PBTAcceptanceChain) {
	t.Helper()
	value, err := os.ReadFile(filepath.Join(node.Tester.Dirs.Snap, "salt-state.txt"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(source.Tester.Dirs.Snap, "salt-state.txt"), value, 0o644))
}

func buildPBTAcceptanceFiles(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain) {
	t.Helper()
	fixture.Tester.Close()
	rawDB := dbCfg(dbcfg.ChainDB, fixture.Tester.Dirs.Chaindata).MustOpen()
	tx, err := rawDB.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	_, lastTxNum, err := rawdbv3.TxNums.Last(tx)
	require.NoError(t, err)
	tx.Rollback()
	rawDB.Close()
	buildPBTAcceptanceFilesAt(t, fixture, lastTxNum)
}

func buildPBTAcceptanceFilesAt(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain, lastTxNum uint64) {
	buildPBTAcceptanceFilesAtWithMerge(t, fixture, lastTxNum, false)
}

func buildPBTAcceptanceFilesAtWithMerge(t *testing.T, fixture *execmoduletester.PBTAcceptanceChain, lastTxNum uint64, doMerge bool) {
	t.Helper()
	fixture.Tester.Close()
	dirs := fixture.Tester.Dirs
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, kv.Step(lastTxNum)+1, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), doMerge))
	agg.WaitForFiles()
	tx.Rollback()
	db.Close()
	agg.Close()
	rawDB.Close()
}

func assertPBTAttachHistory(t *testing.T, sender, contract common.Address, attached, dual *execmoduletester.ExecModuleTester, maxTxNum uint64) {
	t.Helper()
	attachedTx, err := attached.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer attachedTx.Rollback()
	dualTx, err := dual.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer dualTx.Rollback()
	var zeroSlot common.Hash
	storageKey := append(append([]byte(nil), contract[:]...), zeroSlot[:]...)
	keys := []struct {
		domain kv.Domain
		key    []byte
	}{
		{domain: kv.AccountsDomain, key: sender[:]},
		{domain: kv.AccountsDomain, key: contract[:]},
		{domain: kv.StorageDomain, key: storageKey},
		{domain: kv.CodeDomain, key: contract[:]},
	}
	for txNum := uint64(0); txNum <= maxTxNum; txNum++ {
		for _, item := range keys {
			attachedValue, attachedOK, err := attachedTx.GetAsOf(item.domain, item.key, txNum)
			require.NoError(t, err)
			dualValue, dualOK, err := dualTx.GetAsOf(item.domain, item.key, txNum)
			require.NoError(t, err)
			require.Equalf(t, dualOK, attachedOK, "%s presence at txNum %d", item.domain, txNum)
			require.Equalf(t, dualValue, attachedValue, "%s value at txNum %d", item.domain, txNum)
		}
	}
}

func pbtAcceptanceLastTxNum(t *testing.T, fixture *execmoduletester.ExecModuleTester) uint64 {
	t.Helper()
	rawDB := fixture.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	var lastTxNum uint64
	require.NoError(t, rawDB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		_, lastTxNum, err = rawdbv3.TxNums.Last(tx)
		return err
	}))
	return lastTxNum
}

func readCurrentPBTStateRoot(t *testing.T, fixture *execmoduletester.ExecModuleTester) common.Hash {
	t.Helper()
	tx, err := fixture.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	agg := fixture.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	at := agg.BeginFilesRo()
	defer at.Close()
	builder, err := eip8297.NewStreamRootBuilder(eip8297.SelectedHash())
	require.NoError(t, err)
	require.NoError(t, dbstate.ForEachPBinLeaf(at, tx, false, func(leaf dbstate.PBinLeaf) error {
		return builder.Add(leaf.Key, leaf.Value)
	}))
	root, err := builder.RootHash()
	require.NoError(t, err)
	return root
}

func readPBTFilesRoot(t *testing.T, output string) common.Hash {
	t.Helper()
	dirs := datadir.Open(output)
	settings, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(nil))
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	builder, err := eip8297.NewStreamRootBuilder(eip8297.SelectedHash())
	require.NoError(t, err)
	require.NoError(t, dbstate.ForEachPBinLeaf(at, nil, true, func(leaf dbstate.PBinLeaf) error {
		return builder.Add(leaf.Key, leaf.Value)
	}))
	root, err := builder.RootHash()
	require.NoError(t, err)
	return root
}

func selectPBTCommandSuite(t *testing.T) {
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
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func selectPBTBinaryCommandSuite(t *testing.T) {
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
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func resetPBTImportProcessGlobals(t *testing.T) {
	t.Helper()
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	statecfg.ExperimentalParallelCommitment = true
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
}

func selectPBTHexCommandSuite(t *testing.T) {
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
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = ""
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
}
