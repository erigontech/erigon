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
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/debug"
)

var (
	importPBTSnapshot  string
	importPBTPreimages string
	importPBTBlock     string
)

func init() {
	withChain(cmdCommitmentImportPBT)
	withDataDir(cmdCommitmentImportPBT)
	withConfig(cmdCommitmentImportPBT)
	withExperimentalCommitment(cmdCommitmentImportPBT)
	cmdCommitmentImportPBT.Flags().StringVar(&importPBTSnapshot, "snapshot", "", "PBT snapshot artifact")
	cmdCommitmentImportPBT.Flags().StringVar(&importPBTPreimages, "preimages", "", "PBT preimage artifact")
	cmdCommitmentImportPBT.Flags().StringVar(&importPBTBlock, "block", "", "canonical block hash")
	must(cmdCommitmentImportPBT.MarkFlagRequired("snapshot"))
	must(cmdCommitmentImportPBT.MarkFlagRequired("preimages"))
	must(cmdCommitmentImportPBT.MarkFlagRequired("block"))
	commitmentCmd.AddCommand(cmdCommitmentImportPBT)
}

var cmdCommitmentImportPBT = &cobra.Command{
	Use:          "import-pbt",
	Short:        "bootstrap execution from a PBT snapshot artifact",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger, ctx := debug.SetupCobra(cmd, "integration"), cmd.Context()
		return importPBT(ctx, datadirCli, importPBTSnapshot, importPBTPreimages, importPBTBlock, chain, logger)
	},
}

func importPBT(ctx context.Context, dataDir, snapshotPath, preimagesPath, blockText, chainName string, logger log.Logger) (retErr error) {
	if dataDir == "" || snapshotPath == "" || preimagesPath == "" || blockText == "" {
		return errors.New("commitment import-pbt: datadir, snapshot, preimages and block are required")
	}
	blockHash := common.HexToHash(blockText)
	if !strings.HasPrefix(strings.ToLower(blockText), "0x") {
		return errors.New("commitment import-pbt: block must be a 32-byte hex hash")
	}
	dirs := datadir.Open(dataDir)
	if _, err := dbstate.EnableCommitmentV3FromFiles(dirs); err != nil {
		return err
	}
	settings, settingsErr := dbstate.ReadErigonDBSettings(dirs)
	if settingsErr != nil && !errors.Is(settingsErr, os.ErrNotExist) {
		return settingsErr
	}
	if errors.Is(settingsErr, os.ErrNotExist) {
		stepSize, err := dbstate.ResolveErigonDBStepSize(dirs)
		if err != nil {
			return err
		}
		settings = &dbstate.ErigonDBSettings{StepSize: stepSize}
	}
	if err := validatePBTImportTargetSettings(settings); err != nil {
		return err
	}
	if err := configureImportVariant(dirs); err != nil {
		return err
	}
	blockNum, txNum, err := validatePBTImportPointReadOnly(ctx, dirs, settings, blockHash, logger)
	if err != nil {
		return err
	}
	if err := validatePBTImportTargetFrontierFiles(ctx, dirs, settings, txNum, logger); err != nil {
		return err
	}
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer snapshot.Close()
	preimages, err := os.Open(preimagesPath)
	if err != nil {
		return err
	}
	defer preimages.Close()
	snapshotInfo, err := snapshot.Stat()
	if err != nil {
		return err
	}
	preimagesInfo, err := preimages.Stat()
	if err != nil {
		return err
	}
	var originalSettings dbstate.ErigonDBSettings
	var db kv.TemporalRwDB
	settingsChanged := false
	if settings != nil {
		originalSettings = *settings
	}
	defer func() {
		if db != nil {
			db.Close()
			db = nil
		}
		if settingsChanged && retErr != nil {
			_ = dbstate.WriteErigonDBSettings(dirs, &originalSettings)
		}
	}()
	targetDomain := kv.CommitmentDomain
	headerRoot, err := readPBTImportHeaderRoot(ctx, dirs, blockHash, logger)
	if err != nil {
		return err
	}
	importOptions := dbstate.PBTImportOptions{
		Snapshot: snapshot, SnapshotSize: snapshotInfo.Size(), Preimages: preimages, PreimageSize: preimagesInfo.Size(),
		BlockHash: blockHash, BlockNum: blockNum, TxNum: txNum, HeaderRoot: &headerRoot, TargetDomain: targetDomain, Hash: eip8297.HashBytes, Logger: logger,
	}
	readDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return err
	}
	validationTx, err := readDB.BeginRo(ctx)
	if err != nil {
		readDB.Close()
		return err
	}
	validationErr := dbstate.ValidatePBTSnapshot(ctx, validationTx, importOptions)
	validationTx.Rollback()
	readDB.Close()
	if validationErr != nil {
		return validationErr
	}
	if settings != nil && settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		variant := dbstate.TrieVariantBin
		settings.TrieVariant = &variant
		settings.ReferencesInCommitmentBranches = new(bool)
		if err := dbstate.WriteErigonDBSettings(dirs, settings); err != nil {
			return err
		}
		settingsChanged = true
	}
	if err := configureImportVariant(dirs); err != nil {
		return err
	}
	db, err = openDB(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chainName, logger)
	if err != nil {
		return err
	}
	if err := rawdbreset.ResetExec(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	root, err := dbstate.ImportPBTSnapshot(ctx, tx, importOptions)
	if err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Execution, blockNum); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	logger.Info("imported PBT snapshot", "block", blockNum, "txNum", txNum, "root", root.Hex())
	return nil
}

func validatePBTImportTargetSettings(settings *dbstate.ErigonDBSettings) error {
	if settings == nil {
		return nil
	}
	for _, domain := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		if frozenAt, frozen := settings.FrozenAt(domain); frozen {
			return fmt.Errorf("commitment import-pbt: target domain %s is frozen at txNum %d", domain, frozenAt)
		}
	}
	return nil
}

func validatePBTImportPointWithReader(ctx context.Context, db kv.TemporalRwDB, txNums rawdbv3.TxNumsReader, blockReader *freezeblocks.BlockReader, blockView *blocksnapshots.View, blockHash common.Hash) (uint64, uint64, error) {
	var blockNum, txNum uint64
	err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		readerTx := tx
		if blockReader != nil {
			readerTx = pbtTemporalBlockFilesTx{TemporalTx: tx, view: blockView}
		}
		var header *types.Header
		var err error
		if blockReader != nil {
			header, err = blockReader.HeaderByHash(ctx, readerTx, blockHash)
		} else {
			header, err = rawdb.ReadHeaderByHash(readerTx, blockHash)
		}
		if err != nil {
			return err
		}
		if header == nil {
			return fmt.Errorf("commitment import-pbt: block %s is not in local chaindata", blockHash.Hex())
		}
		blockNum = header.Number.Uint64()
		var canonical common.Hash
		var canonicalFound bool
		if blockReader != nil {
			canonical, canonicalFound, err = blockReader.CanonicalHash(ctx, readerTx, blockNum)
		} else {
			canonical, err = rawdb.ReadCanonicalHash(readerTx, blockNum)
			canonicalFound = canonical != (common.Hash{})
		}
		if err != nil {
			return err
		}
		if !canonicalFound || canonical != blockHash {
			return fmt.Errorf("commitment import-pbt: block %s is not canonical", blockHash.Hex())
		}
		settings, settingsErr := dbstate.ReadErigonDBSettings(readerTx.Debug().Dirs())
		if settingsErr != nil && !errors.Is(settingsErr, os.ErrNotExist) {
			return settingsErr
		}
		genesisHash, err := rawdb.ReadCanonicalHash(readerTx, 0)
		if err != nil {
			return err
		}
		chainConfig, err := rawdb.ReadChainConfig(readerTx, genesisHash)
		if err != nil {
			return err
		}
		binCanonical := chainConfig != nil && chainConfig.IsBinaryTrie(header.Time)
		if settings != nil && settings.TrieVariantName() == dbstate.TrieVariantBin {
			binCanonical = true
		}
		if !binCanonical {
			return fmt.Errorf("commitment import-pbt: block %d is before the binary trie fork", blockNum)
		}
		var found bool
		if blockReader != nil {
			txNum, found, err = blockReader.TxnumReader().MaxExact(ctx, readerTx, blockNum)
		} else {
			txNum, found, err = txNums.MaxExact(ctx, readerTx, blockNum)
		}
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("commitment import-pbt: block %d has no txNum mapping", blockNum)
		}
		return nil
	})
	return blockNum, txNum, err
}

func validatePBTImportPointReadOnly(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, blockHash common.Hash, logger log.Logger) (uint64, uint64, error) {
	rawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return 0, 0, err
	}
	blockReader, blockView, closeBlockReader, err := openPBTBlockReader(ctx, dirs, rawDB, logger)
	if err != nil {
		rawDB.Close()
		return 0, 0, err
	}
	defer closeBlockReader()
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		rawDB.Close()
		return 0, 0, err
	}
	if err := agg.OpenFolder(rawDB); err != nil {
		agg.Close()
		rawDB.Close()
		return 0, 0, err
	}
	db, err := dbtemporal.New(rawDB, agg, nil)
	if err != nil {
		agg.Close()
		rawDB.Close()
		return 0, 0, err
	}
	defer db.Close()
	return validatePBTImportPointWithReader(ctx, db, blockReader.TxnumReader(), blockReader, blockView, blockHash)
}

func readPBTImportHeaderRoot(ctx context.Context, dirs datadir.Dirs, blockHash common.Hash, logger log.Logger) (common.Hash, error) {
	rawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return common.Hash{}, err
	}
	defer rawDB.Close()
	blockReader, blockView, closeBlockReader, err := openPBTBlockReader(ctx, dirs, rawDB, logger)
	if err != nil {
		return common.Hash{}, err
	}
	defer closeBlockReader()
	tx, err := rawDB.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	defer tx.Rollback()
	header, err := blockReader.HeaderByHash(ctx, pbtBlockFilesTx{Tx: tx, view: blockView}, blockHash)
	if err != nil {
		return common.Hash{}, err
	}
	if header == nil {
		return common.Hash{}, fmt.Errorf("commitment import-pbt: block %s is not in local chaindata", blockHash.Hex())
	}
	return header.Root, nil
}

func validatePBTImportTargetFrontierFiles(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, txNum uint64, logger log.Logger) error {
	aggOpts := dbstate.New(dirs).Logger(logger).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps()
	aggOpts = aggOpts.WithErigonDBSettings(settings)
	agg, err := aggOpts.Open(ctx)
	if err != nil {
		return err
	}
	defer agg.Close()
	if err := agg.OpenFolder(nil); err != nil {
		return err
	}
	at := agg.BeginFilesRo()
	defer at.Close()
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain, kv.CommitmentBinDomain} {
		domainAt := at.DbgDomain(domain)
		if domainAt == nil {
			continue
		}
		if err := validatePBTImportFilesFrontier(domain, domainAt.Files(), txNum); err != nil {
			return err
		}
	}
	return nil
}

func validatePBTImportFilesFrontier(domain kv.Domain, files kv.VisibleFiles, txNum uint64) error {
	if len(files) == 0 || txNum == ^uint64(0) {
		return nil
	}
	end := files[len(files)-1].EndRootNum()
	if end > txNum+1 {
		return fmt.Errorf("commitment import-pbt: target %s files extend past txNum %d", domain, txNum)
	}
	return nil
}

func configureImportVariant(dirs datadir.Dirs) error {
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	if err != nil || settings == nil {
		return nil
	}
	statecfg.ExperimentalBinCommitment = settings.TrieVariantName() == dbstate.TrieVariantBin
	statecfg.ExperimentalHexBinCommitment = settings.TrieVariantName() == dbstate.TrieVariantHexBin
	statecfg.ExperimentalCommitmentV3 = false
	if statecfg.ExperimentalBinCommitment {
		statecfg.ExperimentalParallelCommitment = false
	}
	statecfg.BinCommitmentHash = settings.TrieHashName()
	detected, err := dbstate.EnableCommitmentV3FromFiles(dirs)
	if err != nil {
		return err
	}
	if statecfg.ExperimentalCommitmentV3 || detected {
		statecfg.InitSchemas()
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}
	_ = commitment.SetPBinHashSuite(settings.TrieHashName())
	return nil
}
