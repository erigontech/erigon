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
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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

func importPBT(ctx context.Context, dataDir, snapshotPath, preimagesPath, blockText, chainName string, logger log.Logger) error {
	if dataDir == "" || snapshotPath == "" || preimagesPath == "" || blockText == "" {
		return errors.New("commitment import-pbt: datadir, snapshot, preimages and block are required")
	}
	blockHash := common.HexToHash(blockText)
	if !strings.HasPrefix(strings.ToLower(blockText), "0x") {
		return errors.New("commitment import-pbt: block must be a 32-byte hex hash")
	}
	dirs := datadir.Open(dataDir)
	configureImportVariant(dirs)
	db, err := openDB(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chainName, logger)
	if err != nil {
		return err
	}
	defer db.Close()
	blockNum, txNum, err := validatePBTImportPoint(ctx, db, blockHash)
	if err != nil {
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
	settings, settingsErr := dbstate.ReadErigonDBSettings(dirs)
	if settingsErr != nil && !errors.Is(settingsErr, os.ErrNotExist) {
		return settingsErr
	}
	targetDomain := kv.CommitmentDomain
	if settings != nil && settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		targetDomain = kv.CommitmentBinDomain
	}
	importOptions := dbstate.PBTImportOptions{
		Snapshot: snapshot, SnapshotSize: snapshotInfo.Size(), Preimages: preimages, PreimageSize: preimagesInfo.Size(),
		BlockHash: blockHash, BlockNum: blockNum, TxNum: txNum, TargetDomain: targetDomain, Hash: eip8297.HashBytes, Logger: logger,
	}
	validationTx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	validationErr := dbstate.ValidatePBTSnapshot(ctx, validationTx, importOptions)
	validationTx.Rollback()
	if validationErr != nil {
		return validationErr
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
	if targetDomain == kv.CommitmentBinDomain && settings != nil {
		variant := dbstate.TrieVariantBin
		settings.TrieVariant = &variant
		settings.ReferencesInCommitmentBranches = new(bool)
		if err := dbstate.WriteErigonDBSettings(dirs, settings); err != nil {
			return err
		}
	}
	logger.Info("imported PBT snapshot", "block", blockNum, "txNum", txNum, "root", root.Hex())
	return nil
}

func validatePBTImportPoint(ctx context.Context, db kv.TemporalRwDB, blockHash common.Hash) (uint64, uint64, error) {
	var blockNum, txNum uint64
	err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
		header, err := rawdb.ReadHeaderByHash(tx, blockHash)
		if err != nil {
			return err
		}
		if header == nil {
			return fmt.Errorf("commitment import-pbt: block %s is not in local chaindata", blockHash.Hex())
		}
		blockNum = header.Number.Uint64()
		canonical, err := rawdb.ReadCanonicalHash(tx, blockNum)
		if err != nil {
			return err
		}
		if canonical != blockHash {
			return fmt.Errorf("commitment import-pbt: block %s is not canonical", blockHash.Hex())
		}
		settings, settingsErr := dbstate.ReadErigonDBSettings(tx.Debug().Dirs())
		if settingsErr != nil && !errors.Is(settingsErr, os.ErrNotExist) {
			return settingsErr
		}
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
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
		txNum, found, err = rawdbv3.DefaultTxBlockIndexInstance.MaxTxNum(ctx, tx, nil, blockNum)
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

func configureImportVariant(dirs datadir.Dirs) {
	settings, err := dbstate.ReadErigonDBSettings(dirs)
	if err != nil || settings == nil {
		return
	}
	statecfg.ExperimentalBinCommitment = settings.TrieVariantName() == dbstate.TrieVariantBin
	statecfg.ExperimentalHexBinCommitment = settings.TrieVariantName() == dbstate.TrieVariantHexBin
	statecfg.ExperimentalCommitmentV3 = false
	if statecfg.ExperimentalBinCommitment {
		statecfg.ExperimentalParallelCommitment = false
	}
	statecfg.BinCommitmentHash = settings.TrieHashName()
	if statecfg.ExperimentalCommitmentV3 {
		statecfg.InitSchemas()
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}
	_ = commitment.SetPBinHashSuite(settings.TrieHashName())
}
