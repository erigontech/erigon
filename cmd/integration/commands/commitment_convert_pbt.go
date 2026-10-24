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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/debug"
)

var (
	convertPBTKeepHex       bool
	convertPBTOutputDatadir string
	convertPBTResume        bool
	convertPBTSkipVerify    bool
	convertPBTWorkers       int
)

type pbtConvertHooks struct {
	afterOutput          func() error
	standalone           func(datadir.Dirs) error
	rangeWriterLimits    *dbstate.PBinRangeWriterLimits
	sequentialCommitment bool
	workerViews          *atomic.Int32
	sourceWorkerRuns     *atomic.Int32
	workers              int
	resume               bool
	skipVerify           bool
}

func init() {
	withChain(cmdCommitmentConvertPBT)
	withDataDir(cmdCommitmentConvertPBT)
	withConfig(cmdCommitmentConvertPBT)
	withExperimentalCommitment(cmdCommitmentConvertPBT)
	cmdCommitmentConvertPBT.Flags().BoolVar(&convertPBTKeepHex, "keep-hex", false, "keep the source v3 hex commitment files")
	cmdCommitmentConvertPBT.Flags().StringVar(&convertPBTOutputDatadir, "output.datadir", "", "new datadir for the converted commitment files")
	cmdCommitmentConvertPBT.Flags().IntVar(&convertPBTWorkers, "workers", dbg.PBTWorkers, "source walk and commitment workers")
	cmdCommitmentConvertPBT.Flags().BoolVar(&convertPBTResume, "resume", false, "resume a binary commitment conversion from its state point")
	cmdCommitmentConvertPBT.Flags().BoolVar(&convertPBTSkipVerify, "skip-verify", false, "build only: check the source end by file ranges instead of a leaf pass, and skip re-verifying the written rows and root")
	must(cmdCommitmentConvertPBT.MarkFlagDirname("output.datadir"))
	must(cmdCommitmentConvertPBT.MarkFlagRequired("output.datadir"))
	commitmentCmd.AddCommand(cmdCommitmentConvertPBT)
}

var cmdCommitmentConvertPBT = &cobra.Command{
	Use:          "convert-pbt",
	Short:        "convert the latest state to the binary commitment trie",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger, ctx := debug.SetupCobra(cmd, "integration"), cmd.Context()
		return convertPBTWithOptions(ctx, datadirCli, convertPBTOutputDatadir, convertPBTKeepHex, chain, logger, pbtConvertHooks{resume: convertPBTResume, skipVerify: convertPBTSkipVerify, workers: convertPBTWorkers})
	},
}

type pbinConversionPoint struct {
	BlockNum uint64
	TxNum    uint64
}

type pbtResumeBase struct {
	settings *dbstate.ErigonDBSettings
	point    pbinConversionPoint
	state    []byte
	stateEnd uint64
}

func convertPBT(ctx context.Context, sourcePath, outputPath string, keepHex bool, chainName string, logger log.Logger) (err error) {
	return convertPBTWithOptions(ctx, sourcePath, outputPath, keepHex, chainName, logger, pbtConvertHooks{})
}

func convertPBTWithOptions(ctx context.Context, sourcePath, outputPath string, keepHex bool, chainName string, logger log.Logger, hooks pbtConvertHooks) (err error) {
	if sourcePath == "" || outputPath == "" {
		return errors.New("commitment convert-pbt: source and output datadirs are required")
	}
	sourceDirs := datadir.Open(sourcePath)
	outputDirs := datadir.Open(outputPath)
	resume := hooks.resume || convertPBTResume
	if err := checkPBTChainName(ctx, sourceDirs, chainName, "commitment convert-pbt", "source"); err != nil {
		return err
	}
	if nested, overlapErr := pathsOverlap(sourceDirs.DataDir, outputDirs.DataDir); overlapErr != nil {
		return overlapErr
	} else if nested {
		return fmt.Errorf("commitment convert-pbt: output datadir %s overlaps source datadir %s", outputDirs.DataDir, sourceDirs.DataDir)
	}
	if err := os.MkdirAll(outputDirs.DataDir, 0o755); err != nil {
		return err
	}
	outputHasFiles, err := datadirHasFiles(outputDirs.DataDir)
	if err != nil {
		return err
	} else if outputHasFiles && !resume {
		return fmt.Errorf("commitment convert-pbt: output datadir %s is not empty", outputDirs.DataDir)
	}

	oldBin, oldHexBin, oldV3, oldHash, oldSchema := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.BinCommitmentHash, statecfg.Schema
	defer func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.BinCommitmentHash, statecfg.Schema = oldBin, oldHexBin, oldV3, oldHash, oldSchema
	}()

	removeOutput := !resume || !outputHasFiles
	defer func() {
		if removeOutput {
			_ = dir.RemoveAll(outputDirs.DataDir)
		}
	}()
	sourceSettings, err := dbstate.ReadErigonDBSettings(sourceDirs)
	if errors.Is(err, fs.ErrNotExist) {
		sourceSettings = nil
		err = nil
	}
	if err != nil {
		return err
	}
	requestedHash := statecfg.BinCommitmentHash
	detectedV3, err := dbstate.EnableCommitmentV3FromFiles(sourceDirs)
	if err != nil {
		return err
	}
	configurePBTSourceVariant(sourceSettings)
	if detectedV3 && (sourceSettings == nil || sourceSettings.TrieVariantName() == dbstate.TrieVariantHex) {
		statecfg.ConfigureCommitmentV3Records(true)
	}
	if err := validatePBTFileAccessors(sourceDirs, "source", fmt.Sprintf("ERIGON_COMMITMENT_V3=true erigon snapshots index --datadir=%s", sourceDirs.DataDir)); err != nil {
		return err
	}
	var point pbinConversionPoint
	if !resume {
		point, err = readPBinSourcePoint(ctx, sourceDirs, sourceSettings, keepHex, logger)
		if err != nil {
			return err
		}
	}
	var resumeBase pbtResumeBase
	if resume {
		resumeBase, err = readPBTResumeBase(ctx, sourceDirs, outputDirs, outputHasFiles, sourceSettings, logger)
		if err != nil {
			return err
		}
		resumeSourceSettings := sourceSettings
		if resumeSourceSettings == nil {
			resumeSourceSettings = resumeBase.settings
		}
		if err := pbtResumeValidateSourceLayout(sourceDirs, resumeSourceSettings, resumeBase.point.TxNum); err != nil {
			return err
		}
		if sourceSettings != nil && sourceSettings.TrieVariantName() != dbstate.TrieVariantHex && resumeBase.settings.TrieHashName() != sourceSettings.TrieHashName() {
			return fmt.Errorf("commitment convert-pbt: resume base trie_hash %q differs from source settings %q", resumeBase.settings.TrieHashName(), sourceSettings.TrieHashName())
		}
		if outputHasFiles && (keepHex != (resumeBase.settings.TrieVariantName() == dbstate.TrieVariantHexBin)) {
			return fmt.Errorf("commitment convert-pbt: --keep-hex does not match the resume base trie variant %q", resumeBase.settings.TrieVariantName())
		}
	}
	if err := os.MkdirAll(outputDirs.Snap, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(outputDirs.Tmp, 0o755); err != nil {
		return err
	}
	if resume {
		if err := preparePBTResumeOutput(sourceDirs, outputDirs, outputHasFiles, resumeBase, keepHex); err != nil {
			return err
		}
		resumeSourceSettings := sourceSettings
		if resumeSourceSettings == nil {
			resumeSourceSettings = resumeBase.settings
		}
		if err := pbtResumeValidateBaseLayout(sourceDirs, outputDirs, resumeSourceSettings, resumeBase.settings, keepHex, outputHasFiles, resumeBase.point.TxNum); err != nil {
			return err
		}
	}
	if hooks.afterOutput != nil {
		if err := hooks.afterOutput(); err != nil {
			return err
		}
	}
	if !resume {
		if _, err = linkSnapshotsExceptCommitment(sourceDirs.Snap, outputDirs.Snap); err != nil {
			return err
		}
	}
	sourceRawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, sourceDirs.Chaindata), true)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: open source: %w", err)
	}
	sourceAgg, err := dbstate.NewPBTStateAggregator(sourceDirs, sourceSettings, logger).Open(ctx)
	if err != nil {
		sourceRawDB.Close()
		return fmt.Errorf("commitment convert-pbt: open source state: %w", err)
	}
	if err := sourceAgg.OpenFolder(sourceRawDB); err != nil {
		sourceAgg.Close()
		sourceRawDB.Close()
		return fmt.Errorf("commitment convert-pbt: open source files: %w", err)
	}
	sourceDB, err := dbtemporal.New(sourceRawDB, sourceAgg, nil)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: open source temporal view: %w", err)
	}
	defer sourceDB.Close()
	blockReader, blockView, closeBlockReader, err := openPBTBlockReader(ctx, sourceDirs, sourceRawDB, logger)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: open block snapshots: %w", err)
	}
	defer closeBlockReader()
	if err := sourceAgg.ReloadFiles(); err != nil {
		return fmt.Errorf("commitment convert-pbt: reload source files: %w", err)
	}
	sourceWalkAgg := sourceAgg
	if resume && sourceSettings != nil && sourceSettings.TrieVariantName() == dbstate.TrieVariantHexBin {
		walkSettings := *sourceSettings
		walkVariant := dbstate.TrieVariantHex
		walkSettings.TrieVariant = &walkVariant
		walkSettings.TrieHash = nil
		sourceWalkAgg, err = dbstate.NewPBTStateAggregator(sourceDirs, &walkSettings, logger).Open(ctx)
		if err != nil {
			return fmt.Errorf("commitment convert-pbt: open resume source walk: %w", err)
		}
		if err := sourceWalkAgg.OpenFolder(nil); err != nil {
			sourceWalkAgg.Close()
			return fmt.Errorf("commitment convert-pbt: open resume source walk files: %w", err)
		}
		defer sourceWalkAgg.Close()
	}
	sourceTx, err := sourceDB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer sourceTx.Rollback()
	var resumeSourceEnd uint64
	var resumePostCancun, resumeBytecodeRewrite bool
	if resume {
		sourceEnd, sourceEndErr := pbtResumeSourceFilesEnd(sourceDirs, sourceAgg.StepSize())
		if sourceEndErr != nil {
			return sourceEndErr
		}
		if sourceEnd <= resumeBase.point.TxNum {
			return fmt.Errorf("commitment convert-pbt: resume base txNum %d is at or past source files end %d", resumeBase.point.TxNum, sourceEnd)
		}
		resumeSourceEnd = sourceEnd
		if mdbxErr := pbtResumeRejectMDBXState(sourceTx, sourceAgg.StepSize(), sourceEnd); mdbxErr != nil {
			return mdbxErr
		}
		blockNum, found, blockErr := blockReader.TxnumReader().FindBlockNum(ctx, pbtBlockFilesTx{Tx: sourceTx, view: blockView}, sourceEnd-1)
		if blockErr != nil {
			return fmt.Errorf("commitment convert-pbt: find source block at txNum %d: %w", sourceEnd, blockErr)
		}
		if !found {
			return fmt.Errorf("commitment convert-pbt: source files end txNum %d has no block", sourceEnd)
		}
		point = pbinConversionPoint{BlockNum: blockNum, TxNum: sourceEnd}
		genesisHash, chainErr := rawdb.ReadCanonicalHash(sourceTx, 0)
		if chainErr != nil {
			return fmt.Errorf("commitment convert-pbt: read source genesis: %w", chainErr)
		}
		chainConfig, chainErr := rawdb.ReadChainConfig(sourceTx, genesisHash)
		if chainErr != nil {
			return fmt.Errorf("commitment convert-pbt: read source chain config: %w", chainErr)
		}
		baseHeader, headerErr := blockReader.HeaderByNumber(ctx, pbtBlockFilesTx{Tx: sourceTx, view: blockView}, resumeBase.point.BlockNum)
		if headerErr != nil {
			return fmt.Errorf("commitment convert-pbt: read resume base header: %w", headerErr)
		}
		if chainConfig == nil || baseHeader == nil {
			return errors.New("commitment convert-pbt: resume chain config or base header is missing")
		}
		resumePostCancun = chainConfig.IsCancun(baseHeader.Time)
		if !resumePostCancun {
			return fmt.Errorf("commitment convert-pbt: resume base block %d is before Cancun", resumeBase.point.BlockNum)
		}
		resumeBytecodeRewrite = chainConfig.Aura != nil || chainConfig.ChainName == "gnosis" || chainConfig.ChainName == "chiado"
		logger.Info("PBT resume source", "X", resumeBase.point.TxNum, "Y", sourceEnd, "base_block", resumeBase.point.BlockNum, "source_block", point.BlockNum)
	}
	variant := dbstate.TrieVariantHex
	if sourceSettings != nil {
		variant = sourceSettings.TrieVariantName()
	}
	if keepHex && variant == dbstate.TrieVariantBin {
		return errors.New("commitment convert-pbt: --keep-hex needs a hex or hex+bin source")
	}
	if hooks.skipVerify {
		if err := requirePBinSourceFilesEnd(sourceDirs, sourceAgg.StepSize(), point.TxNum); err != nil {
			return err
		}
		logger.Warn("PBT verification check skipped", "check", "C-a", "replacement", "source file ranges")
	} else if err := requirePBinSourceEndWithWorkers(ctx, sourceWalkAgg, point.TxNum, hooks.workers, logger); err != nil {
		return err
	}
	header, blockEnd, afterFork, err := readPBinForkPoint(ctx, pbtBlockFilesTx{Tx: sourceTx, view: blockView}, blockReader, point)
	if err != nil {
		return err
	}
	if !keepHex && !afterFork {
		return fmt.Errorf("commitment convert-pbt: bin-only output requires a conversion point after the binary trie fork")
	}
	hashName := requestedHash
	if resume {
		hashName = resumeBase.settings.TrieHashName()
	}
	if hashName == "" && sourceSettings != nil && (variant == dbstate.TrieVariantBin || variant == dbstate.TrieVariantHexBin) {
		hashName = sourceSettings.TrieHashName()
	}
	if hashName == "" {
		hashName = commitment.PBinHashBlake3
	}
	if err := commitment.SetPBinHashSuite(hashName); err != nil {
		return err
	}
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = keepHex
	statecfg.ConfigureCommitmentV3Records(keepHex)
	trieVariant := dbstate.TrieVariantBin
	targetDomain := kv.CommitmentDomain
	if keepHex || resume && outputHasFiles && resumeBase.settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		trieVariant = dbstate.TrieVariantHexBin
		targetDomain = kv.CommitmentBinDomain
	}
	targetSettings := &dbstate.ErigonDBSettings{
		StepSize:                       sourceAgg.StepSize(),
		StepsInFrozenFile:              sourceAgg.StepsInFrozenFile(),
		ReferencesInCommitmentBranches: new(bool),
		TrieVariant:                    &trieVariant,
		TrieHash:                       &hashName,
	}
	targetChaindata, err := os.MkdirTemp("", "convert-pbt-target-chaindata-")
	if err != nil {
		return err
	}
	defer func() { _ = dir.RemoveAll(targetChaindata) }()
	targetRaw, err := mdbx.New(dbcfg.ChainDB, logger).Path(targetChaindata).Open(ctx)
	if err != nil {
		return err
	}
	defer targetRaw.Close()
	targetFolderDB := kv.RwDB(targetRaw)
	if resume {
		targetFolderDB = nil
	}
	targetAgg, err := openPBTState(ctx, outputDirs, targetSettings, targetFolderDB, logger)
	if err != nil {
		return err
	}
	defer targetAgg.Close()
	if resume {
		if err := targetAgg.ReloadFiles(); err != nil {
			return err
		}
	}
	targetDB, err := dbtemporal.New(targetRaw, targetAgg, nil)
	if err != nil {
		return err
	}
	defer targetDB.Close()
	targetTx, err := targetDB.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer targetTx.Rollback()
	var parallelDB kv.TemporalRoDB = targetDB
	if hooks.sequentialCommitment {
		parallelDB = nil
	}
	var resumeOptions *dbstate.PBinResumeOptions
	var gapProgress *dbstate.PBTProgress
	var gapKeyCount atomic.Uint64
	if resume {
		gapProgress = dbstate.NewPBTProgress(dbstate.PBTProgressConfig{Logger: logger, Message: "PBT resume progress", Phase: "resume gap walk", TempDir: sourceDirs.Tmp})
		defer gapProgress.Done()
		resumeOptions = &dbstate.PBinResumeOptions{
			BaseTxNum:       resumeBase.point.TxNum,
			BaseState:       resumeBase.state,
			BaseStateEnd:    resumeBase.stateEnd,
			SourceTxNum:     resumeSourceEnd,
			Workers:         hooks.workers,
			PostCancun:      resumePostCancun,
			BytecodeRewrite: resumeBytecodeRewrite,
			Progress:        gapProgress.Add,
			GapKeys:         &gapKeyCount,
		}
	}
	root, err := dbstate.ConvertPBin(ctx, dbstate.PBinConvertOptions{
		SourceAggregator:  sourceWalkAgg,
		SourceTx:          sourceTx,
		TargetAggregator:  targetAgg,
		TargetTx:          targetTx,
		TargetDB:          parallelDB,
		TargetDomain:      targetDomain,
		BlockNum:          point.BlockNum,
		EndTxNum:          point.TxNum,
		Hash:              eip8297.HashBytes,
		RangeWriterLimits: hooks.rangeWriterLimits,
		WorkerViews:       hooks.workerViews,
		Workers:           hooks.workers,
		SourceWorkerRuns:  hooks.sourceWorkerRuns,
		Resume:            resumeOptions,
	})
	targetTx.Rollback()
	if err != nil {
		return err
	}
	if resume {
		targetAgg.WaitForFiles()
		logger.Info("PBT resume merge", "X", resumeBase.point.TxNum, "Y", resumeSourceEnd)
		if err := targetAgg.MergeLoop(ctx); err != nil {
			return err
		}
		targetAgg.WaitForFiles()
		logger.Info("PBT resume gap", "X", resumeBase.point.TxNum, "Y", resumeSourceEnd, "gap_keys", gapKeyCount.Load())
	}
	targetAgg.Close()
	if blockEnd && afterFork && root != header.Root {
		return fmt.Errorf("commitment convert-pbt: root %x differs from header root %x", root, header.Root)
	}
	visibleRanges := pbtVisibleSnapshotFiles(sourceWalkAgg, sourceDirs)
	if keepHex {
		if err := linkPBinHexFiles(sourceDirs.SnapDomain, outputDirs.SnapDomain); err != nil {
			return err
		}
		if err := linkPBinCommitmentFiles(sourceDirs, outputDirs, targetSettings.StepSize, point.TxNum); err != nil {
			return err
		}
	}
	if err := removePBTStateHistoryIndexFiles(outputDirs); err != nil {
		return err
	}
	if err := removePBTFilesPastPoint(outputDirs, targetSettings.StepSize, point.TxNum); err != nil {
		return err
	}
	if err := removePBTInvisibleFiles(visibleRanges, sourceDirs, outputDirs); err != nil {
		return err
	}
	if err := validatePBTFileAccessors(outputDirs, "published", ""); err != nil {
		return err
	}
	if resume {
		resumeSourceSettings := sourceSettings
		if resumeSourceSettings == nil {
			resumeSourceSettings = resumeBase.settings
		}
		if err := pbtResumeValidateFinalLayout(sourceDirs, outputDirs, resumeSourceSettings, resumeBase.settings, keepHex, outputHasFiles); err != nil {
			return err
		}
	}
	conversionBlock, conversionTx := point.BlockNum, point.TxNum
	finalSettings := &dbstate.ErigonDBSettings{
		StepSize:                       targetSettings.StepSize,
		StepsInFrozenFile:              targetSettings.StepsInFrozenFile,
		ReferencesInCommitmentBranches: targetSettings.ReferencesInCommitmentBranches,
		TrieVariant:                    &trieVariant,
		TrieHash:                       &hashName,
		ConversionBlockNum:             &conversionBlock,
		ConversionTxNum:                &conversionTx,
	}
	if hooks.standalone != nil {
		if err := hooks.standalone(outputDirs); err != nil {
			return err
		}
	}
	if hooks.skipVerify {
		logger.Warn("PBT verification check skipped", "check", "C-d")
		logger.Warn("PBT verification check skipped", "check", "C-e")
	} else {
		if err := verifyPBTOutputRows(ctx, outputDirs, finalSettings, targetDomain, point, logger); err != nil {
			return err
		}
		if err := verifyPBTOutputIdentity(sourceDirs, outputDirs, visibleRanges, finalSettings, targetDomain, point, logger); err != nil {
			return err
		}
	}
	if err := dbstate.WriteErigonDBSettings(outputDirs, finalSettings); err != nil {
		return err
	}
	hexRoot := ""
	if hexRootValue, found, hexErr := pbtAttachHexRoot(ctx, outputDirs, finalSettings, point.BlockNum, point.TxNum, logger); hexErr != nil {
		return hexErr
	} else if found {
		hexRoot = hexRootValue.Hex()
	} else if detectedV3 {
		oldBin, oldHexBin, oldV3, oldHash, oldSchema := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.BinCommitmentHash, statecfg.Schema
		statecfg.ConfigureCommitmentV3Records(true)
		hexRootValue, sourceFound, hexErr := pbtAttachHexRoot(ctx, sourceDirs, sourceSettings, point.BlockNum, point.TxNum, logger)
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.BinCommitmentHash, statecfg.Schema = oldBin, oldHexBin, oldV3, oldHash, oldSchema
		if hexErr != nil {
			return hexErr
		}
		if sourceFound {
			hexRoot = hexRootValue.Hex()
		}
	}
	if err := writePBTOutputManifest(ctx, outputDirs, point, root, hexRoot, hashName, hooks.skipVerify, hooks.workers, logger); err != nil {
		return err
	}
	removeOutput = false
	return nil
}

func checkPBTChainName(ctx context.Context, dirs datadir.Dirs, expected, command, role string) error {
	if expected == "" {
		return nil
	}
	rawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return fmt.Errorf("%s: open %s chain config: %w", command, role, err)
	}
	defer rawDB.Close()
	tx, err := rawDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return fmt.Errorf("%s: read %s genesis hash: %w", command, role, err)
	}
	config, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return fmt.Errorf("%s: read %s chain config: %w", command, role, err)
	}
	if config == nil || config.ChainName != expected {
		got := "unknown"
		if config != nil {
			got = config.ChainName
		}
		return fmt.Errorf("%s: chain %q does not match %s chain %q", command, expected, role, got)
	}
	return nil
}

func readPBinSourcePoint(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, keepHex bool, logger log.Logger) (pbinConversionPoint, error) {
	if keepHex && (settings == nil || settings.TrieVariantName() == dbstate.TrieVariantHex) {
		statecfg.ConfigureCommitmentV3Records(true)
	}
	agg, err := dbstate.NewPBTStateAggregator(dirs, settings, logger).Open(ctx)
	if err != nil {
		return pbinConversionPoint{}, fmt.Errorf("commitment convert-pbt: open source point: %w", err)
	}
	if err := agg.OpenFolder(nil); err != nil {
		agg.Close()
		return pbinConversionPoint{}, err
	}
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	variant := dbstate.TrieVariantHex
	if settings != nil {
		variant = settings.TrieVariantName()
	}
	return readPBinConversionPointFromFiles(at, variant, keepHex)
}

func readPBTResumeBase(ctx context.Context, sourceDirs, outputDirs datadir.Dirs, outputHasFiles bool, sourceSettings *dbstate.ErigonDBSettings, logger log.Logger) (pbtResumeBase, error) {
	if outputHasFiles {
		settings, err := dbstate.ReadErigonDBSettings(outputDirs)
		if err != nil {
			return pbtResumeBase{}, fmt.Errorf("commitment convert-pbt: resume output settings: %w", err)
		}
		if settings.TrieVariantName() != dbstate.TrieVariantBin && settings.TrieVariantName() != dbstate.TrieVariantHexBin {
			return pbtResumeBase{}, fmt.Errorf("commitment convert-pbt: resume output has no binary commitment domain: %s", settings.TrieVariantName())
		}
		manifestPath := filepath.Join(outputDirs.Snap, "pbt-manifest.toml")
		sources, err := pbtAttachManifestSources(outputDirs)
		if err != nil {
			return pbtResumeBase{}, fmt.Errorf("commitment convert-pbt: resume manifest files: %w", err)
		}
		manifest, err := dbstate.ReadPBTManifest(manifestPath, sources...)
		if err != nil {
			return pbtResumeBase{}, fmt.Errorf("commitment convert-pbt: resume manifest: %w", err)
		}
		if manifest.TrieHash != settings.TrieHashName() {
			return pbtResumeBase{}, fmt.Errorf("commitment convert-pbt: resume manifest trie_hash %q differs from settings %q", manifest.TrieHash, settings.TrieHashName())
		}
		point, err := readPBinSourcePoint(ctx, outputDirs, settings, false, logger)
		if err != nil {
			return pbtResumeBase{}, err
		}
		state, stateEnd, stateErr := readPBTResumeState(ctx, outputDirs, settings, logger)
		if stateErr != nil {
			return pbtResumeBase{}, stateErr
		}
		if manifest.Point.Block != point.BlockNum || manifest.Point.TxNum != point.TxNum {
			return pbtResumeBase{}, fmt.Errorf("commitment convert-pbt: resume manifest point is (%d, %d), want (%d, %d)", manifest.Point.Block, manifest.Point.TxNum, point.BlockNum, point.TxNum)
		}
		return pbtResumeBase{settings: settings, point: point, state: state, stateEnd: stateEnd}, nil
	}
	if sourceSettings == nil || sourceSettings.TrieVariantName() == dbstate.TrieVariantHex {
		return pbtResumeBase{}, errors.New("commitment convert-pbt: empty resume output requires a source binary commitment domain")
	}
	point, err := readPBinSourcePoint(ctx, sourceDirs, sourceSettings, false, logger)
	if err != nil {
		return pbtResumeBase{}, err
	}
	state, stateEnd, stateErr := readPBTResumeState(ctx, sourceDirs, sourceSettings, logger)
	if stateErr != nil {
		return pbtResumeBase{}, stateErr
	}
	return pbtResumeBase{settings: sourceSettings, point: point, state: state, stateEnd: stateEnd}, nil
}

func readPBTResumeState(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, logger log.Logger) ([]byte, uint64, error) {
	agg, err := dbstate.NewPBTStateAggregator(dirs, settings, logger).Open(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("commitment convert-pbt: open resume state: %w", err)
	}
	if err := agg.OpenFolder(nil); err != nil {
		agg.Close()
		return nil, 0, err
	}
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	domain := kv.CommitmentDomain
	if settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		domain = kv.CommitmentBinDomain
	}
	state, found, _, stateEnd, err := at.DebugGetLatestFromFiles(domain, commitment.KeyCommitmentState, ^uint64(0))
	if err != nil {
		return nil, 0, fmt.Errorf("commitment convert-pbt: read resume state: %w", err)
	}
	if !found || len(state) == 0 {
		return nil, 0, fmt.Errorf("commitment convert-pbt: resume binary commitment state is missing from files")
	}
	return bytes.Clone(state), stateEnd, nil
}

func preparePBTResumeOutput(sourceDirs, outputDirs datadir.Dirs, outputHasFiles bool, base pbtResumeBase, keepHex bool) error {
	if outputHasFiles {
		if err := clearPBTResumeNonCommitmentFiles(outputDirs); err != nil {
			return fmt.Errorf("commitment convert-pbt: clear resume state files: %w", err)
		}
		if keepHex {
			if err := removePBTFiles(outputDirs, func(file pbtAttachFile) bool { return file.domain == kv.CommitmentDomain }); err != nil {
				return fmt.Errorf("commitment convert-pbt: clear resume hex files: %w", err)
			}
		}
	}
	if _, err := linkSnapshotsExceptCommitmentWithSalt(sourceDirs.Snap, outputDirs.Snap, outputHasFiles); err != nil {
		return err
	}
	if outputHasFiles {
		return nil
	}
	return linkPBTResumeBaseFiles(sourceDirs, outputDirs, base, keepHex)
}

func clearPBTResumeNonCommitmentFiles(dirs datadir.Dirs) error {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	commitmentPaths := make(map[string]struct{})
	for _, file := range files {
		if file.domain == kv.CommitmentDomain || file.domain == kv.CommitmentBinDomain {
			commitmentPaths[file.path] = struct{}{}
		}
	}
	touched := make(map[string]struct{})
	err = filepath.WalkDir(dirs.Snap, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if _, commitmentFile := commitmentPaths[path]; commitmentFile || name == dbstate.ERIGONDB_SETTINGS_FILE || name == "pbt-manifest.toml" || name == "salt-state.txt" || name == "salt.txt" {
			return nil
		}
		if err := dir.RemoveFile(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		touched[filepath.Dir(path)] = struct{}{}
		return nil
	})
	if err != nil {
		return err
	}
	return fsyncPBTDirs(touched)
}

func linkPBTResumeBaseFiles(sourceDirs, outputDirs datadir.Dirs, base pbtResumeBase, keepHex bool) error {
	sourceDomain := kv.CommitmentDomain
	if base.settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		sourceDomain = kv.CommitmentBinDomain
	}
	targetDomain := kv.CommitmentDomain
	if keepHex {
		targetDomain = kv.CommitmentBinDomain
	}
	files, err := pbtAttachFiles(sourceDirs)
	if err != nil {
		return err
	}
	linked := 0
	for _, file := range pbtAttachVisibleFiles(files) {
		if file.domain != sourceDomain || !pbtAttachAdoptsFile(file) || file.from*base.settings.StepSize > base.point.TxNum {
			continue
		}
		rel, relErr := filepath.Rel(sourceDirs.Snap, file.path)
		if relErr != nil {
			return relErr
		}
		name := filepath.Base(rel)
		if sourceDomain != targetDomain {
			name = strings.Replace(name, sourceDomain.String(), targetDomain.String(), 1)
		}
		destination := filepath.Join(outputDirs.Snap, filepath.Dir(rel), name)
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return err
		}
		if info, statErr := os.Stat(destination); statErr == nil {
			sourceInfo, sourceErr := os.Stat(file.path)
			if sourceErr != nil {
				return sourceErr
			}
			if !os.SameFile(sourceInfo, info) {
				return fmt.Errorf("commitment convert-pbt: resume base file %s conflicts with source %s", destination, file.path)
			}
			continue
		} else if !errors.Is(statErr, fs.ErrNotExist) {
			return statErr
		}
		if err := os.Link(file.path, destination); err != nil {
			return fmt.Errorf("commitment convert-pbt: link resume base file %s: %w", file.path, err)
		}
		linked++
	}
	if linked == 0 {
		return fmt.Errorf("commitment convert-pbt: source binary commitment files at txNum %d are missing", base.point.TxNum)
	}
	return nil
}

func pbtResumeSourceDomain(settings *dbstate.ErigonDBSettings) (kv.Domain, error) {
	if settings == nil {
		return kv.Domain(0), errors.New("commitment convert-pbt: resume source binary commitment settings are missing")
	}
	switch settings.TrieVariantName() {
	case dbstate.TrieVariantBin:
		return kv.CommitmentDomain, nil
	case dbstate.TrieVariantHexBin:
		return kv.CommitmentBinDomain, nil
	default:
		return kv.Domain(0), fmt.Errorf("commitment convert-pbt: resume source has no binary commitment domain: %s", settings.TrieVariantName())
	}
}

func pbtResumeLayoutSourceDomain(settings *dbstate.ErigonDBSettings) (kv.Domain, error) {
	if settings != nil && settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		return kv.CommitmentDomain, nil
	}
	domain, err := pbtResumeSourceDomain(settings)
	if err == nil {
		return domain, nil
	}
	if settings != nil && settings.TrieVariantName() == dbstate.TrieVariantHex {
		return kv.AccountsDomain, nil
	}
	return kv.Domain(0), err
}

func pbtResumeTargetDomain(base *dbstate.ErigonDBSettings, keepHex, outputHasFiles bool) kv.Domain {
	if keepHex || outputHasFiles && base != nil && base.TrieVariantName() == dbstate.TrieVariantHexBin {
		return kv.CommitmentBinDomain
	}
	return kv.CommitmentDomain
}

func pbtResumeCommitmentFiles(dirs datadir.Dirs, domain kv.Domain) ([]pbtAttachFile, error) {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return nil, err
	}
	visible := pbtAttachVisibleFiles(files)
	result := make([]pbtAttachFile, 0, len(visible))
	for _, file := range visible {
		if file.domain == domain && pbtAttachAdoptsFile(file) && (filepath.Ext(file.path) == ".kv" || filepath.Ext(file.path) == ".kvi") {
			result = append(result, file)
		}
	}
	return result, nil
}

func pbtResumeLayoutKey(file pbtAttachFile) string {
	return fmt.Sprintf("%d:%d", file.from, file.to)
}

func pbtResumeValidateSourceLayout(dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, baseTxNum uint64) error {
	domain, err := pbtResumeLayoutSourceDomain(settings)
	if err != nil {
		return err
	}
	files, err := pbtResumeCommitmentFiles(dirs, domain)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: resume source commitment files: %w", err)
	}
	stepSize := settings.StepSize
	if stepSize == 0 {
		return errors.New("commitment convert-pbt: resume source step size is zero")
	}
	for _, file := range files {
		if file.from == 0 && file.to*stepSize > baseTxNum {
			return fmt.Errorf("commitment convert-pbt: source %s file range %d-%d starts at step 0 and spans resume txNum %d", domain, file.from, file.to, baseTxNum)
		}
	}
	return nil
}

func pbtResumeValidateBaseLayout(sourceDirs, outputDirs datadir.Dirs, sourceSettings, baseSettings *dbstate.ErigonDBSettings, keepHex, outputHasFiles bool, baseTxNum uint64) error {
	sourceDomain, err := pbtResumeLayoutSourceDomain(sourceSettings)
	if err != nil {
		return err
	}
	targetDomain := pbtResumeTargetDomain(baseSettings, keepHex, outputHasFiles)
	sourceFiles, err := pbtResumeCommitmentFiles(sourceDirs, sourceDomain)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: resume source commitment files: %w", err)
	}
	outputFiles, err := pbtResumeCommitmentFiles(outputDirs, targetDomain)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: resume base commitment files: %w", err)
	}
	outputRanges := make(map[string]struct{}, len(outputFiles))
	for _, file := range outputFiles {
		outputRanges[pbtResumeLayoutKey(file)] = struct{}{}
	}
	stepSize := sourceSettings.StepSize
	for _, file := range sourceFiles {
		if file.to*stepSize > baseTxNum {
			continue
		}
		key := pbtResumeLayoutKey(file)
		if _, ok := outputRanges[key]; !ok {
			return fmt.Errorf("commitment convert-pbt: resume base is missing commitment file range %d-%d", file.from, file.to)
		}
	}
	return nil
}

func pbtResumeValidateFinalLayout(sourceDirs, outputDirs datadir.Dirs, sourceSettings, baseSettings *dbstate.ErigonDBSettings, keepHex, outputHasFiles bool) error {
	sourceDomain, err := pbtResumeLayoutSourceDomain(sourceSettings)
	if err != nil {
		return err
	}
	targetDomain := pbtResumeTargetDomain(baseSettings, keepHex, outputHasFiles)
	sourceFiles, err := pbtResumeCommitmentFiles(sourceDirs, sourceDomain)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: resume source commitment files: %w", err)
	}
	outputFiles, err := pbtResumeCommitmentFiles(outputDirs, targetDomain)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: resume output commitment files: %w", err)
	}
	sourceRanges := make(map[string]struct{}, len(sourceFiles))
	for _, file := range sourceFiles {
		sourceRanges[pbtResumeLayoutKey(file)] = struct{}{}
	}
	outputRanges := make(map[string]struct{}, len(outputFiles))
	for _, file := range outputFiles {
		outputRanges[pbtResumeLayoutKey(file)] = struct{}{}
	}
	for key := range sourceRanges {
		if _, ok := outputRanges[key]; !ok {
			return fmt.Errorf("commitment convert-pbt: resume output commitment layout is missing %s", key)
		}
	}
	for key := range outputRanges {
		if _, ok := sourceRanges[key]; !ok {
			return fmt.Errorf("commitment convert-pbt: resume output commitment layout has extra %s", key)
		}
	}
	return nil
}

func pbtResumeSourceFilesEnd(dirs datadir.Dirs, stepSize uint64) (uint64, error) {
	if stepSize == 0 {
		return 0, errors.New("commitment convert-pbt: source state file step size is zero")
	}
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return 0, err
	}
	var end uint64
	for _, file := range pbtAttachVisibleFiles(files) {
		if !file.data || filepath.Ext(file.path) != ".kv" || !pbtAttachStateDomain(file.domain) {
			continue
		}
		fileEnd := file.to * stepSize
		if fileEnd > 0 {
			end = max(end, fileEnd-1)
		}
	}
	if end == 0 {
		return 0, errors.New("commitment convert-pbt: source state file end is missing")
	}
	return end, nil
}

func pbtResumeRejectMDBXState(tx kv.TemporalTx, stepSize, sourceEnd uint64) error {
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		iter, err := tx.Debug().RangeLatest(domain, nil, nil, kv.Unlim)
		if err != nil {
			return fmt.Errorf("commitment convert-pbt: inspect source MDBX %s: %w", domain, err)
		}
		for iter.HasNext() {
			key, _, nextErr := iter.Next()
			if nextErr != nil {
				iter.Close()
				return fmt.Errorf("commitment convert-pbt: inspect source MDBX %s: %w", domain, nextErr)
			}
			_, step, found, getErr := tx.Debug().GetLatestFromDB(domain, key)
			if getErr != nil {
				iter.Close()
				return fmt.Errorf("commitment convert-pbt: inspect source MDBX %s: %w", domain, getErr)
			}
			if found && step.ToTxNum(stepSize) > sourceEnd {
				iter.Close()
				return fmt.Errorf("commitment convert-pbt: source MDBX %s row %x is past source files end txNum %d", domain, key, sourceEnd)
			}
		}
		iter.Close()
	}
	return nil
}

func configurePBTSourceVariant(settings *dbstate.ErigonDBSettings) {
	variant := dbstate.TrieVariantHex
	if settings != nil {
		variant = settings.TrieVariantName()
	}
	switch variant {
	case dbstate.TrieVariantHex:
		statecfg.ExperimentalBinCommitment = false
		statecfg.ExperimentalHexBinCommitment = false
		statecfg.BinCommitmentHash = ""
	case dbstate.TrieVariantHexBin:
		statecfg.ExperimentalBinCommitment = true
		statecfg.ExperimentalHexBinCommitment = true
		statecfg.ConfigureCommitmentV3Records(true)
		statecfg.BinCommitmentHash = settings.TrieHashName()
	case dbstate.TrieVariantBin:
		statecfg.ExperimentalBinCommitment = true
		statecfg.ExperimentalHexBinCommitment = false
		statecfg.ConfigureCommitmentV3Records(false)
		statecfg.BinCommitmentHash = settings.TrieHashName()
	}
}

func readPBinConversionPointFromFiles(at *dbstate.AggregatorRoTx, variant string, keepHex bool) (pbinConversionPoint, error) {
	domain := kv.CommitmentDomain
	key := commitment.KeyCommitmentV3State
	if variant == dbstate.TrieVariantBin || (variant == dbstate.TrieVariantHexBin && !keepHex) {
		if variant == dbstate.TrieVariantHexBin {
			domain = kv.CommitmentBinDomain
		}
		key = commitment.KeyCommitmentState
		value, found, _, _, err := at.DebugGetLatestFromFiles(domain, key, ^uint64(0))
		if err != nil {
			return pbinConversionPoint{}, err
		}
		if !found || len(value) < 16 {
			return pbinConversionPoint{}, errors.New("commitment convert-pbt: binary commitment state is missing from files; collate first")
		}
		txNum, blockNum := commitmentdb.DecodeTxBlockNums(value)
		return pbinConversionPoint{BlockNum: blockNum, TxNum: txNum}, nil
	}
	value, found, _, _, err := at.DebugGetLatestFromFiles(domain, key, ^uint64(0))
	if err != nil {
		return pbinConversionPoint{}, err
	}
	if !found || len(value) == 0 {
		key = commitment.KeyCommitmentState
		value, found, _, _, err = at.DebugGetLatestFromFiles(domain, key, ^uint64(0))
		if err != nil {
			return pbinConversionPoint{}, err
		}
		if !found || len(value) < 16 {
			if keepHex {
				return pbinConversionPoint{}, errors.New("commitment convert-pbt: source hex commitment is legacy or missing; run commitment convert --v3 or collate first")
			}
			return pbinConversionPoint{}, errors.New("commitment convert-pbt: source commitment state is missing from files; collate first")
		}
		if keepHex {
			return pbinConversionPoint{}, errors.New("commitment convert-pbt: source hex commitment is legacy; run commitment convert --v3 or collate first")
		}
		txNum, blockNum := commitmentdb.DecodeTxBlockNums(value)
		return pbinConversionPoint{BlockNum: blockNum, TxNum: txNum}, nil
	}
	blockNum, txNum, _, err := commitment.DecodeCommitmentV3State(value)
	if err != nil {
		if keepHex {
			return pbinConversionPoint{}, errors.New("commitment convert-pbt: source hex commitment is legacy; run commitment convert --v3 or collate first")
		}
		return pbinConversionPoint{}, err
	}
	return pbinConversionPoint{BlockNum: blockNum, TxNum: txNum}, nil
}

func verifyPBTOutputRows(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, domain kv.Domain, point pbinConversionPoint, logger log.Logger) error {
	_, err := verifyPBTRowsInScratchDB(ctx, dirs, settings, domain, point, "commitment convert-pbt: verify written rows", logger)
	return err
}

func verifyPBTOutputIdentity(sourceDirs, outputDirs datadir.Dirs, visibleRanges map[string]struct{}, settings *dbstate.ErigonDBSettings, domain kv.Domain, point pbinConversionPoint, logger log.Logger) error {
	sourceFiles, err := pbtAttachFiles(sourceDirs)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: C-e source files: %w", err)
	}
	outputFiles, err := pbtAttachFiles(outputDirs)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: C-e output files: %w", err)
	}
	isBinary := func(file pbtAttachFile) bool {
		return file.domain == kv.CommitmentBinDomain || settings.TrieVariantName() == dbstate.TrieVariantBin && file.domain == kv.CommitmentDomain
	}
	sourceByPath := make(map[string]pbtAttachFile)
	for _, file := range pbtAttachVisibleFiles(sourceFiles) {
		if pbtAttachAdoptsFile(file) && !isBinary(file) {
			rel, relErr := filepath.Rel(sourceDirs.Snap, file.path)
			if relErr != nil {
				return relErr
			}
			parsed, _, parsedOK := snaptype.ParseFileName(filepath.Dir(file.path), filepath.Base(file.path))
			if !parsedOK {
				continue
			}
			if _, visible := visibleRanges[pbtSnapshotFileKey(pbtSnapshotFileFamily(rel), parsed.TypeString, parsed.From, parsed.To)]; !visible {
				continue
			}
			sourceByPath[filepath.Clean(rel)] = file
		}
	}
	identityFiles := make([]pbtAttachFile, 0, len(outputFiles))
	for _, file := range pbtAttachVisibleFiles(outputFiles) {
		if pbtAttachAdoptsFile(file) && !isBinary(file) {
			identityFiles = append(identityFiles, file)
		}
	}
	outputByPath := make(map[string]pbtAttachFile, len(identityFiles))
	for _, file := range identityFiles {
		rel, relErr := filepath.Rel(outputDirs.Snap, file.path)
		if relErr != nil {
			return relErr
		}
		outputByPath[filepath.Clean(rel)] = file
	}
	paths := make([]string, 0, len(sourceByPath))
	for path := range sourceByPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	progress := dbstate.NewPBTProgress(dbstate.PBTProgressConfig{Logger: logger, Phase: "output identity verify", Total: uint64(len(paths))})
	defer progress.Done()
	var missing error
	for _, rel := range paths {
		source := sourceByPath[rel]
		file, ok := outputByPath[rel]
		if !ok {
			if missing == nil {
				missing = fmt.Errorf("commitment convert-pbt: C-e output state file %s is missing", filepath.Join(outputDirs.Snap, rel))
			}
			continue
		}
		sourceInfo, sourceErr := os.Stat(source.path)
		if sourceErr != nil {
			return sourceErr
		}
		outputInfo, outputErr := os.Stat(file.path)
		if outputErr != nil {
			return outputErr
		}
		if !os.SameFile(sourceInfo, outputInfo) {
			return fmt.Errorf("commitment convert-pbt: C-e output state file %s is not a hard link of %s", file.path, source.path)
		}
		progress.Add(1)
	}
	for rel, file := range outputByPath {
		if _, ok := sourceByPath[rel]; !ok {
			return fmt.Errorf("commitment convert-pbt: C-e output state file %s has no source file", file.path)
		}
	}
	stateErr := verifyPBTOutputStateRecords(outputDirs, settings, domain, point)
	if missing != nil {
		if stateErr != nil {
			return errors.Join(missing, stateErr)
		}
		return missing
	}
	if stateErr != nil {
		return stateErr
	}
	return nil
}

func verifyPBTOutputStateRecords(dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, domain kv.Domain, point pbinConversionPoint) error {
	configurePBTSourceVariant(settings)
	agg, err := openPBTState(context.Background(), dirs, settings, nil, log.Root())
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: C-e state records: %w", err)
	}
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	checks := []struct {
		domain kv.Domain
		key    []byte
		name   string
		decode func([]byte) (uint64, uint64, error)
	}{
		{domain: domain, key: commitment.KeyCommitmentState, name: domain.String(), decode: func(value []byte) (uint64, uint64, error) {
			if len(value) < 16 {
				return 0, 0, errors.New("state record is too short")
			}
			tx, block := commitmentdb.DecodeTxBlockNums(value)
			return block, tx, nil
		}},
	}
	if settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		checks = append(checks, struct {
			domain kv.Domain
			key    []byte
			name   string
			decode func([]byte) (uint64, uint64, error)
		}{domain: kv.CommitmentDomain, key: commitment.KeyCommitmentV3State, name: "commitment", decode: func(value []byte) (uint64, uint64, error) {
			block, tx, _, decodeErr := commitment.DecodeCommitmentV3State(value)
			if decodeErr != nil {
				return 0, 0, decodeErr
			}
			return block, tx, nil
		}})
	}
	for _, check := range checks {
		value, found, start, end, getErr := at.DebugGetLatestFromFiles(check.domain, check.key, ^uint64(0))
		if getErr != nil {
			return fmt.Errorf("commitment convert-pbt: C-e %s state: %w", check.name, getErr)
		}
		if !found || start > point.TxNum || end <= point.TxNum {
			return fmt.Errorf("commitment convert-pbt: C-e %s state is not at conversion txNum %d", check.name, point.TxNum)
		}
		block, tx, decodeErr := check.decode(value)
		if decodeErr != nil {
			return fmt.Errorf("commitment convert-pbt: C-e %s state: %w", check.name, decodeErr)
		}
		if block != point.BlockNum || tx != point.TxNum {
			return fmt.Errorf("commitment convert-pbt: C-e %s state is (%d, %d), want (%d, %d)", check.name, block, tx, point.BlockNum, point.TxNum)
		}
	}
	return nil
}

func verifyPBTRowsInScratchDB(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, domain kv.Domain, point pbinConversionPoint, what string, logger log.Logger) (common.Hash, error) {
	configurePBTSourceVariant(settings)
	chaindataDir, err := os.MkdirTemp("", "pbt-verify-chaindata-")
	if err != nil {
		return common.Hash{}, err
	}
	defer func() { _ = dir.RemoveAll(chaindataDir) }()
	rawDB := mdbx.New(dbcfg.ChainDB, logger).Path(chaindataDir).MustOpen()
	defer rawDB.Close()
	if err := rawDB.Update(ctx, func(tx kv.RwTx) error {
		for blockNum := uint64(0); blockNum <= point.BlockNum; blockNum++ {
			maxTxNum := point.TxNum
			if blockNum == 0 && point.BlockNum != 0 {
				maxTxNum = 0
			}
			if err := rawdbv3.TxNums.Append(tx, blockNum, maxTxNum); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return common.Hash{}, err
	}
	return verifyPBTRows(ctx, rawDB, nil, dirs, settings, domain, what, logger)
}

func requirePBinSourceEnd(agg *dbstate.Aggregator, endTxNum uint64) error {
	err := requirePBinSourceEndWithWorkers(context.Background(), agg, endTxNum, 1, log.Root())
	if err != nil && strings.Contains(err.Error(), "pbin stamp walk") {
		return fmt.Errorf("commitment convert-pbt: the last file holds writes up to txNum, after conversion txNum %d", endTxNum)
	}
	return err
}

func requirePBinSourceEndWithWorkers(ctx context.Context, agg *dbstate.Aggregator, endTxNum uint64, workers int, logger log.Logger) error {
	at := agg.BeginFilesRo()
	defer at.Close()
	progress := dbstate.NewPBTProgress(dbstate.PBTProgressConfig{Logger: logger, Phase: "source end verify", TempDir: agg.Dirs().Tmp})
	defer progress.Done()
	return dbstate.VerifyPBinSourceStamps(ctx, at, nil, true, endTxNum, workers, nil, progress.Add)
}

func requirePBinSourceFilesEnd(dirs datadir.Dirs, stepSize, endTxNum uint64) error {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	if step, past := pbtAttachFirstStepPastPoint(pbtAttachVisibleFiles(files), stepSize, endTxNum); past {
		return fmt.Errorf("commitment convert-pbt: state files from step %d hold writes past conversion txNum %d; wait for the next step", step, endTxNum)
	}
	return nil
}

func writePBTOutputManifest(ctx context.Context, dirs datadir.Dirs, point pbinConversionPoint, pbtRoot common.Hash, hexRoot, trieHash string, skipVerify bool, workers int, logger log.Logger) error {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: manifest files: %w", err)
	}
	sources := make([]dbstate.PBTManifestFileSource, 0, len(files))
	for _, file := range pbtAttachVisibleFiles(files) {
		if !pbtAttachAdoptsFile(file) {
			continue
		}
		rel, relErr := filepath.Rel(dirs.Snap, file.path)
		if relErr != nil {
			return relErr
		}
		sources = append(sources, dbstate.PBTManifestFileSource{Name: filepath.Clean(rel), Path: file.path})
	}
	progress := dbstate.NewPBTProgress(dbstate.PBTProgressConfig{
		Logger:  logger,
		Message: "PBT manifest progress",
		Phase:   "manifest digest",
		Total:   uint64(len(sources)),
		TempDir: dirs.Tmp,
		Fields:  []any{"bytes_done", uint64(0)},
	})
	var bytesDone atomic.Uint64
	manifest, err := dbstate.BuildPBTManifest(ctx, dbstate.PBTManifestPoint{Block: point.BlockNum, TxNum: point.TxNum}, pbtRoot.Hex(), hexRoot, trieHash, skipVerify, sources, workers, func(done, _, bytes uint64) {
		progress.SetFields("bytes_done", bytesDone.Add(bytes))
		progress.Add(1)
	})
	progress.Done()
	if err != nil {
		return err
	}
	path := filepath.Join(dirs.Snap, "pbt-manifest.toml")
	return dbstate.WritePBTManifest(path, manifest)
}

func readPBinForkPoint(ctx context.Context, tx kv.Tx, blockReader *freezeblocks.BlockReader, point pbinConversionPoint) (header *types.Header, blockEnd, afterFork bool, err error) {
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return nil, false, false, err
	}
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return nil, false, false, err
	}
	header, err = blockReader.HeaderByNumber(ctx, tx, point.BlockNum)
	if err != nil {
		return nil, false, false, err
	}
	if chainConfig == nil || header == nil {
		return header, false, false, nil
	}
	afterFork = chainConfig.IsBinaryTrie(header.Time)
	maxTxNum, found, txErr := blockReader.TxnumReader().MaxExact(ctx, tx, nil, point.BlockNum)
	if txErr != nil {
		return nil, false, false, txErr
	}
	if !found {
		return nil, false, false, fmt.Errorf("commitment convert-pbt: block %d has no txNum mapping", point.BlockNum)
	}
	blockEnd = maxTxNum == point.TxNum
	return header, blockEnd, afterFork, nil
}

func removePBTStateHistoryIndexFiles(dirs datadir.Dirs) error {
	return removePBTFiles(dirs, func(file pbtAttachFile) bool {
		return pbtAttachStateDomain(file.domain) && !pbtAttachAdoptsFile(file)
	})
}

func linkPBinHexFiles(sourceDir, outputDir string) error {
	return linkPBinDirectory(sourceDir, outputDir, 0, 0, "commitment convert-pbt: source hex commitment files are missing")
}

func linkPBinCommitmentFiles(sourceDirs, outputDirs datadir.Dirs, stepSize, endTxNum uint64) error {
	for _, roots := range [][2]string{
		{sourceDirs.SnapHistory, outputDirs.SnapHistory},
		{sourceDirs.SnapIdx, outputDirs.SnapIdx},
		{sourceDirs.SnapAccessors, outputDirs.SnapAccessors},
	} {
		if err := linkPBinDirectory(roots[0], roots[1], stepSize, endTxNum, ""); err != nil {
			return err
		}
	}
	return nil
}

func linkPBinDirectory(sourceDir, outputDir string, stepSize, endTxNum uint64, missingSourceMessage string) error {
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		if missingSourceMessage == "" && errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if missingSourceMessage != "" && errors.Is(err, fs.ErrNotExist) {
			return errors.New(missingSourceMessage)
		}
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		parsed, _, ok := snaptype.ParseFileName(sourceDir, entry.Name())
		if !ok || parsed.TypeString != kv.CommitmentDomain.String() || stepSize != 0 && parsed.From*stepSize > endTxNum {
			continue
		}
		if err := os.Link(filepath.Join(sourceDir, entry.Name()), filepath.Join(outputDir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func removePBTInvisibleFiles(visibleRanges map[string]struct{}, sourceDirs, outputDirs datadir.Dirs) error {
	for _, root := range []string{outputDirs.SnapDomain, outputDirs.SnapHistory, outputDirs.SnapIdx, outputDirs.SnapAccessors} {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				if errors.Is(walkErr, fs.ErrNotExist) {
					return nil
				}
				return walkErr
			}
			if entry.IsDir() {
				return nil
			}
			parsed, _, ok := snaptype.ParseFileName(root, entry.Name())
			if !ok {
				return nil
			}
			rel, err := filepath.Rel(outputDirs.Snap, path)
			if err != nil {
				return err
			}
			sourcePath := filepath.Join(sourceDirs.Snap, rel)
			sourceInfo, err := os.Stat(sourcePath)
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			if err != nil {
				return err
			}
			outputInfo, err := os.Stat(path)
			if err != nil || !os.SameFile(sourceInfo, outputInfo) {
				return nil
			}
			key := pbtSnapshotFileKey(pbtSnapshotFileFamily(rel), parsed.TypeString, parsed.From, parsed.To)
			if _, ok := visibleRanges[key]; ok {
				return nil
			}
			return dir.RemoveFile(path)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func validatePBTFileAccessors(dirs datadir.Dirs, scope, remedy string) error {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	files = pbtAttachVisibleFiles(files)
	have := make(map[string]struct{}, len(files))
	for _, file := range files {
		have[pbtAttachFileKind(file)] = struct{}{}
	}
	has := func(domain kv.Domain, from, to uint64, ext string) bool {
		_, ok := have[pbtAttachFileKind(pbtAttachFile{domain: domain, from: from, to: to, path: ext})]
		return ok
	}
	for _, file := range files {
		ext := filepath.Ext(file.path)
		var complete bool
		switch ext {
		case ".kv":
			complete = has(file.domain, file.from, file.to, ".kvi") || has(file.domain, file.from, file.to, ".bt") && has(file.domain, file.from, file.to, ".kvei")
		case ".v":
			complete = has(file.domain, file.from, file.to, ".vi")
		case ".ef":
			complete = has(file.domain, file.from, file.to, ".efi")
		default:
			continue
		}
		if !complete {
			message := fmt.Sprintf("commitment convert-pbt: %s %s file %s has no accessor", scope, file.domain, file.path)
			if remedy != "" {
				message += "; run " + remedy
			}
			return errors.New(message)
		}
	}
	return nil
}

func pbtVisibleSnapshotFiles(sourceAgg *dbstate.Aggregator, sourceDirs datadir.Dirs) map[string]struct{} {
	visibleRanges := make(map[string]struct{})
	add := func(snapDir, path string) {
		rel, err := filepath.Rel(snapDir, path)
		if err != nil {
			return
		}
		parsed, _, ok := snaptype.ParseFileName(filepath.Dir(path), filepath.Base(path))
		if !ok {
			return
		}
		visibleRanges[pbtSnapshotFileKey(pbtSnapshotFileFamily(rel), parsed.TypeString, parsed.From, parsed.To)] = struct{}{}
	}
	at := sourceAgg.BeginFilesRo()
	defer at.Close()
	for _, file := range at.AllFiles() {
		add(sourceAgg.Dirs().Snap, file.Fullpath())
	}
	files, err := pbtSnapshotFiles(sourceDirs, func(domain kv.Domain) bool {
		return domain == kv.CommitmentDomain || domain == kv.CommitmentBinDomain
	})
	if err != nil {
		return visibleRanges
	}
	for _, file := range pbtAttachVisibleFiles(files) {
		add(sourceDirs.Snap, file.path)
	}
	return visibleRanges
}

func pbtSnapshotFileFamily(rel string) string {
	family := filepath.Dir(rel)
	if family != "accessor" {
		return family
	}
	switch filepath.Ext(rel) {
	case ".vi":
		return "history"
	case ".efi":
		return "idx"
	default:
		return "domain"
	}
}

func pbtSnapshotFileKey(family, kind string, from, to uint64) string {
	return fmt.Sprintf("%s:%s:%d:%d", family, kind, from, to)
}
