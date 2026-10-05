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
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
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
	"github.com/erigontech/erigon/db/state/execctx"
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
)

type pbtConvertHooks struct {
	afterOutput       func() error
	standalone        func(datadir.Dirs) error
	rangeWriterLimits *dbstate.PBinRangeWriterLimits
}

func init() {
	withChain(cmdCommitmentConvertPBT)
	withDataDir(cmdCommitmentConvertPBT)
	withConfig(cmdCommitmentConvertPBT)
	withExperimentalCommitment(cmdCommitmentConvertPBT)
	cmdCommitmentConvertPBT.Flags().BoolVar(&convertPBTKeepHex, "keep-hex", false, "keep the source v3 hex commitment files")
	cmdCommitmentConvertPBT.Flags().StringVar(&convertPBTOutputDatadir, "output.datadir", "", "new datadir for the converted commitment files")
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
		return convertPBT(ctx, datadirCli, convertPBTOutputDatadir, convertPBTKeepHex, chain, logger)
	},
}

type pbinConversionPoint struct {
	BlockNum uint64
	TxNum    uint64
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
	if hasFiles, err := datadirHasFiles(outputDirs.DataDir); err != nil {
		return err
	} else if hasFiles {
		return fmt.Errorf("commitment convert-pbt: output datadir %s is not empty", outputDirs.DataDir)
	}

	oldBin, oldHexBin, oldV3, oldHash, oldSchema := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.BinCommitmentHash, statecfg.Schema
	defer func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.BinCommitmentHash, statecfg.Schema = oldBin, oldHexBin, oldV3, oldHash, oldSchema
	}()

	removeOutput := true
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
	point, err := readPBinSourcePoint(ctx, sourceDirs, sourceSettings, keepHex, logger)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outputDirs.Snap, 0o755); err != nil {
		return err
	}
	if err := os.MkdirAll(outputDirs.Tmp, 0o755); err != nil {
		return err
	}
	if hooks.afterOutput != nil {
		if err := hooks.afterOutput(); err != nil {
			return err
		}
	}
	if _, err = linkSnapshotsExceptCommitment(sourceDirs.Snap, outputDirs.Snap); err != nil {
		return err
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
	sourceTx, err := sourceDB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer sourceTx.Rollback()
	variant := dbstate.TrieVariantHex
	if sourceSettings != nil {
		variant = sourceSettings.TrieVariantName()
	}
	if keepHex && variant == dbstate.TrieVariantBin {
		return errors.New("commitment convert-pbt: --keep-hex needs a hex or hex+bin source")
	}
	if err := requirePBinSourceEnd(sourceAgg, point.TxNum); err != nil {
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
	if keepHex {
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
	targetAgg, err := openPBTState(ctx, outputDirs, targetSettings, targetRaw, logger)
	if err != nil {
		return err
	}
	defer targetAgg.Close()
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
	root, err := dbstate.ConvertPBin(ctx, dbstate.PBinConvertOptions{
		SourceAggregator:  sourceAgg,
		SourceTx:          sourceTx,
		TargetAggregator:  targetAgg,
		TargetTx:          targetTx,
		TargetDomain:      targetDomain,
		BlockNum:          point.BlockNum,
		EndTxNum:          point.TxNum,
		Hash:              eip8297.HashBytes,
		RangeWriterLimits: hooks.rangeWriterLimits,
	})
	targetTx.Rollback()
	if err != nil {
		return err
	}
	targetAgg.Close()
	if blockEnd && afterFork && root != header.Root {
		return fmt.Errorf("commitment convert-pbt: root %x differs from header root %x", root, header.Root)
	}
	visibleRanges := pbtVisibleSnapshotFiles(sourceAgg, sourceDirs)
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
	if err := verifyPBTOutputRows(ctx, outputDirs, finalSettings, targetDomain, point, logger); err != nil {
		return err
	}
	if err := verifyPBTOutputStandalone(ctx, outputDirs, finalSettings, point, root, logger); err != nil {
		return err
	}
	if err := dbstate.WriteErigonDBSettings(outputDirs, finalSettings); err != nil {
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

func verifyPBTOutputStandalone(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, point pbinConversionPoint, wantRoot common.Hash, logger log.Logger) error {
	configurePBTSourceVariant(settings)
	chaindataDir, err := os.MkdirTemp("", "convert-pbt-chaindata-")
	if err != nil {
		return err
	}
	defer func() { _ = dir.RemoveAll(chaindataDir) }()
	rawDB := mdbx.New(dbcfg.ChainDB, logger).Path(chaindataDir).MustOpen()
	defer rawDB.Close()
	agg, err := openPBTState(ctx, dirs, settings, nil, logger)
	if err != nil {
		return err
	}
	defer agg.Close()
	db, err := dbtemporal.New(rawDB, agg, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithoutCommitmentSeek())
	if err != nil {
		return err
	}
	defer domains.Close()
	contexts := make([]*commitmentdb.SharedDomainsCommitmentContext, 0, len(domains.CommitmentDomains()))
	for _, domain := range domains.CommitmentDomains() {
		contexts = append(contexts, domains.GetCommitmentCtxForDomain(domain))
	}
	txNum, blockNum, err := commitmentdb.SeekCommitments(ctx, tx, contexts...)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: standalone output: %w", err)
	}
	if txNum != point.TxNum || blockNum != point.BlockNum {
		return fmt.Errorf("commitment convert-pbt: standalone output checkpoint is (%d, %d), want (%d, %d)", blockNum, txNum, point.BlockNum, point.TxNum)
	}
	at := agg.BeginFilesRo()
	defer at.Close()
	gotRoot, err := dbstate.PBinStateRoot(at, nil, true, eip8297.HashBytes)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: standalone output leaves: %w", err)
	}
	if gotRoot != wantRoot {
		return fmt.Errorf("commitment convert-pbt: standalone output root %x differs from converted root %x", gotRoot, wantRoot)
	}
	return nil
}

func verifyPBTOutputRows(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, domain kv.Domain, point pbinConversionPoint, logger log.Logger) error {
	configurePBTSourceVariant(settings)
	chaindataDir, err := os.MkdirTemp("", "convert-pbt-chaindata-")
	if err != nil {
		return err
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
		return err
	}
	_, err = verifyPBTRows(ctx, rawDB, nil, dirs, settings, domain, "commitment convert-pbt: verify written rows", logger)
	return err
}

func requirePBinSourceEnd(agg *dbstate.Aggregator, endTxNum uint64) error {
	at := agg.BeginFilesRo()
	defer at.Close()
	return dbstate.ForEachPBinLeaf(at, nil, true, func(leaf dbstate.PBinLeaf) error {
		if leaf.Stamp > endTxNum {
			return fmt.Errorf("commitment convert-pbt: the last file holds writes up to txNum %d, after conversion txNum %d; wait for the next step", leaf.Stamp, endTxNum)
		}
		return nil
	})
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
	maxTxNum, found, txErr := blockReader.TxnumReader().MaxExact(ctx, tx, point.BlockNum)
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
	files, err := pbtSnapshotFiles(sourceDirs, func(domain kv.Domain) bool { return domain == kv.CommitmentDomain })
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
