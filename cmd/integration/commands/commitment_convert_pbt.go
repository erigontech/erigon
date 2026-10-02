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

	"github.com/spf13/cobra"

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
	afterOutput func() error
	standalone  func(datadir.Dirs) error
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
	if err := checkPBTChainName(ctx, sourceDirs, chainName); err != nil {
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

	stagingRoot, err := os.MkdirTemp(filepath.Dir(outputDirs.DataDir), ".convert-pbt-source-")
	if err != nil {
		return err
	}
	stagingDirs := datadir.Open(stagingRoot)
	defer func() { _ = dir.RemoveAll(stagingDirs.DataDir) }()
	if err := os.MkdirAll(stagingDirs.Snap, 0o755); err != nil {
		return err
	}
	if _, linkErr := linkSnapshotsExceptCommitment(sourceDirs.Snap, stagingDirs.Snap); linkErr != nil {
		return linkErr
	}
	if sourceSettings != nil {
		if err := dbstate.WriteErigonDBSettings(stagingDirs, sourceSettings); err != nil {
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
	header, blockEnd, afterFork, err := readPBinForkPoint(ctx, pbtTemporalBlockFilesTx{TemporalTx: sourceTx, view: blockView}, blockReader, point)
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
	targetSettings := &dbstate.ErigonDBSettings{
		StepSize:                       sourceAgg.StepSize(),
		StepsInFrozenFile:              sourceAgg.StepsInFrozenFile(),
		ReferencesInCommitmentBranches: new(bool),
	}
	trieVariant := dbstate.TrieVariantBin
	if keepHex {
		trieVariant = dbstate.TrieVariantHexBin
	}
	targetSettings.TrieVariant = &trieVariant
	targetSettings.TrieHash = &hashName
	targetAgg, err := dbstate.NewPBTStateAggregator(outputDirs, targetSettings, logger).Open(ctx)
	if err != nil {
		return err
	}
	targetAggClosed := false
	defer func() {
		if !targetAggClosed {
			targetAgg.Close()
		}
	}()
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
	if err := targetAgg.OpenFolder(targetRaw); err != nil {
		return err
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
	targetDomain := kv.CommitmentDomain
	if keepHex {
		targetDomain = kv.CommitmentBinDomain
	}
	root, err := dbstate.ConvertPBin(ctx, dbstate.PBinConvertOptions{
		SourceAggregator: sourceAgg,
		SourceTx:         sourceTx,
		TargetAggregator: targetAgg,
		TargetTx:         targetTx,
		TargetDomain:     targetDomain,
		BlockNum:         point.BlockNum,
		EndTxNum:         point.TxNum,
		Hash:             eip8297.HashBytes,
	})
	targetTx.Rollback()
	if err != nil {
		return err
	}
	targetAgg.Close()
	targetAggClosed = true
	if blockEnd && afterFork && !bytes.Equal(root[:], header.Root[:]) {
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
	if !keepHex {
		finalSettings.ReferencesInCommitmentBranches = new(bool)
	}
	targetAgg.Close()
	if hooks.standalone != nil {
		if err := hooks.standalone(outputDirs); err != nil {
			return err
		}
	}
	if err := verifyPBTOutputRows(ctx, outputDirs, finalSettings, targetDomain, point, logger); err != nil {
		return err
	}
	if err := verifyPBTOutputStandalone(ctx, outputDirs, finalSettings, point, logger); err != nil {
		return err
	}
	if err := dbstate.WriteErigonDBSettings(outputDirs, finalSettings); err != nil {
		return err
	}
	removeOutput = false
	return nil
}

func checkPBTChainName(ctx context.Context, dirs datadir.Dirs, expected string) error {
	if expected == "" {
		return nil
	}
	rawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: open source chain config: %w", err)
	}
	defer rawDB.Close()
	tx, err := rawDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: read genesis hash: %w", err)
	}
	config, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: read chain config: %w", err)
	}
	if config == nil || config.ChainName != expected {
		got := "unknown"
		if config != nil {
			got = config.ChainName
		}
		return fmt.Errorf("commitment convert-pbt: chain %q does not match source chain %q", expected, got)
	}
	return nil
}

func readPBinSourcePoint(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, keepHex bool, logger log.Logger) (pbinConversionPoint, error) {
	if keepHex && (settings == nil || settings.TrieVariantName() == dbstate.TrieVariantHex) {
		statecfg.ConfigureCommitmentV3Records(true)
	}
	aggOpts := dbstate.NewPBTStateAggregator(dirs, settings, logger)
	agg, err := aggOpts.Open(ctx)
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

func verifyPBTOutputStandalone(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, point pbinConversionPoint, logger log.Logger) error {
	configurePBTSourceVariant(settings)
	chaindataDir, err := os.MkdirTemp("", "convert-pbt-chaindata-")
	if err != nil {
		return err
	}
	defer func() { _ = dir.RemoveAll(chaindataDir) }()
	rawDB := mdbx.New(dbcfg.ChainDB, logger).Path(chaindataDir).MustOpen()
	defer rawDB.Close()
	agg, err := dbstate.NewPBTStateAggregator(dirs, settings, logger).Open(ctx)
	if err != nil {
		return err
	}
	defer agg.Close()
	if err := agg.OpenFolder(nil); err != nil {
		return err
	}
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
	agg, err := dbstate.NewPBTStateAggregator(dirs, settings, logger).Open(ctx)
	if err != nil {
		return err
	}
	defer agg.Close()
	if err := agg.OpenFolder(nil); err != nil {
		return err
	}
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
	if err := dbstate.VerifyPBinDomain(ctx, tx, agg, domain); err != nil {
		return fmt.Errorf("commitment convert-pbt: verify written rows: %w", err)
	}
	return nil
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

func readPBinForkPoint(ctx context.Context, tx kv.TemporalTx, blockReader *freezeblocks.BlockReader, point pbinConversionPoint) (header *types.Header, blockEnd, afterFork bool, err error) {
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
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	touched := make(map[string]struct{})
	for _, file := range files {
		if pbtAttachStateDomain(file.domain) && !pbtAttachAdoptsFile(file) {
			if err := dir.RemoveFile(file.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			touched[filepath.Dir(file.path)] = struct{}{}
		}
	}
	for directory := range touched {
		if err := dir.FsyncDir(directory); err != nil {
			return err
		}
	}
	return nil
}

func linkPBinHexFiles(sourceDir, outputDir string) error {
	entries, err := os.ReadDir(sourceDir)
	if errors.Is(err, fs.ErrNotExist) {
		return errors.New("commitment convert-pbt: source hex commitment files are missing")
	}
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		parsed, _, ok := snaptype.ParseFileName(sourceDir, entry.Name())
		if !ok || parsed.TypeString != kv.CommitmentDomain.String() {
			continue
		}
		if err := os.Link(filepath.Join(sourceDir, entry.Name()), filepath.Join(outputDir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func linkPBinCommitmentFiles(sourceDirs, outputDirs datadir.Dirs, stepSize, endTxNum uint64) error {
	for _, roots := range [][2]string{
		{sourceDirs.SnapHistory, outputDirs.SnapHistory},
		{sourceDirs.SnapIdx, outputDirs.SnapIdx},
		{sourceDirs.SnapAccessors, outputDirs.SnapAccessors},
	} {
		entries, err := os.ReadDir(roots[0])
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			parsed, _, ok := snaptype.ParseFileName(roots[0], entry.Name())
			if !ok || parsed.TypeString != kv.CommitmentDomain.String() || parsed.From*stepSize > endTxNum {
				continue
			}
			if err := os.MkdirAll(roots[1], 0o755); err != nil {
				return err
			}
			if err := os.Link(filepath.Join(roots[0], entry.Name()), filepath.Join(roots[1], entry.Name())); err != nil {
				return err
			}
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

func pbtVisibleSnapshotFiles(sourceAgg *dbstate.Aggregator, sourceDirs datadir.Dirs) map[string]struct{} {
	visibleRanges := make(map[string]struct{})
	at := sourceAgg.BeginFilesRo()
	defer at.Close()
	for _, file := range at.AllFiles() {
		rel, err := filepath.Rel(sourceAgg.Dirs().Snap, file.Fullpath())
		if err != nil {
			continue
		}
		parsed, _, ok := snaptype.ParseFileName(filepath.Dir(file.Fullpath()), filepath.Base(file.Fullpath()))
		if !ok {
			continue
		}
		visibleRanges[pbtSnapshotFileKey(pbtSnapshotFileFamily(rel), parsed.TypeString, parsed.From, parsed.To)] = struct{}{}
	}
	addPBTCommitmentVisibleFiles(visibleRanges, sourceDirs)
	return visibleRanges
}

func addPBTCommitmentVisibleFiles(visibleRanges map[string]struct{}, dirs datadir.Dirs) {
	type commitmentFile struct {
		family, kind, ext string
		from, to          uint64
	}
	var files []commitmentFile
	for _, root := range []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx} {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			parsed, _, ok := snaptype.ParseFileName(root, entry.Name())
			if !ok || parsed.TypeString != kv.CommitmentDomain.String() {
				continue
			}
			rel, err := filepath.Rel(dirs.Snap, filepath.Join(root, entry.Name()))
			if err != nil {
				continue
			}
			files = append(files, commitmentFile{
				family: pbtSnapshotFileFamily(rel), kind: parsed.TypeString, ext: filepath.Ext(entry.Name()), from: parsed.From, to: parsed.To,
			})
		}
	}
	for i, file := range files {
		covered := false
		for j, other := range files {
			if i != j && file.family == other.family && file.kind == other.kind && file.ext == other.ext &&
				other.from <= file.from && other.to >= file.to && (other.from < file.from || other.to > file.to) {
				covered = true
				break
			}
		}
		if !covered {
			visibleRanges[pbtSnapshotFileKey(file.family, file.kind, file.from, file.to)] = struct{}{}
		}
	}
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
