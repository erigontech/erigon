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
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
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
	convertPBTKeepHex        bool
	convertPBTOutputDatadir  string
	convertPBTOutputHook     func() error
	convertPBTStandaloneHook func(datadir.Dirs) error
)

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
	if sourcePath == "" || outputPath == "" {
		return errors.New("commitment convert-pbt: source and output datadirs are required")
	}
	sourceDirs := datadir.Open(sourcePath)
	outputDirs := datadir.Open(outputPath)
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

	oldDatadir, oldChaindata, oldRebuildOutput := datadirCli, chaindata, rebuildOutputDatadir
	oldBin, oldHexBin, oldV3, oldHash, oldSchema := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.BinCommitmentHash, statecfg.Schema
	defer func() {
		datadirCli, chaindata = oldDatadir, oldChaindata
		rebuildOutputDatadir = oldRebuildOutput
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
	configurePBTSourceVariant(sourceSettings)
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
	if convertPBTOutputHook != nil {
		if err := convertPBTOutputHook(); err != nil {
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
	if _, err := linkSnapshotsExceptCommitment(sourceDirs.Snap, stagingDirs.Snap); err != nil {
		return err
	}
	if sourceSettings != nil {
		if err := dbstate.WriteErigonDBSettings(stagingDirs, sourceSettings); err != nil {
			return err
		}
	}
	datadirCli = stagingDirs.DataDir
	chaindata = sourceDirs.Chaindata
	rebuildOutputDatadir = stagingDirs.DataDir
	sourceDB, err := openDB(ctx, dbCfg(dbcfg.ChainDB, sourceDirs.Chaindata), false, chainName, logger)
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: open source: %w", err)
	}
	defer sourceDB.Close()
	sourceAgg := sourceDB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
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
	header, blockEnd, afterFork, err := readPBinForkPoint(ctx, sourceTx, point)
	if err != nil {
		return err
	}
	if !keepHex && !afterFork {
		return fmt.Errorf("commitment convert-pbt: bin-only output requires a conversion point after the binary trie fork")
	}

	hashName := statecfg.BinCommitmentHash
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
	statecfg.ExperimentalCommitmentV3 = keepHex
	if keepHex {
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}

	internal, ok := sourceDB.(interface{ InternalDB() kv.RwDB })
	if !ok {
		return errors.New("commitment convert-pbt: source database has no raw database")
	}
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
	targetAgg, err := dbstate.New(outputDirs).Logger(logger).WithErigonDBSettings(targetSettings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return err
	}
	defer targetAgg.Close()
	if err := targetAgg.OpenFolder(internal.InternalDB()); err != nil {
		return err
	}
	if err := requirePBinSourceEnd(targetAgg, point.TxNum); err != nil {
		return err
	}
	targetDB, err := dbtemporal.New(internal.InternalDB(), targetAgg, nil)
	if err != nil {
		return err
	}
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
	verifyTx, err := targetDB.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer verifyTx.Rollback()
	err = dbstate.VerifyPBinDomain(ctx, verifyTx, targetAgg, targetDomain)
	verifyTx.Rollback()
	if err != nil {
		return fmt.Errorf("commitment convert-pbt: verify written rows: %w", err)
	}
	if blockEnd && afterFork && !bytes.Equal(root[:], header.Root[:]) {
		return fmt.Errorf("commitment convert-pbt: root %x differs from header root %x", root, header.Root)
	}
	if keepHex {
		if err := linkPBinHexFiles(sourceDirs.SnapDomain, outputDirs.SnapDomain); err != nil {
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
	if !keepHex {
		finalSettings.ReferencesInCommitmentBranches = new(bool)
	}
	targetAgg.Close()
	if convertPBTStandaloneHook != nil {
		if err := convertPBTStandaloneHook(outputDirs); err != nil {
			return err
		}
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

func readPBinSourcePoint(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, keepHex bool, logger log.Logger) (pbinConversionPoint, error) {
	if keepHex && (settings == nil || settings.TrieVariantName() == dbstate.TrieVariantHex) {
		statecfg.ExperimentalCommitmentV3 = true
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	}
	aggOpts := dbstate.New(dirs).Logger(logger)
	if settings != nil {
		aggOpts = aggOpts.WithErigonDBSettings(settings)
	}
	aggOpts = aggOpts.SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps()
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
		statecfg.ExperimentalCommitmentV3 = true
		statecfg.InitSchemas()
		statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
		statecfg.BinCommitmentHash = settings.TrieHashName()
	case dbstate.TrieVariantBin:
		statecfg.ExperimentalBinCommitment = true
		statecfg.ExperimentalHexBinCommitment = false
		statecfg.ExperimentalCommitmentV3 = false
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
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
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

func requirePBinSourceEnd(agg *dbstate.Aggregator, endTxNum uint64) error {
	at := agg.BeginFilesRo()
	defer at.Close()
	files := at.Files(kv.AccountsDomain)
	if len(files) == 0 {
		return nil
	}
	last := files[len(files)-1]
	if last.StartRootNum() > endTxNum || last.EndRootNum() < endTxNum {
		return fmt.Errorf("commitment convert-pbt: source state txNum %d is outside the accounts file frontier [%d,%d)", endTxNum, last.StartRootNum(), last.EndRootNum())
	}
	return nil
}

func readPBinForkPoint(ctx context.Context, tx kv.TemporalTx, point pbinConversionPoint) (header *types.Header, blockEnd, afterFork bool, err error) {
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return nil, false, false, err
	}
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return nil, false, false, err
	}
	header = rawdb.ReadHeaderByNumber(tx, point.BlockNum)
	if chainConfig == nil || header == nil {
		return header, false, false, nil
	}
	afterFork = chainConfig.IsBinaryTrie(header.Time)
	maxTxNum, txErr := rawdbv3.TxNums.Max(ctx, tx, point.BlockNum)
	if txErr == nil {
		blockEnd = maxTxNum == point.TxNum
	}
	return header, blockEnd, afterFork, nil
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
