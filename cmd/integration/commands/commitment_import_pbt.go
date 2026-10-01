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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/debug"
)

var (
	importPBTSnapshot string
	importPBTSwapHook func(string) error
)

type pbtImportMeta struct {
	ChainID        string `json:"chainId"`
	Block          uint64 `json:"block"`
	BlockHash      string `json:"blockHash"`
	TxNum          uint64 `json:"txNum"`
	HashSuite      string `json:"hashSuite"`
	StateRoot      string `json:"stateRoot"`
	PBTRoot        string `json:"pbtRoot"`
	SnapshotDigest string `json:"snapshotDigest"`
}

func init() {
	withChain(cmdCommitmentImportPBT)
	withDataDir(cmdCommitmentImportPBT)
	withConfig(cmdCommitmentImportPBT)
	withExperimentalCommitment(cmdCommitmentImportPBT)
	cmdCommitmentImportPBT.Flags().StringVar(&importPBTSnapshot, "snapshot", "", "PBT snapshot artifact")
	must(cmdCommitmentImportPBT.MarkFlagRequired("snapshot"))
	commitmentCmd.AddCommand(cmdCommitmentImportPBT)
}

var cmdCommitmentImportPBT = &cobra.Command{
	Use:          "import-pbt",
	Short:        "import a PBT snapshot into a stopped hex node for tests",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger, ctx := debug.SetupCobra(cmd, "integration"), cmd.Context()
		return importPBT(ctx, datadirCli, importPBTSnapshot, chain, logger)
	},
}

func importPBT(ctx context.Context, dataDir, snapshotPath, chainName string, logger log.Logger) error {
	if dataDir == "" || snapshotPath == "" {
		return errors.New("commitment import-pbt: datadir and snapshot are required")
	}
	dirs := datadir.Open(dataDir)
	meta, err := readPBTImportMeta(snapshotPath)
	if err != nil {
		return err
	}
	snapshot, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer snapshot.Close()
	snapshotInfo, err := snapshot.Stat()
	if err != nil {
		return err
	}
	if err := validatePBTImportDigest(snapshot, snapshotInfo.Size(), meta.SnapshotDigest); err != nil {
		return err
	}
	marker, err := dbstate.ReadPBTImportMarker(dirs)
	if err != nil {
		return fmt.Errorf("%w; rerun import-pbt --snapshot %s", err, snapshotPath)
	}
	absSnapshotPath, err := filepath.Abs(snapshotPath)
	if err != nil {
		return err
	}
	if marker != nil && filepath.Clean(marker.SnapshotPath) != filepath.Clean(absSnapshotPath) {
		return fmt.Errorf("commitment import-pbt is incomplete for %s; rerun import-pbt --snapshot %s", marker.SnapshotPath, marker.SnapshotPath)
	}
	if marker != nil && marker.SnapshotHash != meta.SnapshotDigest {
		return fmt.Errorf("commitment import-pbt: incomplete marker does not match snapshot digest")
	}

	oldBin, oldHexBin, oldV3, oldParallel, oldHash, oldSchema := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash, statecfg.Schema
	oldSuite := commitment.PBinHashSuiteName()
	defer func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment, statecfg.ExperimentalCommitmentV3, statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash, statecfg.Schema = oldBin, oldHexBin, oldV3, oldParallel, oldHash, oldSchema
		_ = commitment.SetPBinHashSuite(oldSuite)
	}()

	settings, err := dbstate.ReadErigonDBSettings(dirs)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("commitment import-pbt: target settings are missing")
	}
	if err != nil {
		return err
	}
	if marker != nil && settings.TrieVariantName() == dbstate.TrieVariantHexBin {
		if marker.Settings.TrieHashName() == settings.TrieHashName() && marker.Settings.ConversionTxNum != nil && *marker.Settings.ConversionTxNum == meta.TxNum && marker.Settings.ConversionBlockNum != nil && *marker.Settings.ConversionBlockNum == meta.Block {
			return dbstate.RemovePBTImportMarker(dirs)
		}
		return fmt.Errorf("commitment import-pbt: incomplete marker does not match target settings")
	}
	if settings.TrieVariantName() != dbstate.TrieVariantHex {
		return fmt.Errorf("commitment import-pbt: target must be hex-only, got %s", settings.TrieVariantName())
	}
	if frozenAt, frozen := settings.FrozenAt(kv.CommitmentDomain); frozen {
		return fmt.Errorf("commitment import-pbt: target domain %s is frozen at txNum %d", kv.CommitmentDomain, frozenAt)
	}
	if marker == nil {
		if err := validatePBTImportNoBinFiles(dirs); err != nil {
			return err
		}
	}
	detected, err := dbstate.EnableCommitmentV3FromFiles(dirs)
	if err != nil {
		return err
	}
	if !detected {
		return errors.New("commitment import-pbt: target is not a v3 hex datadir")
	}
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	requestedHash := statecfg.BinCommitmentHash
	if requestedHash != "" && requestedHash != meta.HashSuite {
		return fmt.Errorf("commitment import-pbt: hash suite %q does not match snapshot suite %q", requestedHash, meta.HashSuite)
	}
	if err := commitment.SetPBinHashSuite(meta.HashSuite); err != nil {
		return err
	}

	rawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return fmt.Errorf("commitment import-pbt: open target read-only: %w", err)
	}
	rawDBOpen := true
	defer func() {
		if rawDBOpen {
			rawDB.Close()
		}
	}()
	blockReader, blockView, closeBlockReader, err := openPBTBlockReader(ctx, dirs, rawDB, logger)
	if err != nil {
		return err
	}
	defer func() {
		if closeBlockReader != nil {
			closeBlockReader()
		}
	}()
	readTx, err := rawDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer readTx.Rollback()
	readerTx := pbtBlockFilesTx{Tx: readTx, view: blockView}
	header, err := blockReader.HeaderByHash(ctx, readerTx, common.HexToHash(meta.BlockHash))
	if err != nil {
		readTx.Rollback()
		return err
	}
	if header == nil {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: block %s is not local or canonical", meta.BlockHash)
	}
	if header.Number.Uint64() != meta.Block {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: snapshot block %d has header number %d", meta.Block, header.Number.Uint64())
	}
	canonical, found, err := blockReader.CanonicalHash(ctx, readerTx, meta.Block)
	if err != nil {
		readTx.Rollback()
		return err
	}
	if !found || canonical != header.Hash() {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: block %d is not canonical with hash %s", meta.Block, meta.BlockHash)
	}
	genesisHash, err := rawdb.ReadCanonicalHash(readTx, 0)
	if err != nil {
		readTx.Rollback()
		return err
	}
	chainConfig, err := rawdb.ReadChainConfig(readTx, genesisHash)
	if err != nil {
		readTx.Rollback()
		return err
	}
	if chainConfig == nil {
		readTx.Rollback()
		return errors.New("commitment import-pbt: chain config is missing")
	}
	if chainName != "" && chainConfig.ChainName != chainName {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: chain %q does not match target chain %q", chainName, chainConfig.ChainName)
	}
	if chainConfig.ChainID == nil || chainConfig.ChainID.String() != meta.ChainID {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: chain id %q does not match target", meta.ChainID)
	}
	progress, err := stages.GetStageProgress(readTx, stages.Execution)
	if err != nil {
		readTx.Rollback()
		return err
	}
	if progress != meta.Block {
		readTx.Rollback()
		return pbtImportProgressError(progress, meta, chainConfig.ChainName)
	}
	lastTx, found, err := blockReader.TxnumReader().MaxExact(ctx, readerTx, meta.Block)
	if err != nil {
		readTx.Rollback()
		return err
	}
	if !found {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: block %d has no txNum mapping", meta.Block)
	}
	if lastTx != meta.TxNum {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: snapshot checkpoint (%d, %d) is not the block end; target block %d ends at txNum %d; run integration stage_exec --block=%d --chain=%s, then export-pbt", meta.Block, meta.TxNum, meta.Block, lastTx, meta.Block, chainConfig.ChainName)
	}
	if common.HexToHash(meta.StateRoot) != header.Root {
		readTx.Rollback()
		return fmt.Errorf("commitment import-pbt: snapshot stateRoot %s differs from header root %s", meta.StateRoot, header.Root)
	}
	readTx.Rollback()
	hexBlock, hexTx, err := readPBTImportHexCheckpoint(ctx, dirs, rawDB, settings, logger)
	if err != nil {
		return err
	}
	if hexBlock != meta.Block || hexTx != meta.TxNum {
		return fmt.Errorf("commitment import-pbt: hex commitment checkpoint is (%d, %d), want (%d, %d) at block %d; run integration stage_exec --block=%d --chain=%s, then export-pbt", hexBlock, hexTx, meta.Block, meta.TxNum, meta.Block, meta.Block, chainConfig.ChainName)
	}
	closeBlockReader()
	closeBlockReader = nil
	rawDB.Close()
	rawDBOpen = false

	stageRoot, err := os.MkdirTemp(filepath.Dir(dirs.DataDir), ".import-pbt-")
	if err != nil {
		return err
	}
	stageDirs := datadir.Open(stageRoot)
	defer func() { _ = dir.RemoveAll(stageRoot) }()
	if _, err := linkSnapshotsExceptCommitment(dirs.Snap, stageDirs.Snap); err != nil {
		return err
	}
	if err := linkPBinHexFiles(dirs.SnapDomain, stageDirs.SnapDomain); err != nil {
		return err
	}
	if err := linkPBinCommitmentFiles(dirs, stageDirs, settings.StepSize, meta.TxNum); err != nil {
		return err
	}
	if err := os.MkdirAll(stageDirs.Tmp, 0o755); err != nil {
		return err
	}
	variant := dbstate.TrieVariantHexBin
	hashName := meta.HashSuite
	conversionBlock, conversionTx := meta.Block, meta.TxNum
	finalSettings := &dbstate.ErigonDBSettings{
		StepSize: settings.StepSize, StepsInFrozenFile: settings.StepsInFrozenFile,
		ReferencesInCommitmentBranches: settings.ReferencesInCommitmentBranches,
		TrieVariant:                    &variant, TrieHash: &hashName,
		ConversionBlockNum: &conversionBlock, ConversionTxNum: &conversionTx,
	}
	if err := dbstate.WriteErigonDBSettings(stageDirs, finalSettings); err != nil {
		return err
	}
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = meta.HashSuite
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	targetAgg, err := dbstate.New(stageDirs).Logger(logger).WithErigonDBSettings(finalSettings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return err
	}
	defer targetAgg.Close()
	if err := os.MkdirAll(dirs.Tmp, 0o755); err != nil {
		return err
	}
	stageRawPath, err := os.MkdirTemp(dirs.Tmp, "import-pbt-chaindata-")
	if err != nil {
		return err
	}
	defer func() { _ = dir.RemoveAll(stageRawPath) }()
	stageRaw, err := mdbx.New(dbcfg.ChainDB, logger).Path(stageRawPath).Open(ctx)
	if err != nil {
		return err
	}
	defer stageRaw.Close()
	if err := targetAgg.OpenFolder(stageRaw); err != nil {
		return err
	}
	targetDB, err := dbtemporal.New(stageRaw, targetAgg, nil)
	if err != nil {
		return err
	}
	defer targetDB.Close()
	targetTx, err := targetDB.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer targetTx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(ctx, targetTx, logger, execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomainOnly(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	if err != nil {
		return err
	}
	writer, err := dbstate.NewPBinRangeWriterWithinFiles(targetAgg, kv.CommitmentBinDomain, meta.TxNum)
	if err != nil {
		domains.Close()
		return err
	}
	var artifactRoot common.Hash
	root, err := writer.WriteAtBlock(ctx, targetTx, domains, func(emit func(dbstate.PBinLeaf) error) error {
		var streamErr error
		artifactRoot, streamErr = dbstate.ForEachPBinArtifactLeaf(snapshot, snapshotInfo.Size(), eip8297.HashBytes, func(leaf dbstate.PBinLeaf) error {
			leaf.Stamp = writer.PBinLeafStamp()
			return emit(leaf)
		})
		return streamErr
	}, meta.Block)
	checkpointState := writer.PBinCommitmentState()
	checkpointInFiles := writer.PBinCommitmentStateInFiles()
	if !checkpointInFiles {
		if err := domains.DomainPut(kv.CommitmentBinDomain, targetTx, commitment.KeyCommitmentState, checkpointState, meta.TxNum, nil); err != nil {
			domains.Close()
			return err
		}
		if err := domains.Flush(ctx, targetTx); err != nil {
			domains.Close()
			return err
		}
	}
	domains.Close()
	if err != nil {
		return err
	}
	wantRoot := common.HexToHash(meta.PBTRoot)
	if root != artifactRoot || root != wantRoot {
		return fmt.Errorf("commitment import-pbt: written root %s differs from artifact root %s", root, wantRoot)
	}
	domains.Close()
	if checkpointInFiles {
		targetTx.Rollback()
	} else if err := targetTx.Commit(); err != nil {
		return err
	}
	targetDB.Close()
	targetAgg.Close()
	stageRaw.Close()
	if err := verifyPBTImportRows(ctx, stageDirs, finalSettings, logger, dirs.Chaindata); err != nil {
		return err
	}
	if importPBTSwapHook != nil {
		if err := importPBTSwapHook("staging-built"); err != nil {
			return err
		}
	}
	if err := dbstate.WritePBTImportMarker(dirs, &dbstate.PBTImportMarker{SnapshotPath: absSnapshotPath, SnapshotHash: meta.SnapshotDigest, Settings: finalSettings}); err != nil {
		return err
	}
	if marker != nil {
		if err := removePBTImportFilesForRecovery(dirs); err != nil {
			return err
		}
	}
	moved, err := movePBTImportBinFiles(stageDirs, dirs)
	if err != nil {
		removePBTImportFiles(moved)
		_ = dbstate.RemovePBTImportMarker(dirs)
		return err
	}
	settingsWritten := false
	defer func() {
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
		if !settingsWritten {
			removePBTImportFiles(moved)
			_ = dbstate.RemovePBTImportMarker(dirs)
		}
	}()
	if importPBTSwapHook != nil {
		if err := importPBTSwapHook("files-moved"); err != nil {
			return err
		}
	}
	if err := writePBTImportCheckpoint(ctx, dirs, finalSettings, checkpointState, !checkpointInFiles, meta.TxNum, common.HexToHash(meta.BlockHash), meta.Block, root, logger); err != nil {
		return err
	}
	if err := dbstate.WriteErigonDBSettings(dirs, finalSettings); err != nil {
		return err
	}
	settingsWritten = true
	if importPBTSwapHook != nil {
		if err := importPBTSwapHook("settings-written"); err != nil {
			return err
		}
	}
	if err := dbstate.RemovePBTImportMarker(dirs); err != nil {
		return err
	}
	logger.Info("imported PBT snapshot", "block", meta.Block, "txNum", meta.TxNum, "root", root.Hex())
	return nil
}

func verifyPBTImportRows(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, logger log.Logger, chaindataPath string) error {
	if err := os.MkdirAll(dirs.Tmp, 0o755); err != nil {
		return err
	}
	rawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, chaindataPath), true)
	if err != nil {
		return err
	}
	defer rawDB.Close()
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return err
	}
	defer agg.Close()
	if err := agg.OpenFolder(rawDB); err != nil {
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
	if err := dbstate.VerifyPBinDomain(ctx, tx, agg, kv.CommitmentBinDomain); err != nil {
		return fmt.Errorf("commitment import-pbt: verify written rows: %w", err)
	}
	return nil
}

func writePBTImportCheckpoint(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, state []byte, writeState bool, txNum uint64, blockHash common.Hash, blockNum uint64, root common.Hash, logger log.Logger) error {
	rawDB, err := dbCfg(dbcfg.ChainDB, dirs.Chaindata).Open(ctx)
	if err != nil {
		return fmt.Errorf("commitment import-pbt: open target for checkpoint: %w", err)
	}
	defer rawDB.Close()
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return err
	}
	defer agg.Close()
	if err := agg.OpenFolder(rawDB); err != nil {
		return err
	}
	db, err := dbtemporal.New(rawDB, agg, nil)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomainOnly(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	if err != nil {
		return err
	}
	defer domains.Close()
	if writeState {
		if err := domains.DomainPut(kv.CommitmentBinDomain, tx, commitment.KeyCommitmentState, state, txNum, nil); err != nil {
			return err
		}
		if err := domains.Flush(ctx, tx); err != nil {
			return err
		}
	}
	if err := rawdb.WriteShadowStateRoot(tx, blockHash, blockNum, root[:]); err != nil {
		return err
	}
	return tx.Commit()
}

func readPBTImportMeta(snapshotPath string) (pbtImportMeta, error) {
	data, err := os.ReadFile(filepath.Join(filepath.Dir(snapshotPath), "pbt-snapshot.meta.json"))
	if err != nil {
		return pbtImportMeta{}, fmt.Errorf("commitment import-pbt: read metadata: %w", err)
	}
	var meta pbtImportMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return pbtImportMeta{}, fmt.Errorf("commitment import-pbt: decode metadata: %w", err)
	}
	for name, value := range map[string]string{"blockHash": meta.BlockHash, "hashSuite": meta.HashSuite, "stateRoot": meta.StateRoot, "pbtRoot": meta.PBTRoot, "snapshotDigest": meta.SnapshotDigest} {
		if value == "" {
			return pbtImportMeta{}, fmt.Errorf("commitment import-pbt: metadata field %s is empty", name)
		}
	}
	return meta, nil
}

func validatePBTImportDigest(file *os.File, size int64, expected string) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hash := keccak.NewFastKeccak()
	if _, err := io.Copy(hash, io.LimitReader(file, size)); err != nil {
		return err
	}
	sum := hash.Sum(nil)
	if common.BytesToHash(sum) != common.HexToHash(expected) {
		return fmt.Errorf("commitment import-pbt: snapshot digest %s differs from metadata %s", common.BytesToHash(sum), expected)
	}
	return nil
}

func validatePBTImportNoBinFiles(dirs datadir.Dirs) error {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.domain == kv.CommitmentBinDomain {
			return errors.New("commitment import-pbt: target already has a binary commitment domain")
		}
	}
	return nil
}

func readPBTImportHexCheckpoint(ctx context.Context, dirs datadir.Dirs, rawDB kv.RwDB, settings *dbstate.ErigonDBSettings, logger log.Logger) (uint64, uint64, error) {
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer agg.Close()
	if err := agg.OpenFolder(rawDB); err != nil {
		return 0, 0, err
	}
	at := agg.BeginFilesRo()
	defer at.Close()
	readTx, err := rawDB.BeginRo(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer readTx.Rollback()
	value, _, found, err := at.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, readTx, kv.GetLatestOptions{})
	if err != nil {
		return 0, 0, err
	}
	if !found {
		return 0, 0, errors.New("commitment import-pbt: hex commitment checkpoint is missing from files")
	}
	block, tx, _, err := commitment.DecodeCommitmentV3State(value)
	return block, tx, err
}

func pbtImportProgressError(progress uint64, meta pbtImportMeta, chainName string) error {
	return fmt.Errorf("commitment import-pbt: target is at block %d, snapshot is at block %d; run integration stage_exec --block=%d --chain=%s, then export-pbt", progress, meta.Block, progress, chainName)
}

func movePBTImportBinFiles(stageDirs, targetDirs datadir.Dirs) ([]string, error) {
	files, err := pbtAttachFiles(stageDirs)
	if err != nil {
		return nil, err
	}
	moved := make([]string, 0)
	for _, file := range files {
		if file.domain != kv.CommitmentBinDomain {
			continue
		}
		rel, err := filepath.Rel(stageDirs.Snap, file.path)
		if err != nil {
			return moved, err
		}
		destination := filepath.Join(targetDirs.Snap, rel)
		if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
			return moved, err
		}
		if err := os.Rename(file.path, destination); err != nil {
			return moved, err
		}
		moved = append(moved, destination)
	}
	if len(moved) == 0 {
		return moved, errors.New("commitment import-pbt: staged binary commitment files are missing")
	}
	return moved, nil
}

func removePBTImportFiles(files []string) {
	for _, file := range files {
		_ = dir.RemoveFile(file)
	}
}

func removePBTImportFilesForRecovery(dirs datadir.Dirs) error {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	for _, file := range files {
		if file.domain == kv.CommitmentBinDomain {
			if err := dir.RemoveFile(file.path); err != nil {
				return err
			}
		}
	}
	return nil
}
