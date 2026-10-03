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
	"slices"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/debug"
)

var importPBTSnapshot string

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
	return importPBTWithHook(ctx, dataDir, snapshotPath, chainName, logger, nil)
}

func pbtImportRerunCommand(dataDir, chainName, snapshotPath string) string {
	return fmt.Sprintf("integration commitment import-pbt --datadir=%s --chain=%s --snapshot=%s", dataDir, chainName, snapshotPath)
}

func importPBTWithHook(ctx context.Context, dataDir, snapshotPath, chainName string, logger log.Logger, swapHook func(string) error) (retErr error) {
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
	marker, markerErr := dbstate.ReadPBTImportMarker(dirs)
	if markerErr != nil {
		if settings, settingsErr := dbstate.ReadErigonDBSettings(dirs); settingsErr == nil && settings.TrieVariantName() == dbstate.TrieVariantHexBin {
			if cleanupErr := recoverPBTImportSettings(dirs, settings, nil); cleanupErr != nil {
				return fmt.Errorf("%w; partial import cleanup failed: %w", markerErr, cleanupErr)
			}
			return fmt.Errorf("%w; rerun %s", markerErr, pbtImportRerunCommand(dirs.DataDir, chainName, snapshotPath))
		}
		if removeErr := dbstate.RemovePBTImportMarker(dirs); removeErr != nil {
			return fmt.Errorf("%w; remove corrupt import marker: %w", markerErr, removeErr)
		}
		return fmt.Errorf("%w; rerun %s", markerErr, pbtImportRerunCommand(dirs.DataDir, chainName, snapshotPath))
	}
	absSnapshotPath, err := filepath.Abs(snapshotPath)
	if err != nil {
		return err
	}
	if marker != nil && filepath.Clean(marker.SnapshotPath) != filepath.Clean(absSnapshotPath) {
		return fmt.Errorf("commitment import-pbt is incomplete for %s; rerun %s", marker.SnapshotPath, pbtImportRerunCommand(dirs.DataDir, chainName, marker.SnapshotPath))
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
			if err := validatePBTImportRecoveryFiles(dirs, marker); err != nil {
				if cleanupErr := recoverPBTImportSettings(dirs, settings, marker); cleanupErr != nil {
					return fmt.Errorf("%w; partial import cleanup failed: %w", err, cleanupErr)
				}
				return fmt.Errorf("%w; rerun %s", err, pbtImportRerunCommand(dirs.DataDir, chainName, snapshotPath))
			}
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
	statecfg.ConfigureCommitmentV3Records(true)
	requestedHash := statecfg.BinCommitmentHash
	if requestedHash == "" {
		requestedHash = meta.HashSuite
	}
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
	targetChainName, err := validatePBTImportTarget(ctx, rawDB, blockReader, blockView, meta, chainName, dataDir)
	if err != nil {
		return err
	}
	hexBlock, hexTx, err := readPBTImportHexCheckpoint(ctx, dirs, rawDB, settings, logger)
	if err != nil {
		return err
	}
	if hexBlock != meta.Block || hexTx != meta.TxNum {
		return fmt.Errorf("commitment import-pbt: hex commitment checkpoint is (%d, %d), want (%d, %d) at block %d; run integration stage_exec --datadir=%s --block=%d --chain=%s --experimental.commitment-v3, then erigon snapshots export-pbt --datadir=%s --chain=%s --out=<export-dir> --experimental.bin-commitment.hash=%s", hexBlock, hexTx, meta.Block, meta.TxNum, meta.Block, dataDir, meta.Block+1, targetChainName, dataDir, targetChainName, meta.HashSuite)
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
	statecfg.ConfigureCommitmentV3Records(true)
	statecfg.BinCommitmentHash = meta.HashSuite
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
	targetAgg, err := openPBTState(ctx, stageDirs, finalSettings, stageRaw, logger)
	if err != nil {
		return err
	}
	defer targetAgg.Close()
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
	if err != nil {
		domains.Close()
		return err
	}
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
	wantRoot := common.HexToHash(meta.PBTRoot)
	if root != artifactRoot || root != wantRoot {
		return fmt.Errorf("commitment import-pbt: written root %s differs from artifact root %s or metadata root %s", root, artifactRoot, wantRoot)
	}
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
	if swapHook != nil {
		if err := swapHook("staging-built"); err != nil {
			return err
		}
	}
	stagedFiles, err := pbtImportBinFileNames(stageDirs)
	if err != nil {
		return err
	}
	if err := dbstate.WritePBTImportMarker(dirs, &dbstate.PBTImportMarker{SnapshotPath: absSnapshotPath, SnapshotHash: meta.SnapshotDigest, Files: stagedFiles, Settings: finalSettings, PreviousSettings: settings}); err != nil {
		return err
	}
	if marker != nil {
		if err := removePBTImportFilesForRecovery(dirs); err != nil {
			return err
		}
	}
	moved, err := movePBTImportBinFiles(stageDirs, dirs)
	if err != nil {
		if cleanupErr := removePBTImportFiles(moved); cleanupErr != nil {
			return fmt.Errorf("%w; cleanup failed: %w", err, cleanupErr)
		}
		if markerErr := dbstate.RemovePBTImportMarker(dirs); markerErr != nil {
			return fmt.Errorf("%w; remove marker: %w", err, markerErr)
		}
		return err
	}
	settingsWritten := false
	checkpointWritten := false
	defer func() {
		if recovered := recover(); recovered != nil {
			panic(recovered)
		}
		if !settingsWritten {
			cleanupErr := removePBTImportFiles(moved)
			if checkpointWritten {
				_ = removePBTImportCheckpoint(ctx, dirs, common.HexToHash(meta.BlockHash), meta.Block)
			}
			if cleanupErr == nil {
				_ = dbstate.RemovePBTImportMarker(dirs)
			} else {
				retErr = fmt.Errorf("%w; cleanup failed: %w", retErr, cleanupErr)
			}
		}
	}()
	if swapHook != nil {
		if err := swapHook("files-moved"); err != nil {
			return err
		}
	}
	if err := writePBTImportCheckpoint(ctx, dirs, finalSettings, checkpointState, !checkpointInFiles, meta.TxNum, common.HexToHash(meta.BlockHash), meta.Block, root, logger); err != nil {
		return err
	}
	checkpointWritten = true
	if swapHook != nil {
		if err := swapHook("before-settings"); err != nil {
			return err
		}
	}
	if err := dbstate.WriteErigonDBSettings(dirs, finalSettings); err != nil {
		return err
	}
	settingsWritten = true
	if swapHook != nil {
		if err := swapHook("settings-written"); err != nil {
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
	agg, err := openPBTState(ctx, dirs, settings, rawDB, logger)
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
	if _, err := dbstate.VerifyPBinDomainRoot(ctx, tx, agg, kv.CommitmentBinDomain); err != nil {
		return fmt.Errorf("commitment import-pbt: verify written rows: %w", err)
	}
	return nil
}

func removePBTImportCheckpoint(ctx context.Context, dirs datadir.Dirs, blockHash common.Hash, blockNum uint64) error {
	rawDB, err := dbCfg(dbcfg.ChainDB, dirs.Chaindata).Open(ctx)
	if err != nil {
		return err
	}
	defer rawDB.Close()
	tx, err := rawDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.Delete(kv.TblCommitmentBinVals, commitment.KeyCommitmentState); err != nil {
		return err
	}
	if err := tx.Delete(kv.ShadowStateRoot, dbutils.BlockBodyKey(blockNum, blockHash)); err != nil {
		return err
	}
	return tx.Commit()
}

func writePBTImportCheckpoint(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, state []byte, writeState bool, txNum uint64, blockHash common.Hash, blockNum uint64, root common.Hash, logger log.Logger) error {
	rawDB, err := dbCfg(dbcfg.ChainDB, dirs.Chaindata).Open(ctx)
	if err != nil {
		return fmt.Errorf("commitment import-pbt: open target for checkpoint: %w", err)
	}
	defer rawDB.Close()
	agg, err := openPBTState(ctx, dirs, settings, rawDB, logger)
	if err != nil {
		return err
	}
	defer agg.Close()
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
	block, tx, _, found, err := readPBTCommitmentState(ctx, dirs, rawDB, settings, logger)
	if err != nil {
		return 0, 0, err
	}
	if !found {
		return 0, 0, errors.New("commitment import-pbt: hex commitment checkpoint is missing from files")
	}
	return block, tx, err
}

func pbtImportProgressError(progress uint64, meta pbtImportMeta, dataDir, chainName string) error {
	return fmt.Errorf("commitment import-pbt: target is at block %d, snapshot is at block %d; run integration stage_exec --datadir=%s --block=%d --chain=%s --experimental.commitment-v3, then erigon snapshots export-pbt --datadir=%s --chain=%s --out=<export-dir> --experimental.bin-commitment.hash=%s", progress, meta.Block, dataDir, progress, chainName, dataDir, chainName, meta.HashSuite)
}

func validatePBTImportTarget(ctx context.Context, rawDB kv.RoDB, blockReader *freezeblocks.BlockReader, blockView *blocksnapshots.View, meta pbtImportMeta, chainName, dataDir string) (string, error) {
	readTx, err := rawDB.BeginRo(ctx)
	if err != nil {
		return "", err
	}
	defer readTx.Rollback()
	readerTx := pbtBlockFilesTx{Tx: readTx, view: blockView}
	header, err := blockReader.HeaderByHash(ctx, readerTx, common.HexToHash(meta.BlockHash))
	if err != nil {
		return "", err
	}
	if header == nil {
		return "", fmt.Errorf("commitment import-pbt: block %s is not local or canonical", meta.BlockHash)
	}
	if header.Number.Uint64() != meta.Block {
		return "", fmt.Errorf("commitment import-pbt: snapshot block %d has header number %d", meta.Block, header.Number.Uint64())
	}
	canonical, found, err := blockReader.CanonicalHash(ctx, readerTx, meta.Block)
	if err != nil {
		return "", err
	}
	if !found || canonical != header.Hash() {
		return "", fmt.Errorf("commitment import-pbt: block %d is not canonical with hash %s", meta.Block, meta.BlockHash)
	}
	genesisHash, err := rawdb.ReadCanonicalHash(readTx, 0)
	if err != nil {
		return "", err
	}
	chainConfig, err := rawdb.ReadChainConfig(readTx, genesisHash)
	if err != nil {
		return "", err
	}
	if chainConfig == nil {
		return "", errors.New("commitment import-pbt: chain config is missing")
	}
	if chainName != "" && chainConfig.ChainName != chainName {
		return "", fmt.Errorf("commitment import-pbt: chain %q does not match target chain %q", chainName, chainConfig.ChainName)
	}
	if chainConfig.ChainID == nil || chainConfig.ChainID.String() != meta.ChainID {
		return "", fmt.Errorf("commitment import-pbt: chain id %q does not match target", meta.ChainID)
	}
	progress, err := stages.GetStageProgress(readTx, stages.Execution)
	if err != nil {
		return "", err
	}
	if progress != meta.Block {
		return "", pbtImportProgressError(progress, meta, dataDir, chainConfig.ChainName)
	}
	lastTx, found, err := blockReader.TxnumReader().MaxExact(ctx, readerTx, meta.Block)
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("commitment import-pbt: block %d has no txNum mapping", meta.Block)
	}
	if lastTx != meta.TxNum {
		return "", fmt.Errorf("commitment import-pbt: snapshot checkpoint (%d, %d) is not the block end; target block %d ends at txNum %d; run integration stage_exec --datadir=%s --block=%d --chain=%s --experimental.commitment-v3, then erigon snapshots export-pbt --datadir=%s --chain=%s --out=<export-dir> --experimental.bin-commitment.hash=%s", meta.Block, meta.TxNum, meta.Block, lastTx, dataDir, meta.Block+1, chainConfig.ChainName, dataDir, chainConfig.ChainName, meta.HashSuite)
	}
	if common.HexToHash(meta.StateRoot) != header.Root {
		return "", fmt.Errorf("commitment import-pbt: snapshot stateRoot %s differs from header root %s", meta.StateRoot, header.Root)
	}
	return chainConfig.ChainName, nil
}

func movePBTImportBinFiles(stageDirs, targetDirs datadir.Dirs) ([]string, error) {
	files, err := pbtAttachFiles(stageDirs)
	if err != nil {
		return nil, err
	}
	moved := make([]string, 0)
	destinationDirs := make(map[string]struct{})
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
		destinationDirs[filepath.Dir(destination)] = struct{}{}
	}
	if len(moved) == 0 {
		return moved, errors.New("commitment import-pbt: staged binary commitment files are missing")
	}
	for destinationDir := range destinationDirs {
		if err := dir.FsyncDir(destinationDir); err != nil {
			return moved, err
		}
	}
	return moved, nil
}

func pbtImportBinFileNames(dirs datadir.Dirs) ([]string, error) {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0)
	for _, file := range files {
		if file.domain == kv.CommitmentBinDomain {
			rel, err := filepath.Rel(dirs.Snap, file.path)
			if err != nil {
				return nil, err
			}
			names = append(names, rel)
		}
	}
	if len(names) == 0 {
		return nil, errors.New("commitment import-pbt: staged binary commitment files are missing")
	}
	slices.Sort(names)
	return names, nil
}

func validatePBTImportRecoveryFiles(dirs datadir.Dirs, marker *dbstate.PBTImportMarker) error {
	if marker == nil || len(marker.Files) == 0 {
		return errors.New("commitment import-pbt: recovery marker has no file list; rerun import-pbt")
	}
	for _, name := range marker.Files {
		if _, err := os.Stat(filepath.Join(dirs.Snap, name)); err != nil {
			return fmt.Errorf("commitment import-pbt: recovery file %s is missing; rerun import-pbt", name)
		}
	}
	return nil
}

func recoverPBTImportSettings(dirs datadir.Dirs, current *dbstate.ErigonDBSettings, marker *dbstate.PBTImportMarker) error {
	if err := removePBTImportFilesForRecovery(dirs); err != nil {
		return err
	}
	var previous *dbstate.ErigonDBSettings
	if marker != nil && marker.PreviousSettings != nil {
		previous = marker.PreviousSettings
	} else {
		variant := dbstate.TrieVariantHex
		previous = &dbstate.ErigonDBSettings{
			StepSize:                       current.StepSize,
			StepsInFrozenFile:              current.StepsInFrozenFile,
			ReferencesInCommitmentBranches: current.ReferencesInCommitmentBranches,
			FrozenAtTxNum:                  current.FrozenAtTxNum,
			TrieVariant:                    &variant,
		}
	}
	if err := dbstate.WriteErigonDBSettings(dirs, previous); err != nil {
		return err
	}
	return dbstate.RemovePBTImportMarker(dirs)
}

func removePBTImportFiles(files []string) error {
	directories := make(map[string]struct{})
	for _, file := range files {
		if err := dir.RemoveFile(file); err != nil {
			return err
		}
		directories[filepath.Dir(file)] = struct{}{}
	}
	for directory := range directories {
		if err := dir.FsyncDir(directory); err != nil {
			return err
		}
	}
	return nil
}

func removePBTImportFilesForRecovery(dirs datadir.Dirs) error {
	return removePBTFiles(dirs, func(file pbtAttachFile) bool { return file.domain == kv.CommitmentBinDomain })
}
