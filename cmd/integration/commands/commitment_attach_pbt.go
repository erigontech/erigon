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
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/ethconfig"
)

var (
	attachPBTFrom       string
	attachPBTSkipVerify bool
	attachPBTWorkers    int
)

type pbtAttachHooks struct {
	step       func(string) error
	genesis    func(context.Context, datadir.Dirs, datadir.Dirs, log.Logger) error
	hexRoot    func(context.Context, datadir.Dirs, *dbstate.ErigonDBSettings, uint64, uint64, log.Logger) (common.Hash, bool, error)
	afterCopy  func() error
	skipVerify bool
	workers    int
}

func init() {
	withChain(cmdCommitmentAttachPBT)
	withDataDir(cmdCommitmentAttachPBT)
	withConfig(cmdCommitmentAttachPBT)
	withExperimentalCommitment(cmdCommitmentAttachPBT)
	cmdCommitmentAttachPBT.Flags().StringVar(&attachPBTFrom, "from", "", "published datadir containing converted commitment files")
	cmdCommitmentAttachPBT.Flags().IntVar(&attachPBTWorkers, "workers", dbg.PBTWorkers, "manifest digest workers")
	cmdCommitmentAttachPBT.Flags().BoolVar(&attachPBTSkipVerify, "skip-verify", false, "skip only the manifest digest passes")
	must(cmdCommitmentAttachPBT.MarkFlagDirname("from"))
	must(cmdCommitmentAttachPBT.MarkFlagRequired("from"))
	commitmentCmd.AddCommand(cmdCommitmentAttachPBT)
}

var cmdCommitmentAttachPBT = &cobra.Command{
	Use:          "attach-pbt",
	Short:        "attach published binary commitment files",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger, ctx := debug.SetupCobra(cmd, "integration"), cmd.Context()
		return attachPBTWithHooks(ctx, datadirCli, attachPBTFrom, chain, logger, pbtAttachHooks{skipVerify: attachPBTSkipVerify, workers: attachPBTWorkers})
	},
}

var pbtAttachDomains = []kv.Domain{
	kv.AccountsDomain,
	kv.StorageDomain,
	kv.CodeDomain,
	kv.CommitmentDomain,
	kv.CommitmentBinDomain,
}

type pbtAttachFile struct {
	path   string
	domain kv.Domain
	from   uint64
	to     uint64
	data   bool
}

func attachPBT(ctx context.Context, nodePath, publishedPath, chainName string, logger log.Logger) error {
	return attachPBTWithHooks(ctx, nodePath, publishedPath, chainName, logger, pbtAttachHooks{})
}

func attachPBTWithHooks(ctx context.Context, nodePath, publishedPath, chainName string, logger log.Logger, hooks pbtAttachHooks) error {
	if hooks.step == nil {
		hooks.step = func(string) error { return nil }
	}
	if hooks.genesis == nil {
		hooks.genesis = validatePBTAttachGenesis
	}
	if hooks.hexRoot == nil {
		hooks.hexRoot = pbtAttachHexRoot
	}
	if hooks.workers <= 0 {
		hooks.workers = dbg.PBTWorkers
	}
	hooks.workers = max(hooks.workers, 1)
	if nodePath == "" || publishedPath == "" {
		return errors.New("commitment attach-pbt: node and published datadirs are required")
	}
	nodeDirs := datadir.Open(nodePath)
	publishedDirs := datadir.Open(publishedPath)
	if err := checkPBTChainName(ctx, nodeDirs, chainName, "commitment attach-pbt", "node"); err != nil {
		return err
	}
	marker, err := dbstate.ReadPBTAttachMarker(nodeDirs)
	if err != nil {
		return fmt.Errorf("%w; remove only %s and rerun attach-pbt --from %s", err, dbstate.PBTAttachMarkerPath(nodeDirs), publishedPath)
	}
	legacyMarker := marker != nil && marker.NodeHexRoot == "" && marker.HeaderRoot == ""
	nodeV3 := false
	if marker == nil {
		if nodeV3, err = dbstate.EnableCommitmentV3FromFiles(nodeDirs); err != nil {
			return err
		}
	}
	if _, err := dbstate.EnableCommitmentV3FromFiles(publishedDirs); err != nil {
		return err
	}
	absolutePublishedPath, err := filepath.Abs(publishedDirs.DataDir)
	if err != nil {
		return err
	}
	if marker != nil && filepath.Clean(marker.PublishedPath) != filepath.Clean(absolutePublishedPath) {
		return fmt.Errorf("commitment attach-pbt is incomplete for %s; rerun attach-pbt --from %s", marker.PublishedPath, marker.PublishedPath)
	}
	if nested, err := pathsOverlap(nodeDirs.DataDir, publishedDirs.DataDir); err != nil {
		return err
	} else if nested {
		return fmt.Errorf("commitment attach-pbt: published datadir %s overlaps node datadir %s", publishedDirs.DataDir, nodeDirs.DataDir)
	}
	publishedSettings, err := dbstate.ReadErigonDBSettings(publishedDirs)
	if errors.Is(err, fs.ErrNotExist) {
		return errors.New("commitment attach-pbt: published datadir has no erigondb.toml")
	}
	if err != nil {
		return err
	}
	var nodeSettings *dbstate.ErigonDBSettings
	if marker != nil {
		nodeSettings = marker.Settings
	} else {
		nodeSettings, err = dbstate.ReadErigonDBSettings(nodeDirs)
		if errors.Is(err, fs.ErrNotExist) {
			stepSize, stepErr := dbstate.ResolveErigonDBStepSize(nodeDirs)
			if stepErr != nil {
				return stepErr
			}
			nodeSettings = &dbstate.ErigonDBSettings{StepSize: stepSize}
			err = nil
		}
		if err != nil {
			return err
		}
	}
	blockNum, txNum, ok, err := publishedSettings.ConversionPoint()
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("commitment attach-pbt: published datadir has no conversion point")
	}
	if publishedSettings.TrieVariantName() != dbstate.TrieVariantHexBin {
		return errors.New("commitment attach-pbt: published datadir must contain hex+bin commitment files")
	}
	if publishedSettings.TrieHashName() != configuredPBTNodeHash(nodeSettings) {
		return fmt.Errorf("commitment attach-pbt: trie_hash %q differs from the node suite %q", publishedSettings.TrieHashName(), configuredPBTNodeHash(nodeSettings))
	}
	if nodeSettings.StepSize != publishedSettings.StepSize {
		return fmt.Errorf("commitment attach-pbt: step size %d differs from the node step size %d", publishedSettings.StepSize, nodeSettings.StepSize)
	}
	if err := validatePBTAttachSalts(nodeDirs, publishedDirs); err != nil {
		return err
	}
	manifestPath := filepath.Join(publishedDirs.Snap, "pbt-manifest.toml")
	manifest, manifestErr := dbstate.ReadPBTManifest(manifestPath)
	manifestAvailable := true
	if errors.Is(manifestErr, fs.ErrNotExist) {
		manifestAvailable = false
	} else if manifestErr != nil {
		return fmt.Errorf("commitment attach-pbt: read pbt-manifest.toml: %w", manifestErr)
	}
	if manifestAvailable {
		sources, sourceErr := pbtAttachManifestSources(publishedDirs)
		if sourceErr != nil {
			return fmt.Errorf("commitment attach-pbt: manifest files: %w", sourceErr)
		}
		if fileSetErr := dbstate.ValidatePBTManifestFileSet(manifest, sources); fileSetErr != nil {
			return fileSetErr
		}
	}
	if marker == nil {
		nodeFiles, fileErr := pbtAttachFiles(nodeDirs)
		if fileErr != nil {
			return fileErr
		}
		if firstStep, ok := pbtAttachFirstStepPastPoint(pbtAttachVisibleFiles(nodeFiles), publishedSettings.StepSize, txNum); ok {
			progress, progressErr := pbtAttachExecutionProgress(ctx, nodeDirs)
			if progressErr != nil {
				return progressErr
			}
			return pbtAttachFilesAheadError(nodeDirs.DataDir, chainName, firstStep, publishedSettings.StepSize, progress, blockNum, txNum)
		}
		if err := validatePBTAttachFiles(nodeDirs, publishedDirs, publishedSettings.StepSize, txNum); err != nil {
			return err
		}
	} else if err := validatePBTAttachPublishedFiles(publishedDirs, publishedSettings.StepSize, txNum); err != nil {
		return err
	}
	var nodeBlock, nodeTx, nodeProgress uint64
	var nodeHexRoot common.Hash
	if marker == nil {
		nodeBlock, nodeTx, nodeHexRoot, err = pbtAttachNodeHexState(ctx, nodeDirs, nodeSettings, nodeV3, logger)
		if err != nil {
			return err
		}
		nodeProgress, err = pbtAttachExecutionProgress(ctx, nodeDirs)
		if err != nil {
			return err
		}
		if nodeBlock != blockNum || nodeTx != txNum {
			_, _, blockEnd, _, _, err := pbtAttachBlockEnd(ctx, nodeDirs, blockNum, txNum)
			if err != nil {
				blockEnd = true
			}
			return pbtAttachNodePointError(nodeDirs.DataDir, chainName, nodeBlock, nodeTx, nodeProgress, blockNum, txNum, blockEnd)
		}
	} else {
		if legacyMarker {
			nodeBlock, nodeTx, nodeHexRoot, err = pbtAttachNodeHexState(ctx, nodeDirs, nodeSettings, false, logger)
			if err != nil {
				return fmt.Errorf("commitment attach-pbt: legacy attach marker has no roots; restore the node before retry: %w", err)
			}
		}
		if legacyMarker && (nodeBlock != blockNum || nodeTx != txNum) {
			return fmt.Errorf("commitment attach-pbt: marker conversion point does not match published point (%d, %d)", blockNum, txNum)
		}
	}
	if !manifestAvailable {
		return errors.New("commitment attach-pbt: pbt-manifest.toml is missing; re-run convert-pbt with this binary")
	}
	manifestDigest, err := dbstate.PBTManifestDigest(manifestPath)
	if err != nil {
		return fmt.Errorf("commitment attach-pbt: manifest digest: %w", err)
	}
	if marker != nil {
		if marker.ManifestBlake3 == "" {
			return fmt.Errorf("commitment attach-pbt: marker has no manifest digest; remove only %s and rerun attach-pbt --from %s", dbstate.PBTAttachMarkerPath(nodeDirs), publishedPath)
		}
		if marker.ManifestBlake3 != manifestDigest {
			return fmt.Errorf("commitment attach-pbt: published manifest digest %s differs from marker digest %s", manifestDigest, marker.ManifestBlake3)
		}
		if !legacyMarker {
			nodeHexRoot, err = parsePBTAttachHash("marker node hex root", marker.NodeHexRoot)
			if err != nil {
				return err
			}
		}
	}
	blockHash, headerRoot, blockEnd, afterFork, maxTxNum, err := pbtAttachBlockEnd(ctx, nodeDirs, blockNum, txNum)
	if err != nil {
		return err
	}
	if marker != nil {
		if !legacyMarker {
			markerHeaderRoot, markerErr := parsePBTAttachHash("marker header root", marker.HeaderRoot)
			if markerErr != nil {
				return markerErr
			}
			if headerRoot != markerHeaderRoot {
				return fmt.Errorf("commitment attach-pbt: current header root %s differs from marker root %s", headerRoot, markerHeaderRoot)
			}
		}
	}
	if marker == nil {
		if nodeProgress > blockNum {
			return pbtAttachNodePointError(nodeDirs.DataDir, chainName, nodeBlock, nodeTx, nodeProgress, blockNum, txNum, blockEnd)
		}
		if nodeProgress == blockNum && maxTxNum < txNum {
			return fmt.Errorf("commitment attach-pbt: node is behind conversion txNum %d", txNum)
		}
	}
	manifestPbtRoot, err := parsePBTManifestHash("pbt_root", manifest.PBTRoot)
	if err != nil {
		return err
	}
	manifestHexRoot, err := parsePBTManifestHash("hex_root", manifest.HexRoot)
	if err != nil {
		return err
	}
	if manifest.Point.Block != blockNum || manifest.Point.TxNum != txNum {
		return fmt.Errorf("commitment attach-pbt: manifest point is (%d, %d), want (%d, %d)", manifest.Point.Block, manifest.Point.TxNum, blockNum, txNum)
	}
	if marker != nil {
		markerBlock, markerTx, markerPoint, markerErr := marker.Settings.ConversionPoint()
		if markerErr != nil {
			return markerErr
		}
		if !markerPoint || markerBlock != blockNum || markerTx != txNum {
			return fmt.Errorf("commitment attach-pbt: marker conversion point does not match published point (%d, %d)", blockNum, txNum)
		}
	}
	if manifest.TrieHash != publishedSettings.TrieHashName() {
		return fmt.Errorf("commitment attach-pbt: manifest trie_hash %q differs from published settings %q", manifest.TrieHash, publishedSettings.TrieHashName())
	}
	if err := validatePBTAttachManifestFiles(ctx, manifest, publishedDirs, hooks.workers, hooks.skipVerify, logger, "attach published manifest digest"); err != nil {
		return err
	}
	if err := validatePBTAttachPublishedPoint(ctx, publishedDirs, publishedSettings, blockNum, txNum, logger); err != nil {
		return err
	}
	if err := verifyPBTAttachPublishedBin(ctx, publishedDirs, publishedSettings, pbinConversionPoint{BlockNum: blockNum, TxNum: txNum}, manifestPbtRoot, logger); err != nil {
		return err
	}
	publishedHexRoot, publishedHexFound, err := hooks.hexRoot(ctx, publishedDirs, publishedSettings, blockNum, txNum, logger)
	if err != nil {
		return err
	}
	if !publishedHexFound {
		return fmt.Errorf("commitment attach-pbt: published hex root is missing at (%d, %d)", blockNum, txNum)
	}
	if publishedHexRoot != manifestHexRoot {
		return fmt.Errorf("commitment attach-pbt: published hex root %s differs from manifest root %s at (%d, %d)", publishedHexRoot, manifestHexRoot, blockNum, txNum)
	}
	if nodeHexRoot != manifestHexRoot {
		return fmt.Errorf("commitment attach-pbt: node hex root %s differs from manifest root %s at (%d, %d)", nodeHexRoot, manifestHexRoot, blockNum, txNum)
	}
	if blockEnd {
		wantHeaderRoot := manifestHexRoot
		if afterFork {
			wantHeaderRoot = manifestPbtRoot
		}
		if headerRoot != wantHeaderRoot {
			return fmt.Errorf("commitment attach-pbt: published root %s differs from header root %s at block %d", wantHeaderRoot, headerRoot, blockNum)
		}
	}
	if marker == nil {
		if err := hooks.genesis(ctx, nodeDirs, publishedDirs, logger); err != nil {
			return err
		}
	}
	refs := publishedSettings.RefsInCommitmentBranches()
	variant := dbstate.TrieVariantHexBin
	hash := publishedSettings.TrieHashName()
	finalSettings := &dbstate.ErigonDBSettings{
		StepSize:                       publishedSettings.StepSize,
		StepsInFrozenFile:              publishedSettings.StepsInFrozenFile,
		ReferencesInCommitmentBranches: &refs,
		FrozenAtTxNum:                  publishedSettings.FrozenAtTxNum,
		TrieVariant:                    &variant,
		TrieHash:                       &hash,
		ConversionBlockNum:             &blockNum,
		ConversionTxNum:                &txNum,
	}
	if marker != nil {
		finalSettings = marker.Settings
		if legacyMarker {
			marker.NodeHexRoot = nodeHexRoot.Hex()
			marker.HeaderRoot = headerRoot.Hex()
			if err := dbstate.WritePBTAttachMarker(nodeDirs, marker); err != nil {
				return err
			}
		}
	} else if err := dbstate.WritePBTAttachMarker(nodeDirs, &dbstate.PBTAttachMarker{PublishedPath: absolutePublishedPath, ManifestBlake3: manifestDigest, NodeHexRoot: nodeHexRoot.Hex(), HeaderRoot: headerRoot.Hex(), Settings: finalSettings}); err != nil {
		return err
	}
	if err := hooks.step("marker"); err != nil {
		return err
	}
	if err := adoptPBTFiles(nodeDirs, publishedDirs, publishedSettings.StepSize, txNum); err != nil {
		return err
	}
	if hooks.afterCopy != nil {
		if err := hooks.afterCopy(); err != nil {
			return err
		}
	}
	if err := validatePBTAttachManifestFiles(ctx, manifest, nodeDirs, hooks.workers, hooks.skipVerify, logger, "attach destination manifest digest"); err != nil {
		cleanupErr := removePBTAdoptedManifestFiles(nodeDirs)
		if cleanupErr != nil {
			return errors.Join(err, cleanupErr)
		}
		return err
	}
	if err := hooks.step("swap"); err != nil {
		return err
	}
	if err := resetPBTExecution(ctx, nodeDirs, publishedSettings, logger); err != nil {
		return err
	}
	if err := hooks.step("reset"); err != nil {
		return err
	}
	if blockEnd {
		shadowRoot := manifestPbtRoot
		if afterFork {
			shadowRoot = publishedHexRoot
		}
		if err := writePBTAttachShadowRoot(ctx, nodeDirs, blockHash, blockNum, shadowRoot); err != nil {
			return err
		}
	}
	if err := dbstate.WriteErigonDBSettings(nodeDirs, finalSettings); err != nil {
		return err
	}
	if err := hooks.step("settings"); err != nil {
		return err
	}
	return dbstate.RemovePBTAttachMarker(nodeDirs)
}

func configurePBTNodeVariant(settings *dbstate.ErigonDBSettings, v3 bool) {
	if v3 && settings.TrieVariantName() == dbstate.TrieVariantHex {
		statecfg.ConfigureCommitmentV3Records(true)
		statecfg.ExperimentalBinCommitment = false
		statecfg.ExperimentalHexBinCommitment = false
		statecfg.BinCommitmentHash = ""
		return
	}
	configurePBTSourceVariant(settings)
}

func parsePBTManifestHash(name, value string) (common.Hash, error) {
	return parsePBTAttachHash("manifest "+name, value)
}

func parsePBTAttachHash(name, value string) (common.Hash, error) {
	var hash common.Hash
	if err := hash.UnmarshalText([]byte(value)); err != nil {
		return common.Hash{}, fmt.Errorf("commitment attach-pbt: %s is not a 32-byte hash: %w", name, err)
	}
	return hash, nil
}

func pbtAttachManifestSources(dirs datadir.Dirs) ([]dbstate.PBTManifestFileSource, error) {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return nil, err
	}
	visible := pbtAttachVisibleFiles(files)
	sources := make([]dbstate.PBTManifestFileSource, 0, len(visible))
	for _, file := range visible {
		if !pbtAttachAdoptsFile(file) {
			continue
		}
		rel, err := filepath.Rel(dirs.Snap, file.path)
		if err != nil {
			return nil, err
		}
		sources = append(sources, dbstate.PBTManifestFileSource{Name: filepath.Clean(rel), Path: file.path})
	}
	return sources, nil
}

func validatePBTAttachManifestFiles(ctx context.Context, manifest dbstate.PBTManifest, dirs datadir.Dirs, workers int, skipVerify bool, logger log.Logger, phase string) error {
	sources, err := pbtAttachManifestSources(dirs)
	if err != nil {
		return fmt.Errorf("commitment attach-pbt: manifest files: %w", err)
	}
	if err := dbstate.ValidatePBTManifestFileSet(manifest, sources); err != nil {
		return err
	}
	if skipVerify {
		logger.Warn("commitment attach-pbt: manifest digest verification skipped", "phase", phase)
		return nil
	}
	progress := dbstate.NewPBTProgress(dbstate.PBTProgressConfig{
		Logger:  logger,
		Message: "PBT manifest progress",
		Phase:   phase,
		Total:   uint64(len(sources)),
		TempDir: dirs.Tmp,
		Fields:  []any{"bytes_done", uint64(0)},
	})
	var bytesDone atomic.Uint64
	err = dbstate.ValidatePBTManifestFiles(ctx, manifest, sources, workers, func(done, _, bytes uint64) {
		progress.SetFields("bytes_done", bytesDone.Add(bytes))
		progress.Add(1)
		_ = done
	})
	progress.Done()
	return err
}

func removePBTAdoptedManifestFiles(dirs datadir.Dirs) error {
	if err := removePBTFiles(dirs, pbtAttachAdoptsFile); err != nil {
		return err
	}
	path := filepath.Join(dirs.Snap, "pbt-manifest.toml")
	if err := removePBTPath(path); err != nil {
		return err
	}
	return fsyncPBTDirs(map[string]struct{}{dirs.Snap: {}})
}

func pbtAttachBlockEnd(ctx context.Context, dirs datadir.Dirs, blockNum, txNum uint64) (common.Hash, common.Hash, bool, bool, uint64, error) {
	db, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return common.Hash{}, common.Hash{}, false, false, 0, err
	}
	defer db.Close()
	blockReader, blockView, closeBlockReader, err := openPBTBlockReader(ctx, dirs, db, log.Root())
	if err != nil {
		return common.Hash{}, common.Hash{}, false, false, 0, err
	}
	defer closeBlockReader()
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, common.Hash{}, false, false, 0, err
	}
	defer tx.Rollback()
	blockTx := pbtBlockFilesTx{Tx: tx, view: blockView}
	maxTxNum, found, err := blockReader.TxnumReader().MaxExact(ctx, blockTx, nil, blockNum)
	if err != nil {
		return common.Hash{}, common.Hash{}, false, false, 0, err
	}
	if !found {
		return common.Hash{}, common.Hash{}, false, false, 0, fmt.Errorf("commitment attach-pbt: block %d has no txNum mapping", blockNum)
	}
	header, err := blockReader.HeaderByNumber(ctx, blockTx, blockNum)
	if err != nil {
		return common.Hash{}, common.Hash{}, false, false, 0, err
	}
	if header == nil {
		return common.Hash{}, common.Hash{}, false, false, maxTxNum, nil
	}
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return common.Hash{}, common.Hash{}, false, false, 0, err
	}
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return common.Hash{}, common.Hash{}, false, false, 0, err
	}
	afterFork := chainConfig != nil && chainConfig.IsBinaryTrie(header.Time)
	return header.Hash(), header.Root, maxTxNum == txNum, afterFork, maxTxNum, nil
}

func writePBTAttachShadowRoot(ctx context.Context, dirs datadir.Dirs, blockHash common.Hash, blockNum uint64, root common.Hash) error {
	db, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), false)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteShadowStateRoot(tx, blockHash, blockNum, root[:])
	})
}

func pbtAttachHexRoot(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, blockNum, txNum uint64, logger log.Logger) (common.Hash, bool, error) {
	configurePBTSourceVariant(settings)
	agg, err := openPBTState(ctx, dirs, settings, nil, logger)
	if err != nil {
		return common.Hash{}, false, err
	}
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	value, found, _, _, err := at.DebugGetLatestFromFiles(kv.CommitmentDomain, commitment.KeyCommitmentV3State, math.MaxUint64)
	if err != nil || !found {
		return common.Hash{}, found, err
	}
	gotBlock, gotTx, root, err := commitment.DecodeCommitmentV3State(value)
	if err != nil {
		return common.Hash{}, false, err
	}
	if gotBlock != blockNum || gotTx != txNum {
		return common.Hash{}, false, fmt.Errorf("commitment attach-pbt: published hex state is (%d, %d), want (%d, %d)", gotBlock, gotTx, blockNum, txNum)
	}
	return common.BytesToHash(root), true, nil
}

func pbtAttachNodeHexState(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, v3 bool, logger log.Logger) (uint64, uint64, common.Hash, error) {
	db, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return 0, 0, common.Hash{}, err
	}
	defer db.Close()
	configurePBTNodeVariant(settings, v3)
	block, tx, root, found, err := readPBTCommitmentState(ctx, dirs, db, settings, logger)
	if err != nil {
		return 0, 0, common.Hash{}, err
	}
	if !found {
		return 0, 0, common.Hash{}, fmt.Errorf("commitment attach-pbt: node hex state is missing")
	}
	return block, tx, root, nil
}

func readPBTCommitmentState(ctx context.Context, dirs datadir.Dirs, rawDB kv.RwDB, settings *dbstate.ErigonDBSettings, logger log.Logger) (uint64, uint64, common.Hash, bool, error) {
	agg, err := openPBTState(ctx, dirs, settings, rawDB, logger)
	if err != nil {
		return 0, 0, common.Hash{}, false, err
	}
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	roTx, err := rawDB.BeginRo(ctx)
	if err != nil {
		return 0, 0, common.Hash{}, false, err
	}
	defer roTx.Rollback()
	value, _, found, err := at.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentV3State, roTx, kv.GetLatestOptions{})
	if err != nil {
		return 0, 0, common.Hash{}, false, err
	}
	if !found {
		return 0, 0, common.Hash{}, false, nil
	}
	gotBlock, gotTx, root, err := commitment.DecodeCommitmentV3State(value)
	if err != nil {
		return 0, 0, common.Hash{}, false, err
	}
	return gotBlock, gotTx, common.BytesToHash(root), found, nil
}

func pbtAttachNodePointError(dataDir, chainName string, nodeBlock, nodeTx, executionProgress, blockNum, txNum uint64, blockEnd bool) error {
	reset, canRun := pbtAttachStageExecReset(dataDir, chainName)
	if executionProgress < blockNum || nodeBlock < blockNum || (nodeBlock == blockNum && nodeTx < txNum) {
		if !canRun {
			return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, behind conversion point block %d txNum %d; target chain %q cannot be loaded by integration", nodeBlock, nodeTx, executionProgress, blockNum, txNum, chainName)
		}
		return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, behind conversion point block %d txNum %d; run %s", nodeBlock, nodeTx, executionProgress, blockNum, txNum, reset)
	}
	if !blockEnd {
		if !canRun {
			return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, conversion point is mid-block %d txNum %d; target chain %q cannot be loaded by integration", nodeBlock, nodeTx, executionProgress, blockNum, txNum, chainName)
		}
		return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, conversion point is mid-block %d txNum %d; run %s", nodeBlock, nodeTx, executionProgress, blockNum, txNum, reset)
	}
	if executionProgress <= blockNum {
		if !canRun {
			return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, conversion point is block %d txNum %d; target chain %q cannot be loaded by integration", nodeBlock, nodeTx, executionProgress, blockNum, txNum, chainName)
		}
		return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, conversion point is block %d txNum %d; run %s", nodeBlock, nodeTx, executionProgress, blockNum, txNum, reset)
	}
	unwind := executionProgress - blockNum
	if !canRun {
		return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, conversion point is block %d txNum %d; target chain %q cannot be loaded by integration", nodeBlock, nodeTx, executionProgress, blockNum, txNum, chainName)
	}
	return fmt.Errorf("commitment attach-pbt: node checkpoint is block %d txNum %d and execution progress is block %d, conversion point is block %d txNum %d; run %s when the node files end at the point, or run integration stage_exec --datadir=%s --unwind=%d --chain=%s --experimental.commitment-v3 followed by integration stage_exec --datadir=%s --block=%d --chain=%s --experimental.commitment-v3", nodeBlock, nodeTx, executionProgress, blockNum, txNum, reset, dataDir, unwind, chainName, dataDir, blockNum, chainName)
}

func pbtAttachStageExecReset(dataDir, chainName string) (string, bool) {
	if _, err := chainspec.ChainSpecByName(chainName); err != nil {
		return "", false
	}
	return fmt.Sprintf("integration stage_exec --datadir=%s --reset --chain=%s --experimental.commitment-v3", dataDir, chainName), true
}

func pbtAttachFilesAheadError(dataDir, chainName string, firstStep, stepSize, executionProgress, blockNum, txNum uint64) error {
	if firstStep*stepSize <= txNum {
		return fmt.Errorf("commitment attach-pbt: node file range starting at step %d spans conversion txNum %d; stage_exec cannot stop at a mid-block point, so no printed command can reach it", firstStep, txNum)
	}
	reset, canRun := pbtAttachStageExecReset(dataDir, chainName)
	if !canRun {
		return fmt.Errorf("commitment attach-pbt: node files extend past conversion point block %d txNum %d; target chain %q cannot be loaded by integration", blockNum, txNum, chainName)
	}
	return fmt.Errorf("commitment attach-pbt: node files extend past conversion point block %d txNum %d; remove state from step %d onward with erigon snapshots rm-state --datadir=%s --chain=%s --step=%d+ --experimental.commitment-v3, then run %s (execution progress is block %d)", blockNum, txNum, firstStep, dataDir, chainName, firstStep, reset, executionProgress)
}

func validatePBTAttachGenesis(ctx context.Context, nodeDirs, publishedDirs datadir.Dirs, logger log.Logger) error {
	nodeDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, nodeDirs.Chaindata), true)
	if err != nil {
		return err
	}
	defer nodeDB.Close()
	var nodeGenesis common.Hash
	var chainName string
	if err := nodeDB.View(ctx, func(tx kv.Tx) error {
		var err error
		nodeGenesis, err = rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		config, err := rawdb.ReadChainConfig(tx, nodeGenesis)
		if err != nil {
			return err
		}
		if config == nil {
			return errors.New("commitment attach-pbt: chain config is missing")
		}
		chainName = config.ChainName
		return nil
	}); err != nil {
		return err
	}
	cfg := ethconfig.NewSnapCfg(false, true, true, chainName)
	snapshots := blocksnapshots.NewRoSnapshots(cfg, publishedDirs.Snap, logger)
	if err := snapshots.OpenFolder(); err != nil {
		return err
	}
	defer snapshots.Close()
	view := snapshots.View()
	defer view.Close()
	reader := freezeblocks.NewBlockReader(snapshots)
	genesis, err := reader.HeaderFromView(view, 0)
	if err != nil {
		return err
	}
	if genesis == nil {
		return errors.New("commitment attach-pbt: published genesis is missing")
	}
	if genesis.Hash() != nodeGenesis {
		return fmt.Errorf("commitment attach-pbt: genesis hash %s differs from node genesis %s", genesis.Hash(), nodeGenesis)
	}
	return nil
}

func validatePBTAttachPublishedPoint(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, blockNum, txNum uint64, logger log.Logger) error {
	configurePBTSourceVariant(settings)
	if err := eip8297.SetHashSuite(settings.TrieHashName()); err != nil {
		return err
	}
	agg, err := openPBTState(ctx, dirs, settings, nil, logger)
	if err != nil {
		return err
	}
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	publishedFiles, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	publishedFiles = pbtAttachVisibleFiles(publishedFiles)
	opened := make(map[kv.Domain]map[string]struct{})
	for _, domain := range pbtAttachDomains {
		opened[domain] = make(map[string]struct{})
		for _, file := range at.Files(domain) {
			if filepath.Ext(file.Fullpath()) == ".kv" {
				opened[domain][filepath.Clean(file.Fullpath())] = struct{}{}
			}
		}
	}
	for _, domain := range pbtAttachDomains {
		files := at.Files(domain)
		needsOpened := domain == kv.CommitmentDomain || domain == kv.CommitmentBinDomain
		if (needsOpened && len(opened[domain]) == 0) || len(files) == 0 || files[len(files)-1].StartRootNum() > txNum || files[len(files)-1].EndRootNum() <= txNum {
			return fmt.Errorf("commitment attach-pbt: published %s files do not cover conversion txNum %d", domain, txNum)
		}
		for _, file := range publishedFiles {
			if file.domain != domain || !file.data || file.from*settings.StepSize > txNum {
				continue
			}
			if _, ok := opened[domain][filepath.Clean(file.path)]; !ok {
				return fmt.Errorf("commitment attach-pbt: published %s file %s was not opened", domain, file.path)
			}
		}
	}
	checks := []struct {
		domain kv.Domain
		key    []byte
		name   string
		decode func([]byte) (uint64, uint64, error)
	}{
		{domain: kv.CommitmentDomain, key: commitment.KeyCommitmentV3State, name: "commitment", decode: func(value []byte) (uint64, uint64, error) {
			block, tx, _, err := commitment.DecodeCommitmentV3State(value)
			return block, tx, err
		}},
		{domain: kv.CommitmentBinDomain, key: commitment.KeyCommitmentState, name: "commitment-bin", decode: func(value []byte) (uint64, uint64, error) {
			if len(value) < 16 {
				return 0, 0, errors.New("state record is too short")
			}
			tx, block := commitmentdb.DecodeTxBlockNums(value)
			return block, tx, nil
		}},
	}
	for _, check := range checks {
		value, found, start, end, err := at.DebugGetLatestFromFiles(check.domain, check.key, math.MaxUint64)
		if err != nil {
			return err
		}
		if !found || start > txNum || end <= txNum {
			return fmt.Errorf("commitment attach-pbt: published %s state is not at conversion txNum %d", check.name, txNum)
		}
		gotBlock, gotTx, err := check.decode(value)
		if err != nil {
			return fmt.Errorf("commitment attach-pbt: decode published %s state: %w", check.name, err)
		}
		if gotBlock != blockNum || gotTx != txNum {
			return fmt.Errorf("commitment attach-pbt: published %s state is (%d, %d), want (%d, %d)", check.name, gotBlock, gotTx, blockNum, txNum)
		}
	}
	return nil
}

func verifyPBTAttachPublishedBin(ctx context.Context, publishedDirs datadir.Dirs, settings *dbstate.ErigonDBSettings, point pbinConversionPoint, wantRoot common.Hash, logger log.Logger) error {
	progress := dbstate.NewPBTProgress(dbstate.PBTProgressConfig{Logger: logger, Phase: "attach published bin verify", Total: 1, TempDir: publishedDirs.Tmp})
	defer progress.Done()
	configurePBTSourceVariant(settings)
	if err := eip8297.SetHashSuite(settings.TrieHashName()); err != nil {
		return err
	}
	agg, err := openPBTState(ctx, publishedDirs, settings, nil, logger)
	if err != nil {
		return err
	}
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	value, found, start, end, err := at.DebugGetLatestFromFiles(kv.CommitmentBinDomain, pbt.GlobalRootKey(), math.MaxUint64)
	if err != nil {
		return err
	}
	if !found || start > point.TxNum || end < point.TxNum {
		if !found && wantRoot == eip8297.EmptyTreeHash {
			state, stateFound, stateStart, stateEnd, stateErr := at.DebugGetLatestFromFiles(kv.CommitmentBinDomain, commitment.KeyCommitmentState, math.MaxUint64)
			if stateErr != nil {
				return stateErr
			}
			if stateFound && stateStart <= point.TxNum && stateEnd >= point.TxNum {
				empty, emptyErr := pbtStateRootIsEmpty(state)
				if emptyErr != nil {
					return fmt.Errorf("commitment attach-pbt: decode published binary state: %w", emptyErr)
				}
				if empty {
					progress.Add(1)
					return nil
				}
			}
		}
		return fmt.Errorf("commitment attach-pbt: published binary root record is not at conversion txNum %d", point.TxNum)
	}
	record, err := pbt.DecodeRecord(pbt.GlobalRootKey(), value)
	if err != nil {
		return fmt.Errorf("commitment attach-pbt: decode published binary root record: %w", err)
	}
	root, err := pbt.Fold(pbt.GlobalRootKey(), &record)
	if err != nil {
		return fmt.Errorf("commitment attach-pbt: fold published binary root record: %w", err)
	}
	if root != wantRoot {
		return fmt.Errorf("commitment attach-pbt: published binary root record %s differs from manifest root %s", root, wantRoot)
	}
	progress.Add(1)
	return nil
}

func pbtStateRootIsEmpty(value []byte) (bool, error) {
	state := value
	if !commitment.IsPBinState(state) {
		if len(state) < 18 {
			return false, fmt.Errorf("state envelope is %d bytes, want at least 18", len(state))
		}
		stateLen := int(state[16])<<8 | int(state[17])
		if len(state) != 18+stateLen {
			return false, fmt.Errorf("state envelope claims %d bytes, %d present", stateLen, len(state)-18)
		}
		state = state[18:]
	}
	if err := commitment.PBinValidateRowStateFormat(state); err != nil {
		return false, err
	}
	return len(state) == 5, nil
}

func configuredPBTNodeHash(settings *dbstate.ErigonDBSettings) string {
	if settings != nil && (settings.TrieVariantName() == dbstate.TrieVariantBin || settings.TrieVariantName() == dbstate.TrieVariantHexBin) {
		return settings.TrieHashName()
	}
	if statecfg.BinCommitmentHash != "" {
		return statecfg.BinCommitmentHash
	}
	return commitment.PBinHashSuiteName()
}

func validatePBTAttachFiles(nodeDirs, publishedDirs datadir.Dirs, stepSize, endTxNum uint64) error {
	if stepSize == 0 {
		return errors.New("commitment attach-pbt: step size is zero")
	}
	nodeFiles, err := pbtAttachFiles(nodeDirs)
	if err != nil {
		return err
	}
	publishedFiles, err := pbtAttachFiles(publishedDirs)
	if err != nil {
		return err
	}
	nodeFiles = pbtAttachVisibleFiles(nodeFiles)
	publishedFiles = pbtAttachVisibleFiles(publishedFiles)
	if err := validatePBTAttachUncutFiles(nodeFiles, stepSize, endTxNum); err != nil {
		return err
	}
	if err := validatePBTAttachHistoryFrontier(nodeFiles, stepSize, endTxNum); err != nil {
		return err
	}
	if err := validatePBTAttachPublishedFiles(publishedDirs, stepSize, endTxNum); err != nil {
		return err
	}
	nodeRanges := pbtAttachRanges(nodeFiles, stepSize, endTxNum)
	publishedRanges := pbtAttachRanges(publishedFiles, stepSize, endTxNum)
	if err := validatePBTAttachFileKinds(nodeFiles, publishedFiles, stepSize, endTxNum); err != nil {
		return err
	}
	for _, domain := range pbtAttachDomains {
		if domain == kv.CommitmentBinDomain {
			continue
		}
		if !slices.Equal(nodeRanges[domain], publishedRanges[domain]) {
			return fmt.Errorf("commitment attach-pbt: ranges for %s do not match through txNum %d", domain, endTxNum)
		}
	}
	return nil
}

func validatePBTAttachPublishedFiles(publishedDirs datadir.Dirs, stepSize, endTxNum uint64) error {
	if stepSize == 0 {
		return errors.New("commitment attach-pbt: step size is zero")
	}
	publishedFiles, err := pbtAttachFiles(publishedDirs)
	if err != nil {
		return err
	}
	publishedFiles = pbtAttachVisibleFiles(publishedFiles)
	for _, file := range publishedFiles {
		if file.from*stepSize > endTxNum {
			return fmt.Errorf("commitment attach-pbt: published file %s extends past conversion txNum %d", file.path, endTxNum)
		}
	}
	ranges := pbtAttachRanges(publishedFiles, stepSize, endTxNum)
	if !slices.Equal(ranges[kv.CommitmentDomain], ranges[kv.CommitmentBinDomain]) {
		return fmt.Errorf("commitment attach-pbt: published commitment-bin ranges do not match commitment ranges")
	}
	for _, domain := range pbtAttachDomains {
		if len(ranges[domain]) == 0 {
			return fmt.Errorf("commitment attach-pbt: published files are missing domain %s through txNum %d", domain, endTxNum)
		}
	}
	return validatePBTAttachFrontier(publishedFiles, stepSize, endTxNum)
}

func validatePBTAttachFrontier(files []pbtAttachFile, stepSize, endTxNum uint64) error {
	frontiers := make(map[kv.Domain]uint64, len(pbtAttachDomains))
	for _, file := range files {
		if file.data && file.from*stepSize <= endTxNum && file.to*stepSize > frontiers[file.domain] {
			frontiers[file.domain] = file.to * stepSize
		}
	}
	for _, domain := range pbtAttachDomains {
		if frontiers[domain] <= endTxNum {
			return fmt.Errorf("commitment attach-pbt: published %s files do not cover conversion txNum %d", domain, endTxNum)
		}
	}
	return nil
}

func validatePBTAttachFileKinds(nodeFiles, publishedFiles []pbtAttachFile, stepSize, endTxNum uint64) error {
	published := make(map[string]struct{}, len(publishedFiles))
	for _, file := range publishedFiles {
		if file.from*stepSize <= endTxNum {
			published[pbtAttachFileKind(file)] = struct{}{}
		}
	}
	for _, file := range nodeFiles {
		if file.from*stepSize > endTxNum || !pbtAttachAdoptsFile(file) || file.domain == kv.CommitmentDomain || file.domain == kv.CommitmentBinDomain {
			continue
		}
		if _, ok := published[pbtAttachFileKind(file)]; !ok {
			return fmt.Errorf("commitment attach-pbt: published set is missing %s for %s", filepath.Ext(file.path), file.domain)
		}
	}
	return nil
}

func validatePBTAttachUncutFiles(nodeFiles []pbtAttachFile, stepSize, endTxNum uint64) error {
	for _, file := range nodeFiles {
		if !pbtAttachStateDomain(file.domain) || pbtAttachAdoptsFile(file) {
			continue
		}
		if file.from*stepSize <= endTxNum && file.to*stepSize > endTxNum+1 {
			return fmt.Errorf("commitment attach-pbt: node file %s spans conversion txNum %d and cannot be cut", file.path, endTxNum)
		}
	}
	return nil
}

func validatePBTAttachHistoryFrontier(nodeFiles []pbtAttachFile, stepSize, endTxNum uint64) error {
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		latestHistory := make(map[string]*pbtAttachFile)
		for i := range nodeFiles {
			file := &nodeFiles[i]
			if file.domain != domain || file.from*stepSize > endTxNum {
				continue
			}
			ext := filepath.Ext(file.path)
			if !slices.Contains([]string{".v", ".vi", ".ef", ".efi"}, ext) {
				continue
			}
			if latestHistory[ext] == nil || file.to > latestHistory[ext].to {
				latestHistory[ext] = file
			}
		}
		for _, file := range latestHistory {
			if file.to*stepSize < endTxNum {
				return fmt.Errorf("commitment attach-pbt: node history file %s ends at txNum %d before conversion txNum %d", file.path, file.to*stepSize, endTxNum)
			}
		}
	}
	return nil
}

func pbtAttachAdoptsFile(file pbtAttachFile) bool {
	if file.domain == kv.CommitmentDomain || file.domain == kv.CommitmentBinDomain {
		return true
	}
	if !pbtAttachStateDomain(file.domain) {
		return false
	}
	switch filepath.Ext(file.path) {
	case ".kv", ".bt", ".kvi", ".kvei":
		return true
	default:
		return false
	}
}

func pbtAttachStateDomain(domain kv.Domain) bool {
	return domain == kv.AccountsDomain || domain == kv.StorageDomain || domain == kv.CodeDomain
}

func pbtAttachFileKind(file pbtAttachFile) string {
	return fmt.Sprintf("%s:%d:%d:%s", file.domain, file.from, file.to, filepath.Ext(file.path))
}

func pbtAttachRanges(files []pbtAttachFile, stepSize, endTxNum uint64) map[kv.Domain][]string {
	ranges := make(map[kv.Domain][]string, len(pbtAttachDomains))
	for _, file := range files {
		if !file.data || file.from*stepSize > endTxNum {
			continue
		}
		ranges[file.domain] = append(ranges[file.domain], fmt.Sprintf("%d-%d", file.from*stepSize, file.to*stepSize))
	}
	for _, values := range ranges {
		slices.Sort(values)
	}
	return ranges
}

func pbtAttachFiles(dirs datadir.Dirs) ([]pbtAttachFile, error) {
	return pbtSnapshotFiles(dirs, func(domain kv.Domain) bool { return slices.Contains(pbtAttachDomains, domain) })
}

func pbtSnapshotFiles(dirs datadir.Dirs, include func(kv.Domain) bool) ([]pbtAttachFile, error) {
	roots := []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors}
	files := make([]pbtAttachFile, 0)
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
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
			domain, err := kv.String2Domain(parsed.TypeString)
			if err != nil {
				domain = kv.Domain(kv.MaxUint16)
			}
			if !include(domain) {
				return nil
			}
			files = append(files, pbtAttachFile{path: path, domain: domain, from: parsed.From, to: parsed.To, data: filepath.Ext(path) == ".kv"})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return files, nil
}

func pbtAttachVisibleFiles(files []pbtAttachFile) []pbtAttachFile {
	visible := make([]pbtAttachFile, 0, len(files))
	for i, file := range files {
		hidden := false
		for j, other := range files {
			if i == j || file.domain != other.domain || filepath.Ext(file.path) != filepath.Ext(other.path) {
				continue
			}
			if other.from <= file.from && other.to >= file.to && (other.from < file.from || other.to > file.to) {
				hidden = true
				break
			}
		}
		if !hidden {
			visible = append(visible, file)
		}
	}
	return visible
}

func pbtAttachExecutionProgress(ctx context.Context, dirs datadir.Dirs) (uint64, error) {
	db, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	var progress uint64
	err = db.View(ctx, func(tx kv.Tx) error {
		var err error
		progress, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	})
	return progress, err
}

func pbtAttachFirstStepPastPoint(files []pbtAttachFile, stepSize, endTxNum uint64) (uint64, bool) {
	var first uint64
	found := false
	for _, file := range files {
		if !pbtAttachAdoptsFile(file) || file.to*stepSize <= endTxNum+1 {
			continue
		}
		if !found || file.from < first {
			first = file.from
			found = true
		}
	}
	return first, found
}

func adoptPBTFiles(nodeDirs, publishedDirs datadir.Dirs, stepSize, endTxNum uint64) error {
	if err := removePBTFilesPastPoint(nodeDirs, stepSize, endTxNum); err != nil {
		return err
	}
	if err := removePBTFiles(nodeDirs, pbtAttachAdoptsFile); err != nil {
		return err
	}
	if err := removePBTPath(filepath.Join(nodeDirs.Snap, "pbt-manifest.toml")); err != nil {
		return err
	}
	touched := make(map[string]struct{})
	publishedFiles, err := pbtAttachFiles(publishedDirs)
	if err != nil {
		return err
	}
	publishedFiles = pbtAttachVisibleFiles(publishedFiles)
	var adoptable uint64
	for _, file := range publishedFiles {
		if file.from*stepSize <= endTxNum && pbtAttachAdoptsFile(file) {
			adoptable++
		}
	}
	_, manifestErr := os.Stat(filepath.Join(publishedDirs.Snap, "pbt-manifest.toml"))
	manifestPresent := manifestErr == nil
	progress := dbstate.NewPBTProgress(dbstate.PBTProgressConfig{Phase: "attach adopt", Total: adoptable + btoi(manifestPresent), TempDir: nodeDirs.Tmp})
	defer progress.Done()
	for _, file := range publishedFiles {
		if file.from*stepSize > endTxNum || !pbtAttachAdoptsFile(file) {
			continue
		}
		rel, err := filepath.Rel(publishedDirs.Snap, file.path)
		if err != nil {
			return err
		}
		dst := filepath.Join(nodeDirs.Snap, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := linkOrCopyPBTFile(file.path, dst); err != nil {
			return err
		}
		if err := syncPBTFileSidecar(file.path, dst); err != nil {
			return err
		}
		progress.Add(1)
		touched[filepath.Dir(dst)] = struct{}{}
	}
	manifestSrc := filepath.Join(publishedDirs.Snap, "pbt-manifest.toml")
	if manifestPresent {
		manifestDst := filepath.Join(nodeDirs.Snap, "pbt-manifest.toml")
		if err := linkOrCopyPBTFile(manifestSrc, manifestDst); err != nil {
			return err
		}
		touched[filepath.Dir(manifestDst)] = struct{}{}
		progress.Add(1)
	}
	if err := fsyncPBTDirs(touched); err != nil {
		return err
	}
	return nil
}

func btoi(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func validatePBTAttachSalts(nodeDirs, publishedDirs datadir.Dirs) error {
	nodeSalt, nodeFound, err := readPBTAttachSalt(nodeDirs)
	if err != nil {
		return err
	}
	publishedSalt, publishedFound, err := readPBTAttachSalt(publishedDirs)
	if err != nil {
		return err
	}
	if nodeFound != publishedFound {
		return fmt.Errorf("commitment attach-pbt: salt-state.txt presence differs: node=%t published=%t", nodeFound, publishedFound)
	}
	if nodeFound && !bytes.Equal(nodeSalt, publishedSalt) {
		return fmt.Errorf("commitment attach-pbt: salt-state.txt differs: node=%x published=%x", nodeSalt, publishedSalt)
	}
	return nil
}

func readPBTAttachSalt(dirs datadir.Dirs) ([]byte, bool, error) {
	value, err := os.ReadFile(filepath.Join(dirs.Snap, "salt-state.txt"))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func removePBTFilesPastPoint(dirs datadir.Dirs, stepSize, endTxNum uint64) error {
	if stepSize == 0 {
		return errors.New("commitment attach-pbt: step size is zero")
	}
	return removePBTFiles(dirs, func(file pbtAttachFile) bool {
		return file.from*stepSize > endTxNum
	})
}

func removePBTFiles(dirs datadir.Dirs, match func(pbtAttachFile) bool) error {
	files, err := pbtSnapshotFiles(dirs, func(kv.Domain) bool { return true })
	if err != nil {
		return err
	}
	touched := make(map[string]struct{})
	for _, file := range files {
		if !match(file) {
			continue
		}
		if err := removePBTFileWithSidecar(file.path); err != nil {
			return err
		}
		touched[filepath.Dir(file.path)] = struct{}{}
	}
	if err := fsyncPBTDirs(touched); err != nil {
		return err
	}
	return nil
}

func fsyncPBTDirs(dirs map[string]struct{}) error {
	for directory := range dirs {
		if err := dir.FsyncDir(directory); err != nil {
			return err
		}
	}
	return nil
}

func removePBTFileWithSidecar(path string) error {
	if err := removePBTPath(path); err != nil {
		return err
	}
	return removePBTPath(path + ".torrent")
}

func removePBTPath(path string) error {
	if err := dir.RemoveFile(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func syncPBTFileSidecar(src, dst string) error {
	_, err := os.Stat(src + ".torrent")
	if errors.Is(err, fs.ErrNotExist) {
		return removePBTPath(dst + ".torrent")
	}
	if err != nil {
		return err
	}
	return linkOrCopyPBTFile(src+".torrent", dst+".torrent")
}

func linkOrCopyPBTFile(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if dstInfo, statErr := os.Stat(dst); statErr == nil && os.SameFile(srcInfo, dstInfo) {
		return nil
	} else if statErr != nil && !errors.Is(statErr, fs.ErrNotExist) {
		return statErr
	}
	if err := os.Link(src, dst); err == nil {
		return dir.FsyncDir(filepath.Dir(dst))
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, srcInfo.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return dir.FsyncDir(filepath.Dir(dst))
}

func resetPBTExecution(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, logger log.Logger) error {
	configurePBTSourceVariant(settings)
	rawDB, err := dbCfg(dbcfg.ChainDB, dirs.Chaindata).Accede(false).Exclusive(true).Open(ctx)
	if err != nil {
		return err
	}
	agg, err := openPBTState(ctx, dirs, settings, rawDB, logger)
	if err != nil {
		rawDB.Close()
		return err
	}
	db, err := dbtemporal.New(rawDB, agg, nil)
	if err != nil {
		agg.Close()
		rawDB.Close()
		return err
	}
	defer db.Close()
	return rawdbreset.ResetExec(ctx, db)
}
