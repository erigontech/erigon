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

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
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
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/debug"
)

var (
	attachPBTFrom                 string
	attachPBTStepHook             func(string) error
	validatePBTAttachLeafStampsFn = validatePBTAttachLeafStamps
	pbtAttachPublishedRootFn      = pbtAttachPublishedRoot
)

func init() {
	withChain(cmdCommitmentAttachPBT)
	withDataDir(cmdCommitmentAttachPBT)
	withConfig(cmdCommitmentAttachPBT)
	withExperimentalCommitment(cmdCommitmentAttachPBT)
	cmdCommitmentAttachPBT.Flags().StringVar(&attachPBTFrom, "from", "", "published datadir containing converted commitment files")
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
		return attachPBT(ctx, datadirCli, attachPBTFrom, chain, logger)
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
	if nodePath == "" || publishedPath == "" {
		return errors.New("commitment attach-pbt: node and published datadirs are required")
	}
	nodeDirs := datadir.Open(nodePath)
	publishedDirs := datadir.Open(publishedPath)
	marker, err := dbstate.ReadPBTAttachMarker(nodeDirs)
	if err != nil {
		return err
	}
	if marker == nil {
		if _, err := dbstate.EnableCommitmentV3FromFiles(nodeDirs); err != nil {
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
	if marker == nil {
		if err := validatePBTAttachFiles(nodeDirs, publishedDirs, publishedSettings.StepSize, txNum); err != nil {
			return err
		}
	} else if err := validatePBTAttachPublishedFiles(publishedDirs, publishedSettings.StepSize, txNum); err != nil {
		return err
	}
	rawDB, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, nodeDirs.Chaindata), true)
	if err != nil {
		return err
	}
	if marker == nil {
		if err := checkPBTNodeProgress(ctx, rawDB, blockNum); err != nil {
			rawDB.Close()
			return err
		}
	}
	blockReader, blockView, closeBlockReader, err := openPBTBlockReader(ctx, nodeDirs, rawDB, logger)
	if err != nil {
		rawDB.Close()
		return err
	}
	if marker == nil {
		if err := checkPBTNodePositionWithFiles(ctx, rawDB, blockReader, blockView, blockNum, txNum); err != nil {
			closeBlockReader()
			rawDB.Close()
			return err
		}
	}
	closeBlockReader()
	rawDB.Close()
	if err := validatePBTAttachPublishedPoint(ctx, publishedDirs, publishedSettings, blockNum, txNum, logger); err != nil {
		return err
	}
	blockHash, blockEnd, afterFork, err := pbtAttachBlockEnd(ctx, nodeDirs, blockNum, txNum)
	if err != nil {
		return err
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
		markerBlock, markerTx, markerPoint, markerErr := marker.Settings.ConversionPoint()
		if markerErr != nil {
			return markerErr
		}
		if !markerPoint || markerBlock != blockNum || markerTx != txNum {
			return fmt.Errorf("commitment attach-pbt: marker conversion point does not match published point (%d, %d)", blockNum, txNum)
		}
		finalSettings = marker.Settings
	}
	if marker == nil {
		if err := dbstate.WritePBTAttachMarker(nodeDirs, &dbstate.PBTAttachMarker{PublishedPath: absolutePublishedPath, Settings: finalSettings}); err != nil {
			return err
		}
	}
	if err := runPBTAttachStepHook("marker"); err != nil {
		return err
	}
	if err := adoptPBTFiles(nodeDirs, publishedDirs, publishedSettings.StepSize, txNum); err != nil {
		return err
	}
	if err := runPBTAttachStepHook("swap"); err != nil {
		return err
	}
	if err := resetPBTExecution(ctx, nodeDirs, publishedSettings, logger); err != nil {
		return err
	}
	if err := runPBTAttachStepHook("reset"); err != nil {
		return err
	}
	if blockEnd {
		var shadowRoot common.Hash
		if afterFork {
			if hexRoot, found, err := pbtAttachHexRoot(ctx, publishedDirs, publishedSettings, blockNum, txNum, logger); err != nil {
				return err
			} else if found {
				shadowRoot = hexRoot
			} else {
				blockEnd = false
			}
		} else {
			shadowRoot, err = pbtAttachPublishedRootFn(ctx, publishedDirs, publishedSettings, logger)
			if err != nil {
				return err
			}
		}
		if blockEnd {
			if err := writePBTAttachShadowRoot(ctx, nodeDirs, blockHash, blockNum, shadowRoot); err != nil {
				return err
			}
		}
	}
	if err := dbstate.WriteErigonDBSettings(nodeDirs, finalSettings); err != nil {
		return err
	}
	if err := runPBTAttachStepHook("settings"); err != nil {
		return err
	}
	return dbstate.RemovePBTAttachMarker(nodeDirs)
}

func runPBTAttachStepHook(step string) error {
	if attachPBTStepHook == nil {
		return nil
	}
	return attachPBTStepHook(step)
}

func pbtAttachPublishedRoot(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, logger log.Logger) (common.Hash, error) {
	configurePBTSourceVariant(settings)
	if err := eip8297.SetHashSuite(settings.TrieHashName()); err != nil {
		return common.Hash{}, err
	}
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return common.Hash{}, err
	}
	defer agg.Close()
	if err := agg.OpenFolder(nil); err != nil {
		return common.Hash{}, err
	}
	at := agg.BeginFilesRo()
	defer at.Close()
	builder, err := eip8297.NewStreamRootBuilder(eip8297.SelectedHash())
	if err != nil {
		return common.Hash{}, err
	}
	if err := dbstate.ForEachPBinLeaf(at, nil, true, func(leaf dbstate.PBinLeaf) error {
		return builder.Add(leaf.Key, leaf.Value)
	}); err != nil {
		return common.Hash{}, err
	}
	return builder.RootHash()
}

func pbtAttachBlockEnd(ctx context.Context, dirs datadir.Dirs, blockNum, txNum uint64) (common.Hash, bool, bool, error) {
	db, err := backup.OpenExisting(ctx, dbCfg(dbcfg.ChainDB, dirs.Chaindata), true)
	if err != nil {
		return common.Hash{}, false, false, err
	}
	defer db.Close()
	blockReader, blockView, closeBlockReader, err := openPBTBlockReader(ctx, dirs, db, log.Root())
	if err != nil {
		return common.Hash{}, false, false, err
	}
	defer closeBlockReader()
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return common.Hash{}, false, false, err
	}
	defer tx.Rollback()
	blockTx := pbtBlockFilesTx{Tx: tx, view: blockView}
	maxTxNum, found, err := blockReader.TxnumReader().MaxExact(ctx, blockTx, blockNum)
	if err != nil {
		return common.Hash{}, false, false, err
	}
	if !found {
		return common.Hash{}, false, false, fmt.Errorf("commitment attach-pbt: block %d has no txNum mapping", blockNum)
	}
	header, err := blockReader.HeaderByNumber(ctx, blockTx, blockNum)
	if err != nil {
		return common.Hash{}, false, false, err
	}
	if header == nil {
		return common.Hash{}, false, false, nil
	}
	genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
	if err != nil {
		return common.Hash{}, false, false, err
	}
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisHash)
	if err != nil {
		return common.Hash{}, false, false, err
	}
	afterFork := chainConfig != nil && chainConfig.IsBinaryTrie(header.Time)
	return header.Hash(), maxTxNum == txNum, afterFork, nil
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
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return common.Hash{}, false, err
	}
	defer agg.Close()
	if err := agg.OpenFolder(nil); err != nil {
		return common.Hash{}, false, err
	}
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

func validatePBTAttachPublishedPoint(ctx context.Context, dirs datadir.Dirs, settings *dbstate.ErigonDBSettings, blockNum, txNum uint64, logger log.Logger) error {
	configurePBTSourceVariant(settings)
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		return err
	}
	defer agg.Close()
	if err := agg.OpenFolder(nil); err != nil {
		return err
	}
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
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		files := at.Files(domain)
		if len(files) == 0 || files[len(files)-1].StartRootNum() > txNum || files[len(files)-1].EndRootNum() <= txNum {
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
	for _, domain := range []kv.Domain{kv.CommitmentDomain, kv.CommitmentBinDomain} {
		files := at.Files(domain)
		if len(opened[domain]) == 0 || len(files) == 0 || files[len(files)-1].StartRootNum() > txNum || files[len(files)-1].EndRootNum() <= txNum {
			return fmt.Errorf("commitment attach-pbt: published %s files do not cover conversion txNum %d", domain, txNum)
		}
		for _, file := range publishedFiles {
			if file.domain == domain && file.data && file.from*settings.StepSize <= txNum {
				if _, ok := opened[domain][filepath.Clean(file.path)]; !ok {
					return fmt.Errorf("commitment attach-pbt: published %s file %s was not opened", domain, file.path)
				}
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
	if err := validatePBTAttachLeafStampsFn(txNum, func(emit func(dbstate.PBinLeaf) error) error {
		return dbstate.ForEachPBinLeaf(at, nil, true, emit)
	}); err != nil {
		return err
	}
	return nil
}

func validatePBTAttachLeafStamps(txNum uint64, forEach func(func(dbstate.PBinLeaf) error) error) error {
	return forEach(func(leaf dbstate.PBinLeaf) error {
		if leaf.Stamp > txNum {
			return fmt.Errorf("commitment attach-pbt: published leaf stamp %d is after conversion txNum %d", leaf.Stamp, txNum)
		}
		return nil
	})
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
	for _, file := range publishedFiles {
		if file.from*stepSize > endTxNum {
			return fmt.Errorf("commitment attach-pbt: published file %s extends past conversion txNum %d", file.path, endTxNum)
		}
	}
	nodeRanges := pbtAttachRanges(nodeFiles, stepSize, endTxNum)
	publishedRanges := pbtAttachRanges(publishedFiles, stepSize, endTxNum)
	if err := validatePBTAttachPublishedRanges(publishedRanges, endTxNum); err != nil {
		return err
	}
	if err := validatePBTAttachFrontier(publishedFiles, stepSize, endTxNum); err != nil {
		return err
	}
	if err := validatePBTAttachFileKinds(nodeFiles, publishedFiles, stepSize, endTxNum); err != nil {
		return err
	}
	for _, domain := range pbtAttachDomains {
		if (domain == kv.CommitmentDomain || domain == kv.CommitmentBinDomain) && len(publishedRanges[domain]) == 0 {
			return fmt.Errorf("commitment attach-pbt: published files are missing domain %s", domain)
		}
		if domain == kv.CommitmentBinDomain && len(nodeRanges[domain]) == 0 {
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
	if err := validatePBTAttachPublishedRanges(ranges, endTxNum); err != nil {
		return err
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
		if file.from*stepSize > endTxNum || !pbtAttachAdoptsFile(file) {
			continue
		}
		if file.domain == kv.CommitmentDomain || file.domain == kv.CommitmentBinDomain {
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
		var latestHistory *pbtAttachFile
		for i := range nodeFiles {
			file := &nodeFiles[i]
			if file.domain != domain || file.from*stepSize > endTxNum || pbtAttachAdoptsFile(*file) {
				continue
			}
			if latestHistory == nil || file.to > latestHistory.to {
				latestHistory = file
			}
		}
		if latestHistory != nil && latestHistory.to*stepSize <= endTxNum {
			return fmt.Errorf("commitment attach-pbt: node history file %s ends at txNum %d before conversion txNum %d", latestHistory.path, latestHistory.to*stepSize, endTxNum)
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

func validatePBTAttachPublishedRanges(ranges map[kv.Domain][]string, endTxNum uint64) error {
	if !slices.Equal(ranges[kv.CommitmentDomain], ranges[kv.CommitmentBinDomain]) {
		return fmt.Errorf("commitment attach-pbt: published commitment-bin ranges do not match commitment ranges")
	}
	for _, domain := range pbtAttachDomains {
		if len(ranges[domain]) == 0 {
			return fmt.Errorf("commitment attach-pbt: published files are missing domain %s through txNum %d", domain, endTxNum)
		}
	}
	return nil
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
			if err != nil || !pbtAttachDomain(domain) {
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

func pbtAttachDomain(domain kv.Domain) bool {
	return slices.Contains(pbtAttachDomains, domain)
}

func checkPBTNodeProgress(ctx context.Context, db kv.RoDB, blockNum uint64) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	progress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	if progress < blockNum {
		return fmt.Errorf("commitment attach-pbt: node is behind conversion block %d", blockNum)
	}
	return nil
}

func checkPBTNodePositionWithFiles(ctx context.Context, db kv.RoDB, reader *freezeblocks.BlockReader, view *blocksnapshots.View, blockNum, txNum uint64) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	progress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return err
	}
	if progress == blockNum {
		blockTx := pbtBlockFilesTx{Tx: tx, view: view}
		maxTxNum, found, err := reader.TxnumReader().MaxExact(ctx, blockTx, blockNum)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("commitment attach-pbt: block %d has no txNum mapping", blockNum)
		}
		if maxTxNum < txNum {
			return fmt.Errorf("commitment attach-pbt: node is behind conversion txNum %d", txNum)
		}
	}
	return nil
}

func adoptPBTFiles(nodeDirs, publishedDirs datadir.Dirs, stepSize, endTxNum uint64) error {
	if err := removePBTFilesPastPoint(nodeDirs, stepSize, endTxNum); err != nil {
		return err
	}
	nodeFiles, err := pbtAttachFiles(nodeDirs)
	if err != nil {
		return err
	}
	nodeFiles = pbtAttachVisibleFiles(nodeFiles)
	for _, file := range nodeFiles {
		if !pbtAttachAdoptsFile(file) {
			continue
		}
		if err := dir.RemoveFile(file.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	publishedFiles, err := pbtAttachFiles(publishedDirs)
	if err != nil {
		return err
	}
	publishedFiles = pbtAttachVisibleFiles(publishedFiles)
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
	}
	return nil
}

func validatePBTAttachSalts(nodeDirs, publishedDirs datadir.Dirs) error {
	name := "salt-state.txt"
	nodeSalt, nodeFound, err := readPBTAttachSalt(nodeDirs, name)
	if err != nil {
		return err
	}
	publishedSalt, publishedFound, err := readPBTAttachSalt(publishedDirs, name)
	if err != nil {
		return err
	}
	if nodeFound != publishedFound {
		return fmt.Errorf("commitment attach-pbt: %s presence differs: node=%t published=%t", name, nodeFound, publishedFound)
	}
	if nodeFound && !bytes.Equal(nodeSalt, publishedSalt) {
		return fmt.Errorf("commitment attach-pbt: %s differs: node=%x published=%x", name, nodeSalt, publishedSalt)
	}
	return nil
}

func readPBTAttachSalt(dirs datadir.Dirs, name string) ([]byte, bool, error) {
	value, err := os.ReadFile(filepath.Join(dirs.Snap, name))
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
	for _, root := range []string{dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
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
			if ok && parsed.From*stepSize > endTxNum {
				return dir.RemoveFile(path)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func removePBTStateHistoryIndexFiles(dirs datadir.Dirs) error {
	files, err := pbtAttachFiles(dirs)
	if err != nil {
		return err
	}
	for _, file := range files {
		if pbtAttachStateDomain(file.domain) && !pbtAttachAdoptsFile(file) {
			if err := dir.RemoveFile(file.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
		}
	}
	return nil
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
		return nil
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
	agg, err := dbstate.New(dirs).Logger(logger).WithErigonDBSettings(settings).SkipFilesDBGapCheck().SkipPBinStateDBCheck().DisableInterDomainDeps().Open(ctx)
	if err != nil {
		rawDB.Close()
		return err
	}
	if err := agg.OpenFolder(rawDB); err != nil {
		agg.Close()
		rawDB.Close()
		return err
	}
	db, err := dbtemporal.New(rawDB, agg, nil)
	if err != nil {
		agg.Close()
		rawDB.Close()
		return err
	}
	if err := rawdbreset.ResetExec(ctx, db); err != nil {
		db.Close()
		return err
	}
	db.Close()
	return nil
}
