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

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	dbtemporal "github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func runExportPBT(ctx context.Context, tx kv.TemporalTx, headerAt func(uint64) (*types.Header, error), outDir string, logger log.Logger) error {
	return runExportPBTWithTxNumReader(ctx, tx, rawdbv3.TxNums, headerAt, outDir, logger, nil)
}

func runExportPBTWithReadbackHook(ctx context.Context, tx kv.TemporalTx, headerAt func(uint64) (*types.Header, error), outDir string, logger log.Logger, beforeReadback func(string) error) error {
	return runExportPBTWithTxNumReader(ctx, tx, rawdbv3.TxNums, headerAt, outDir, logger, beforeReadback)
}

func TestRunExportPBTWritesStrictArtifacts(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	db, root := newPBTExportDB(t)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	outDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, runExportPBT(t.Context(), tx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: root, Time: 10}, nil
	}, outDir, log.New()))
	_, err = os.Stat(filepath.Join(outDir, pbtSnapshotFileName))
	require.NoError(t, err, "the export must write the PBT snapshot")
	preimages, err := os.ReadFile(filepath.Join(outDir, pbtPreimagesFileName))
	require.NoError(t, err)
	require.NotEmpty(t, preimages)
	snapshotBytes, err := os.ReadFile(filepath.Join(outDir, pbtSnapshotFileName))
	require.NoError(t, err)
	snapshotMeta, err := artifact.ReadSnapshotAt(bytes.NewReader(snapshotBytes), int64(len(snapshotBytes)), artifact.SnapshotCallbacks{})
	require.NoError(t, err)
	require.NoError(t, artifact.JoinAt(bytes.NewReader(snapshotBytes), int64(len(snapshotBytes)), bytes.NewReader(preimages), int64(len(preimages)), eip8297.HashBytes, nil, t.TempDir()))
	require.Equal(t, root, snapshotMeta.Root)
	metaBytes, err := os.ReadFile(filepath.Join(outDir, pbtMetaFileName))
	require.NoError(t, err)
	var meta pbtExportMeta
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	require.Equal(t, root.Hex(), meta.PBTRoot)
	require.Equal(t, commitment.PBinHashBlake3, meta.HashSuite)
}

func TestRunExportPBTRefusesChangedBinRecord(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	db, root := newPBTExportDB(t)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentBinDomain), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	previous, _, err := domains.GetLatest(kv.CommitmentBinDomain, tx, pbt.GlobalRootKey())
	require.NoError(t, err)
	require.NotEmpty(t, previous)
	corrupted := bytes.Clone(previous)
	corrupted[len(corrupted)-1] ^= 1
	require.NoError(t, domains.DomainPut(kv.CommitmentBinDomain, tx, pbt.GlobalRootKey(), corrupted, 1, previous))
	require.NoError(t, domains.Flush(t.Context(), tx))
	domains.Close()
	require.NoError(t, tx.Commit())
	roTx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer roTx.Rollback()
	binRoot, found, err := exportPBTBinRootAtPin(t.Context(), roTx, exportPin{Block: 7, TxNum: 1, Domain: kv.CommitmentBinDomain}, log.New())
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, root, binRoot)
	err = runExportPBT(t.Context(), roTx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: binRoot, Time: 10}, nil
	}, filepath.Join(t.TempDir(), "export"), log.New())
	require.Error(t, err, "a changed bin record must refuse export")
}

func TestExportPBTBinOnlyRootCrossCheck(t *testing.T) {
	selectPBTExportSuite(t)
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = false
	db, storedRoot := newPBTBinOnlyEmptyExportDB(t)
	require.NotEqual(t, eip8297.EmptyTreeHash, storedRoot)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	err = runExportPBT(t.Context(), tx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: storedRoot, Time: 10}, nil
	}, filepath.Join(t.TempDir(), "export"), log.New())
	require.Error(t, err, "a nonzero stored bin root with an empty latest state must refuse export")
}

func TestRunExportPBTReadbackRefusesTruncatedSnapshot(t *testing.T) {
	selectPBTExportSuite(t)
	db, root := newPBTExportDB(t)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	err = runExportPBTWithReadbackHook(t.Context(), tx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: root, Time: 10}, nil
	}, filepath.Join(t.TempDir(), "export"), log.New(), func(path string) error {
		return os.Truncate(path, 1)
	})
	require.Error(t, err, "truncated output must fail strict read-back")
}

func TestRunExportPBTDigestsAreStable(t *testing.T) {
	selectPBTExportSuite(t)
	db, root := newPBTExportDB(t)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	paths := make([]string, 2)
	for i := range paths {
		paths[i] = filepath.Join(t.TempDir(), "export")
		require.NoError(t, runExportPBT(t.Context(), tx, func(uint64) (*types.Header, error) {
			return &types.Header{Root: root, Time: 10}, nil
		}, paths[i], log.New()))
	}
	for _, name := range []string{pbtSnapshotFileName, pbtPreimagesFileName, pbtMetaFileName} {
		first, readErr := os.ReadFile(filepath.Join(paths[0], name))
		require.NoError(t, readErr)
		second, readErr := os.ReadFile(filepath.Join(paths[1], name))
		require.NoError(t, readErr)
		require.Equal(t, first, second, name)
	}
}

func TestRunExportPBTEmptyState(t *testing.T) {
	selectPBTExportSuite(t)
	db, root := newPBTEmptyExportDB(t)
	require.Equal(t, eip8297.EmptyTreeHash, root)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	outDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, runExportPBT(t.Context(), tx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: root, Time: 10}, nil
	}, outDir, log.New()))
	snapshotBytes, err := os.ReadFile(filepath.Join(outDir, pbtSnapshotFileName))
	require.NoError(t, err)
	snapshot, err := artifact.ReadSnapshotAt(bytes.NewReader(snapshotBytes), int64(len(snapshotBytes)), artifact.SnapshotCallbacks{})
	require.NoError(t, err)
	require.Equal(t, eip8297.EmptyTreeHash, snapshot.Root)
	require.Zero(t, snapshot.HeaderCount)
	preimageBytes, err := os.ReadFile(filepath.Join(outDir, pbtPreimagesFileName))
	require.NoError(t, err)
	require.NoError(t, artifact.ReadPreimagesAt(bytes.NewReader(preimageBytes), int64(len(preimageBytes)), nil))
}

func TestRunExportPBTRealAcceptanceChain(t *testing.T) {
	selectPBTExportSuite(t)
	fixture, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
	tx, err := fixture.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	outDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, runExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		if block == 0 {
			return fixture.Tester.Genesis.HeaderNoCopy(), nil
		}
		return fixture.Chain.Headers[block-1], nil
	}, outDir, log.New()))
}

func TestRunExportPBTUsesStoppedExecutionStage(t *testing.T) {
	selectPBTExportSuite(t)
	const block = 2
	full, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, full.Tester.InsertChain(full.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), full.Tester.DB))
	require.NoError(t, full.Tester.ExecModule.ResetCurrentContext(t.Context()))
	require.NoError(t, full.Tester.ReExecuteTo(t.Context(), block))
	fullDir := filepath.Join(t.TempDir(), "full")
	fullTx, err := full.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer fullTx.Rollback()
	require.NoError(t, runExportPBT(t.Context(), fullTx, func(n uint64) (*types.Header, error) {
		return full.Chain.Headers[n-1], nil
	}, fullDir, log.New()))

	partial, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, partial.Tester.InsertChain(partial.Chain.Slice(0, block)))
	partialDir := filepath.Join(t.TempDir(), "partial")
	partialTx, err := partial.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer partialTx.Rollback()
	require.NoError(t, runExportPBT(t.Context(), partialTx, func(n uint64) (*types.Header, error) {
		return partial.Chain.Headers[n-1], nil
	}, partialDir, log.New()))
	fullMeta, err := os.ReadFile(filepath.Join(fullDir, pbtMetaFileName))
	require.NoError(t, err)
	partialMeta, err := os.ReadFile(filepath.Join(partialDir, pbtMetaFileName))
	require.NoError(t, err)
	var fullResult, partialResult pbtExportMeta
	require.NoError(t, json.Unmarshal(fullMeta, &fullResult))
	require.NoError(t, json.Unmarshal(partialMeta, &partialResult))
	require.Equal(t, partialResult.SnapshotDigest, fullResult.SnapshotDigest)
}

func TestDoExportPBTUsesFrozenBlockFiles(t *testing.T) {
	selectPBTExportSuite(t)
	dirs := buildFrozenPBTExportDatadir(t)
	tmpFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(tmpFile, nil, 0o644))
	t.Setenv("TMPDIR", tmpFile)
	outDir := filepath.Join(t.TempDir(), "export")
	cmd := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "out"},
	}}
	require.NoError(t, cmd.Set(utils.DataDirFlag.Name, dirs.DataDir))
	require.NoError(t, cmd.Set("out", outDir))
	require.NoError(t, doExportPBT(t.Context(), cmd))
	require.FileExists(t, filepath.Join(outDir, pbtSnapshotFileName))
}

func TestDoExportPreimagesUsesFrozenBlockFiles(t *testing.T) {
	selectPBTExportSuite(t)
	dirs := buildFrozenPBTExportDatadir(t)
	tmpFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(tmpFile, nil, 0o644))
	t.Setenv("TMPDIR", tmpFile)
	outDir := filepath.Join(t.TempDir(), "export")
	cmd := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "out"},
		&cli.StringFlag{Name: "tmpdir"},
	}}
	require.NoError(t, cmd.Set(utils.DataDirFlag.Name, dirs.DataDir))
	require.NoError(t, cmd.Set("out", outDir))
	require.NoError(t, cmd.Set("tmpdir", filepath.Join(t.TempDir(), "tmp")))
	require.NoError(t, doExportPreimages(t.Context(), cmd))
	require.FileExists(t, filepath.Join(outDir, preimagesFileName))
}

func TestDoExportPBTHexOnlyDefaultsToBlake3(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = ""
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	fixture, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	tx, err := fixture.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	builder, err := eip8297.NewStreamRootBuilder(eip8297.SelectedHash())
	require.NoError(t, err)
	require.NoError(t, state.ForEachPBinLeaf(state.AggTx(tx), tx, false, func(leaf state.PBinLeaf) error {
		return builder.Add(leaf.Key, leaf.Value)
	}))
	want, err := builder.RootHash()
	require.NoError(t, err)
	tx.Rollback()
	fixture.Tester.Close()
	outDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	cmd := &cli.Command{Flags: []cli.Flag{
		&cli.StringFlag{Name: utils.DataDirFlag.Name},
		&cli.StringFlag{Name: "out"},
		&utils.ExperimentalBinCommitmentHashFlag,
	}}
	require.NoError(t, cmd.Set(utils.DataDirFlag.Name, fixture.Tester.Dirs.DataDir))
	require.NoError(t, cmd.Set("out", outDir))
	require.NoError(t, doExportPBT(t.Context(), cmd))
	metaBytes, err := os.ReadFile(filepath.Join(outDir, pbtMetaFileName))
	require.NoError(t, err)
	var meta pbtExportMeta
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	require.Equal(t, commitment.PBinHashBlake3, meta.HashSuite)
	require.Equal(t, want.Hex(), meta.PBTRoot)
}

func TestConfigurePBTExportHashDefaultsForPreimages(t *testing.T) {
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	dirs := datadir.New(t.TempDir())
	variant := state.TrieVariantHex
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{TrieVariant: &variant}))
	cmd := &cli.Command{Flags: []cli.Flag{&utils.ExperimentalBinCommitmentHashFlag}}
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	restore, err := configurePBTExportHash(dirs, cmd)
	require.NoError(t, err)
	require.Equal(t, commitment.PBinHashBlake3, commitment.PBinHashSuiteName())
	restore()
	statecfg.BinCommitmentHash = commitment.PBinHashKeccak
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	restore, err = configurePBTExportHash(dirs, cmd)
	require.NoError(t, err)
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName())
	restore()
}

func TestConfigurePBTExportHashRejectsDatadirMismatch(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	variant, hash := state.TrieVariantHexBin, commitment.PBinHashBlake3
	refs := false
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs, TrieVariant: &variant, TrieHash: &hash}))
	cmd := &cli.Command{Flags: []cli.Flag{&utils.ExperimentalBinCommitmentHashFlag}}
	require.NoError(t, cmd.Set(utils.ExperimentalBinCommitmentHashFlag.Name, commitment.PBinHashKeccak))
	_, err := configurePBTExportHash(dirs, cmd)
	require.ErrorContains(t, err, "differs from datadir trie_hash")
}

func buildFrozenPBTExportDatadir(t *testing.T) datadir.Dirs {
	t.Helper()
	fixture, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, fixture.Tester.InsertChain(fixture.Chain))
	block := fixture.Chain.Blocks[1]
	blockNum := block.NumberU64()
	config := snapcfg.KnownCfgOrDevnet(fixture.Tester.ChainConfig.ChainName)
	require.NoError(t, freezeblocks.DumpBlocks(t.Context(), 0, 3, fixture.Tester.ChainConfig, fixture.Tester.Dirs.Tmp, fixture.Tester.Dirs.Snap, fixture.Tester.DB, 1, log.LvlInfo, log.New(), fixture.Tester.BlockReader, config, nil))
	fixture.Tester.Close()
	dirs := fixture.Tester.Dirs
	settings, err := state.ReadErigonDBSettings(dirs)
	require.NoError(t, err)
	rawDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	agg := state.New(dirs).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(rawDB))
	db, err := dbtemporal.New(rawDB, agg, nil)
	require.NoError(t, err)
	var lastTxNum uint64
	require.NoError(t, rawDB.View(t.Context(), func(rawTx kv.Tx) error {
		var found bool
		lastTxNum, found, err = rawdbv3.TxNums.MaxExact(t.Context(), rawTx, blockNum)
		require.True(t, found)
		return err
	}))
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 0, kv.Step(lastTxNum)+1, execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums), false))
	agg.WaitForFiles()
	require.NoError(t, rawdbreset.ResetExec(t.Context(), db))
	db.Close()
	agg.Close()
	rawDB.Close()

	rawDB = dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(rawTx kv.RwTx) error {
		rawdb.DeleteHeader(rawTx, block.Hash(), blockNum)
		rawdb.DeleteBody(rawTx, block.Hash(), blockNum)
		if err := rawTx.Delete(kv.HeaderCanonical, hexutil.EncodeTs(blockNum)); err != nil {
			return err
		}
		if err := rawTx.Delete(kv.MaxTxNum, hexutil.EncodeTs(blockNum)); err != nil {
			return err
		}
		return stages.SaveStageProgress(rawTx, stages.Execution, blockNum)
	}))
	rawDB.Close()
	return dirs
}

func selectPBTExportSuite(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousParallel := statecfg.ExperimentalParallelCommitment
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.ExperimentalParallelCommitment = previousParallel
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.ExperimentalParallelCommitment = false
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
}

func newPBTExportDB(t *testing.T) (kv.TemporalRwDB, common.Hash) {
	return newPBTExportDBWithAccount(t, true)
}

func newPBTEmptyExportDB(t *testing.T) (kv.TemporalRwDB, common.Hash) {
	return newPBTExportDBWithAccount(t, false)
}

func newPBTBinOnlyEmptyExportDB(t *testing.T) (kv.TemporalRwDB, common.Hash) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	refs := false
	variant := state.TrieVariantBin
	hash := commitment.PBinHashBlake3
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{
		StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
		TrieVariant: &variant, TrieHash: &hash,
	}))
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesisHash := common.Hash{0x42}
	forkTime := uint64(10)
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesisHash, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesisHash, &chain.Config{BinaryTrieTime: &forkTime}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 7, 1))
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg), execctx.WithCommitmentDomain(kv.CommitmentDomain))
	require.NoError(t, err)
	binCtx := domains.GetCommitmentCtxForDomain(kv.CommitmentDomain)
	address := bytes.Repeat([]byte{0xaa}, 20)
	binCtx.SetPBinFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{{
		Address: address, Exists: true, CodeWritten: true, Balance: *uint256.NewInt(1), CodeHash: common.Hash(empty.CodeHash),
	}}})
	rootBytes, err := binCtx.ComputeCommitment(t.Context(), tx, true, 7, 1, "export-pbt-test", nil)
	require.NoError(t, err)
	root := common.BytesToHash(rootBytes)
	require.NoError(t, rawdb.WriteHeader(tx, &types.Header{Number: *uint256.NewInt(7), Time: forkTime, Root: root}))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, common.Hash{7}, 7))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 7))
	require.NoError(t, tx.Commit())
	domains.Close()
	return db, root
}

func newPBTExportDBWithAccount(t *testing.T, withAccount bool) (kv.TemporalRwDB, common.Hash) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	refs := false
	variant := state.TrieVariantHexBin
	hash := commitment.PBinHashBlake3
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{
		StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
		TrieVariant: &variant, TrieHash: &hash,
	}))
	db := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesisHash := common.Hash{0x42}
	forkTime := uint64(10)
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesisHash, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesisHash, &chain.Config{BinaryTrieTime: &forkTime}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 7, 1))
	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantCommitmentV3
	cfg.EnableTrieWarmup = false
	domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithTrieConfig(cfg))
	require.NoError(t, err)
	defer domains.Close()
	binCtx := domains.GetCommitmentCtxForDomain(kv.CommitmentBinDomain)
	require.NotNil(t, binCtx)
	if withAccount {
		address := bytes.Repeat([]byte{0xaa}, 20)
		slot := bytes.Repeat([]byte{0x11}, 32)
		storageKey := append(append([]byte{}, address...), slot...)
		storageValue := bytes.Repeat([]byte{0x22}, 32)
		account := accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), 1, nil))
		residue := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{0x42}, 20)...)
		require.NoError(t, domains.DomainPut(kv.CodeDomain, tx, address, residue, 1, nil))
		require.NoError(t, domains.DomainPut(kv.StorageDomain, tx, storageKey, storageValue, 1, nil))
		binCtx.SetPBinFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{{
			Address: address, Exists: true, CodeWritten: true, Balance: *uint256.NewInt(1), CodeHash: common.Hash(empty.CodeHash),
			Slots: []commitment.PBinFeedSlot{{Key: slot, Value: storageValue}},
		}}})
	}
	_, err = binCtx.ComputeCommitment(t.Context(), tx, true, 7, 1, "export-pbt-test", nil)
	require.NoError(t, err)
	binRootBytes, err := binCtx.Trie().RootHash()
	require.NoError(t, err)
	binRoot := common.BytesToHash(binRootBytes)
	require.NoError(t, rawdb.WriteHeader(tx, &types.Header{Number: *uint256.NewInt(7), Time: forkTime, Root: binRoot}))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, common.Hash{7}, 7))
	require.NoError(t, domains.Flush(t.Context(), tx))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 7))
	require.NoError(t, tx.Commit())
	return db, binRoot
}
