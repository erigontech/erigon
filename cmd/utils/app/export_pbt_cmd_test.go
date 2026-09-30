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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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
	require.NoError(t, artifact.JoinAt(bytes.NewReader(snapshotBytes), int64(len(snapshotBytes)), bytes.NewReader(preimages), int64(len(preimages)), eip8297.HashBytes, nil))
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

func TestRunExportPBTUsesStoppedExecutionStage(t *testing.T) {
	selectPBTExportSuite(t)
	db, root := newPBTExportDB(t)
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	progress, err := stages.GetStageProgress(tx, stages.Execution)
	require.NoError(t, err)
	require.Equal(t, uint64(7), progress)
	outDir := filepath.Join(t.TempDir(), "export")
	require.NoError(t, runExportPBT(t.Context(), tx, func(blockNum uint64) (*types.Header, error) {
		require.Equal(t, uint64(7), blockNum)
		return &types.Header{Root: root, Time: 10}, nil
	}, outDir, log.New()))
}

func TestRunExportPBTReplayMatchesConvertedState(t *testing.T) {
	selectPBTExportSuite(t)
	firstDB, firstRoot := newPBTExportDB(t)
	secondDB, secondRoot := newPBTExportDB(t)
	firstTx, err := firstDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer firstTx.Rollback()
	secondTx, err := secondDB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer secondTx.Rollback()
	firstDir := filepath.Join(t.TempDir(), "first")
	secondDir := filepath.Join(t.TempDir(), "second")
	require.NoError(t, runExportPBT(t.Context(), firstTx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: firstRoot, Time: 10}, nil
	}, firstDir, log.New()))
	require.NoError(t, runExportPBT(t.Context(), secondTx, func(uint64) (*types.Header, error) {
		return &types.Header{Root: secondRoot, Time: 10}, nil
	}, secondDir, log.New()))
	firstMeta, err := os.ReadFile(filepath.Join(firstDir, pbtMetaFileName))
	require.NoError(t, err)
	secondMeta, err := os.ReadFile(filepath.Join(secondDir, pbtMetaFileName))
	require.NoError(t, err)
	var first, second pbtExportMeta
	require.NoError(t, json.Unmarshal(firstMeta, &first))
	require.NoError(t, json.Unmarshal(secondMeta, &second))
	require.Equal(t, first.SnapshotDigest, second.SnapshotDigest)
}

func selectPBTExportSuite(t *testing.T) {
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
		account := accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, tx, address, accounts.SerialiseV3(&account), 1, nil))
		binCtx.SetPBinFeed(&commitment.PBinFeed{Accounts: []commitment.PBinFeedAccount{{
			Address: address, Exists: true, CodeWritten: true, Balance: *uint256.NewInt(1), CodeHash: common.Hash(empty.CodeHash),
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
