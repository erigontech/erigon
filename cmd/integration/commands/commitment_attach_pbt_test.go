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
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestValidatePBTAttachFilesRequiresBothCommitmentDomains(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, validatePBTAttachFiles(node, published, 8, 7))
	require.NoError(t, validatePBTAttachFiles(node, published, 8, 6))
	require.ErrorContains(t, validatePBTAttachFiles(node, published, 8, 8), "do not cover conversion txNum")
	require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")))
	require.ErrorContains(t, validatePBTAttachFiles(node, published, 8, 7), "commitment-bin")
}

func TestPBTAttachVisibleFilesPrefersMergedRanges(t *testing.T) {
	files := []pbtAttachFile{
		{domain: kv.AccountsDomain, from: 0, to: 1, path: "v2.0-accounts.0-1.kv", data: true},
		{domain: kv.AccountsDomain, from: 1, to: 2, path: "v2.0-accounts.1-2.kv", data: true},
		{domain: kv.AccountsDomain, from: 0, to: 2, path: "v2.0-accounts.0-2.kv", data: true},
		{domain: kv.AccountsDomain, from: 0, to: 1, path: "v2.0-accounts.0-1.bt"},
		{domain: kv.AccountsDomain, from: 1, to: 2, path: "v2.0-accounts.1-2.bt"},
		{domain: kv.AccountsDomain, from: 0, to: 2, path: "v2.0-accounts.0-2.bt"},
	}
	visible := pbtAttachVisibleFiles(files)
	require.Len(t, visible, 2)
	require.ElementsMatch(t, []string{"v2.0-accounts.0-2.kv", "v2.0-accounts.0-2.bt"}, pbtAttachFilePaths(visible))
}

func pbtAttachFilePaths(files []pbtAttachFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.path)
	}
	return paths
}

func TestValidatePBTAttachSaltsIgnoresBlockSalt(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, os.WriteFile(filepath.Join(node.Snap, "salt-state.txt"), []byte{1}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(published.Snap, "salt-state.txt"), []byte{1}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.Snap, "salt-blocks.txt"), []byte{2}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(published.Snap, "salt-blocks.txt"), []byte{3}, 0o644))
	require.NoError(t, validatePBTAttachSalts(node, published))
}

func TestAttachPBTUsesLegacyStepSizeWithoutNodeSettings(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	hash := commitment.PBinHashBlake3
	variant := state.TrieVariantHexBin
	refs := false
	blockNum, txNum := uint64(2), uint64(7)
	require.NoError(t, state.WriteErigonDBSettings(published, &state.ErigonDBSettings{
		StepSize:                       config3.LegacyStepSize,
		StepsInFrozenFile:              config3.LegacyStepsInFrozenFile,
		ReferencesInCommitmentBranches: &refs,
		TrieVariant:                    &variant,
		TrieHash:                       &hash,
		ConversionBlockNum:             &blockNum,
		ConversionTxNum:                &txNum,
	}))
	require.NoError(t, os.WriteFile(filepath.Join(node.Snap, "salt-state.txt"), []byte{1}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(published.Snap, "salt-state.txt"), []byte{1}, 0o644))
	previousHash := statecfg.BinCommitmentHash
	statecfg.BinCommitmentHash = hash
	t.Cleanup(func() { statecfg.BinCommitmentHash = previousHash })
	before := snapshotTree(t, node.DataDir)
	err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
	require.ErrorContains(t, err, "step size")
	require.Equal(t, before, snapshotTree(t, node.DataDir))
}

func TestValidatePBTAttachFilesRejectsUncutStateHistory(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, os.MkdirAll(node.SnapHistory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-accounts.0-2.v"), []byte("history"), 0o644))
	require.ErrorContains(t, validatePBTAttachFiles(node, published, 8, 7), "spans conversion txNum 7")
}

func TestValidatePBTAttachFilesRejectsHistoryBelowStateFrontier(t *testing.T) {
	files := []pbtAttachFile{
		{path: "accounts.0-2.kv", domain: kv.AccountsDomain, from: 0, to: 2, data: true},
		{path: "accounts.0-1.v", domain: kv.AccountsDomain, from: 0, to: 1},
	}
	err := validatePBTAttachHistoryFrontier(files, 8, 10)
	require.ErrorContains(t, err, "accounts.0-1.v")
	require.ErrorContains(t, err, "ends at txNum 8")
}

func TestAttachPBTRejectsHistorySpanningPointWithoutMutation(t *testing.T) {
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	require.NoError(t, os.MkdirAll(source.SnapHistory, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(source.SnapHistory, "v1.0-accounts.0-2.v"), []byte("history"), 0o644))
	setExecutionProgress(t, source.Chaindata, 1)
	before := snapshotTree(t, source.DataDir)
	err := attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "spans conversion txNum 7")
	require.Equal(t, before, snapshotTree(t, source.DataDir))
}

func TestAdoptPBTFilesReplacesStateAndCommitmentFiles(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapDomain, "v1.0-accounts.1-2.kv"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapDomain, "v1.0-receipt.2-3.kv"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-receipt.2-3.v"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-rcache.2-3.v"), []byte("past"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapIdx, "v1.0-logtopics.2-3.ef"), []byte("past"), 0o644))
	require.NoError(t, adoptPBTFiles(node, published, 8, 7))
	_, err := os.Stat(filepath.Join(node.SnapDomain, "v1.0-accounts.1-2.kv"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapDomain, "v1.0-receipt.2-3.kv"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapHistory, "v1.0-receipt.2-3.v"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapHistory, "v1.0-rcache.2-3.v"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(node.SnapIdx, "v1.0-logtopics.2-3.ef"))
	require.ErrorIs(t, err, os.ErrNotExist)
	for _, name := range []string{
		"v1.0-accounts.0-1.kv",
		"v1.0-storage.0-1.kv",
		"v1.0-code.0-1.kv",
		"v1.0-commitment.0-1.kv",
		"v1.0-commitment-bin.0-1.kv",
	} {
		got, readErr := os.ReadFile(filepath.Join(node.SnapDomain, name))
		require.NoError(t, readErr)
		require.Equal(t, []byte("published"), got)
	}
}

func TestConfiguredPBTNodeHashUsesPersistedSuite(t *testing.T) {
	hash := commitment.PBinHashBlake3
	variant := state.TrieVariantHexBin
	settings := &state.ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash}
	require.Equal(t, hash, configuredPBTNodeHash(settings))
}

func TestAttachPBTResetsAndReopensAtPublishedRoot(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousHash := statecfg.BinCommitmentHash
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.BinCommitmentHash = previousHash
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, wantRoot := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	corruptPBTBinTable(t, source.Chaindata)
	require.NoError(t, attachPBT(t.Context(), source.DataDir, published, "", log.New()))
	settings, err := state.ReadErigonDBSettings(datadir.Open(source.DataDir))
	require.NoError(t, err)
	require.Equal(t, state.TrieVariantHexBin, settings.TrieVariantName())
	require.Zero(t, readExecutionStageProgress(t, source.Chaindata))
	genesisDB := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
	var genesisHash common.Hash
	require.NoError(t, genesisDB.View(t.Context(), func(tx kv.Tx) error {
		var err error
		genesisHash, err = rawdb.ReadCanonicalHash(tx, 0)
		return err
	}))
	require.NotEqual(t, [32]byte{}, genesisHash)
	genesisDB.Close()
	root, _ := reopenBinSource(t, source.DataDir, source.Chaindata)
	require.Equal(t, wantRoot[:], root)
}

func TestAttachPBTRejectsDifferentStateSalt(t *testing.T) {
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	require.NoError(t, os.WriteFile(filepath.Join(source.Snap, "salt-state.txt"), []byte{9, 9, 9, 9}, 0o644))
	publishedDirs := datadir.Open(published)
	require.NoError(t, dir.RemoveFile(filepath.Join(publishedDirs.Snap, "salt-state.txt")))
	require.NoError(t, os.WriteFile(filepath.Join(publishedDirs.Snap, "salt-state.txt"), []byte{8, 8, 8, 8}, 0o644))
	err := attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "salt-state.txt")
	require.ErrorContains(t, err, "node=09090909")
}

func TestAttachPBTRemovesOutputSettingsRefusalCases(t *testing.T) {
	t.Run("missing settings", func(t *testing.T) {
		node := datadir.New(t.TempDir())
		published := datadir.New(t.TempDir())
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "no erigondb.toml")
	})
	t.Run("missing node settings leaves datadir unchanged", func(t *testing.T) {
		previousHash := statecfg.BinCommitmentHash
		t.Cleanup(func() { statecfg.BinCommitmentHash = previousHash })
		statecfg.BinCommitmentHash = commitment.PBinHashBlake3
		node, published := newPBTAttachFileTrees(t, true)
		refs := false
		variant := state.TrieVariantHexBin
		hash := commitment.PBinHashKeccak
		require.NoError(t, state.WriteErigonDBSettings(published, &state.ErigonDBSettings{
			StepSize: config3.DefaultStepSize, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
			TrieVariant: &variant, TrieHash: &hash, ConversionBlockNum: uint64Ptr(1), ConversionTxNum: uint64Ptr(7),
		}))
		before := snapshotTree(t, node.DataDir)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "trie_hash")
		require.Equal(t, before, snapshotTree(t, node.DataDir))
	})
	t.Run("history spanning conversion point", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, os.MkdirAll(node.SnapHistory, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(node.SnapHistory, "v1.0-accounts.0-2.v"), []byte("history"), 0o644))
		before := snapshotTree(t, node.DataDir)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "spans conversion txNum 7")
		require.Equal(t, before, snapshotTree(t, node.DataDir))
	})
	t.Run("hash", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashKeccak, 1, 7)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "trie_hash")
	})
	t.Run("variant", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		refs := false
		variant := state.TrieVariantHex
		require.NoError(t, state.WriteErigonDBSettings(published, &state.ErigonDBSettings{StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs, TrieVariant: &variant, ConversionBlockNum: uint64Ptr(1), ConversionTxNum: uint64Ptr(7)}))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "hex+bin")
	})
	t.Run("ranges", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-accounts.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "accounts")
	})
	t.Run("conversion point", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 9)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "do not cover conversion txNum")
	})
	t.Run("published bin range", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, false)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		oldPath := filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")
		newPath := filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-0.kv")
		require.NoError(t, os.Rename(oldPath, newPath))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment-bin ranges do not match commitment ranges")
	})
	t.Run("missing bin", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment-bin")
	})
	t.Run("missing hex", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment")
	})
	t.Run("node behind", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 7)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		require.NoError(t, os.MkdirAll(node.Chaindata, 0o755))
		rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(node.Chaindata).MustOpen()
		rawDB.Close()
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "behind conversion block")
	})
}

func TestAttachPBTRetryAfterEachInterruptedStep(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	previousHook := attachPBTStepHook
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
		attachPBTStepHook = previousHook
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	for _, step := range []string{"marker", "swap", "reset", "settings"} {
		t.Run(step, func(t *testing.T) {
			source, _ := newPBTConversionSource(t)
			published := filepath.Join(t.TempDir(), "published")
			require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
			setExecutionProgress(t, source.Chaindata, 1)
			attachPBTStepHook = func(current string) error {
				if current == step {
					return errors.New("interrupted attach")
				}
				return nil
			}
			require.ErrorContains(t, attachPBT(t.Context(), source.DataDir, published, "", log.New()), "interrupted attach")
			marker, err := state.ReadPBTAttachMarker(datadir.Open(source.DataDir))
			require.NoError(t, err)
			require.NotNil(t, marker)
			require.ErrorContains(t, state.RefusePBTAttachMarker(datadir.Open(source.DataDir)), "rerun attach-pbt")
			attachPBTStepHook = nil
			require.NoError(t, attachPBT(t.Context(), source.DataDir, published, "", log.New()))
			marker, err = state.ReadPBTAttachMarker(datadir.Open(source.DataDir))
			require.NoError(t, err)
			require.Nil(t, marker)
		})
	}
}

func TestAttachPBTRetryAfterPartialSwap(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	previousHook := attachPBTStepHook
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
		attachPBTStepHook = previousHook
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	attachPBTStepHook = func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}
	require.ErrorContains(t, attachPBT(t.Context(), source.DataDir, published, "", log.New()), "interrupted attach")
	files, err := pbtAttachFiles(datadir.Open(source.DataDir))
	require.NoError(t, err)
	for _, file := range files {
		if file.domain == kv.AccountsDomain && file.data {
			require.NoError(t, dir.RemoveFile(file.path))
			break
		}
	}
	attachPBTStepHook = nil
	require.NoError(t, attachPBT(t.Context(), source.DataDir, published, "", log.New()))
}

func TestAttachPBTRetryAfterPartialSettingsWrite(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	previousHook := attachPBTStepHook
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
		attachPBTStepHook = previousHook
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	attachPBTStepHook = func(step string) error {
		if step == "reset" {
			return errors.New("interrupted attach")
		}
		return nil
	}
	require.ErrorContains(t, attachPBT(t.Context(), source.DataDir, published, "", log.New()), "interrupted attach")
	require.NoError(t, os.WriteFile(filepath.Join(source.Snap, state.ERIGONDB_SETTINGS_FILE), []byte("trie_variant = \""), 0o644))
	attachPBTStepHook = nil
	require.NoError(t, attachPBT(t.Context(), source.DataDir, published, "", log.New()))
}

func TestAttachPBTRejectsConversionPointBeyondPublishedCheckpoint(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	previousHook := attachPBTStepHook
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
		attachPBTStepHook = previousHook
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	settings, err := state.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionTx := uint64(16)
	settings.ConversionTxNum = &conversionTx
	require.NoError(t, state.WriteErigonDBSettings(datadir.Open(published), settings))
	setExecutionProgress(t, source.Chaindata, 2)
	err = attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "ends at txNum 8 before conversion txNum 16")
}

func TestAttachPBTRejectsTruncatedPublishedAccountsFile(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	files, err := pbtAttachFiles(datadir.Open(published))
	require.NoError(t, err)
	for _, file := range files {
		if file.domain == kv.AccountsDomain && file.data {
			require.NoError(t, os.Truncate(file.path, 1))
			break
		}
	}
	settings, err := state.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	blockNum, txNum, ok, err := settings.ConversionPoint()
	require.NoError(t, err)
	require.True(t, ok)
	require.ErrorContains(t, validatePBTAttachPublishedPoint(t.Context(), datadir.Open(published), settings, blockNum, txNum, log.New()), "accounts")
	setExecutionProgress(t, source.Chaindata, 1)
	err = attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "accounts")

	recoverySource, _ := newPBTConversionSource(t)
	recoveryPublished := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), recoverySource.DataDir, recoveryPublished, true, "", log.New()))
	setExecutionProgress(t, recoverySource.Chaindata, 1)
	attachPBTStepHook = func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}
	require.ErrorContains(t, attachPBT(t.Context(), recoverySource.DataDir, recoveryPublished, "", log.New()), "interrupted attach")
	recoveryFiles, err := pbtAttachFiles(datadir.Open(recoveryPublished))
	require.NoError(t, err)
	for _, file := range recoveryFiles {
		if file.domain == kv.AccountsDomain && file.data {
			require.NoError(t, os.Truncate(file.path, 1))
			break
		}
	}
	attachPBTStepHook = nil
	require.ErrorContains(t, attachPBT(t.Context(), recoverySource.DataDir, recoveryPublished, "", log.New()), "accounts")
}

func TestAttachPBTRejectsPublishedLeafAfterConversion(t *testing.T) {
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	renamePBTFilesRange(t, source.Snap, "0-1", "0-2")
	renamePBTAllFilesRange(t, published, "0-1", "0-2")
	setExecutionProgress(t, source.Chaindata, 1)
	validatePBTAttachLeafStampsFn = validatePBTAttachLeafStamps
	before := snapshotTree(t, source.DataDir)
	err := attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "published leaf stamp 15")
	require.Equal(t, before, snapshotTree(t, source.DataDir))
}

func renamePBTFilesRange(t *testing.T, root, from, to string) {
	renamePBTFilesRangeWithFilter(t, root, from, to, true)
}

func renamePBTAllFilesRange(t *testing.T, root, from, to string) {
	renamePBTFilesRangeWithFilter(t, root, from, to, false)
}

func renamePBTFilesRangeWithFilter(t *testing.T, root, from, to string, adoptedOnly bool) {
	t.Helper()
	var paths []string
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.Contains(entry.Name(), "."+from+".") {
			return err
		}
		ext := filepath.Ext(entry.Name())
		if adoptedOnly && ext != ".kv" && ext != ".bt" && ext != ".kvi" && ext != ".kvei" {
			return err
		}
		paths = append(paths, path)
		return nil
	}))
	for _, path := range paths {
		name := strings.Replace(filepath.Base(path), "."+from+".", "."+to+".", 1)
		require.NoError(t, os.Rename(path, filepath.Join(filepath.Dir(path), name)))
	}
}

func writePBTAttachSettings(t *testing.T, dirs datadir.Dirs, hash string, blockNum, txNum uint64) {
	t.Helper()
	refs := false
	variant := state.TrieVariantHexBin
	require.NoError(t, state.WriteErigonDBSettings(dirs, &state.ErigonDBSettings{
		StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs,
		TrieVariant: &variant, TrieHash: &hash, ConversionBlockNum: uint64Ptr(blockNum), ConversionTxNum: uint64Ptr(txNum),
	}))
}

func uint64Ptr(value uint64) *uint64 { return &value }

func corruptPBTBinTable(t *testing.T, rawPath string) {
	t.Helper()
	db := dbCfg(dbcfg.ChainDB, rawPath).MustOpen()
	defer db.Close()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.TblCommitmentBinVals, []byte{0}, []byte{1})
	}))
}

func newPBTAttachFileTrees(t *testing.T, both bool) (datadir.Dirs, datadir.Dirs) {
	t.Helper()
	node := datadir.New(t.TempDir())
	published := datadir.New(t.TempDir())
	for _, dirs := range []*datadir.Dirs{&node, &published} {
		require.NoError(t, os.MkdirAll(dirs.SnapDomain, 0o755))
		for _, name := range []string{
			"v1.0-accounts.0-1.kv",
			"v1.0-storage.0-1.kv",
			"v1.0-code.0-1.kv",
			"v1.0-commitment.0-1.kv",
		} {
			require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, name), []byte("node"), 0o644))
		}
		if both {
			require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v1.0-commitment-bin.0-1.kv"), []byte("node"), 0o644))
		}
	}
	for _, name := range []string{
		"v1.0-accounts.0-1.kv",
		"v1.0-storage.0-1.kv",
		"v1.0-code.0-1.kv",
		"v1.0-commitment.0-1.kv",
		"v1.0-commitment-bin.0-1.kv",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(published.SnapDomain, name), []byte("published"), 0o644))
	}
	return node, published
}
