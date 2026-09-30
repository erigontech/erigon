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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func TestValidatePBTAttachFilesRequiresBothCommitmentDomains(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, validatePBTAttachFiles(node, published, 8, 8))
	require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")))
	require.ErrorContains(t, validatePBTAttachFiles(node, published, 8, 8), "commitment-bin")
}

func TestAdoptPBTFilesReplacesStateAndCommitmentFiles(t *testing.T) {
	node, published := newPBTAttachFileTrees(t, true)
	require.NoError(t, os.WriteFile(filepath.Join(node.SnapDomain, "v1.0-accounts.1-2.kv"), []byte("past"), 0o644))
	require.NoError(t, adoptPBTFiles(node, published, 8, 8))
	_, err := os.Stat(filepath.Join(node.SnapDomain, "v1.0-accounts.1-2.kv"))
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

func TestAttachPBTRemovesOutputSettingsRefusalCases(t *testing.T) {
	t.Run("missing settings", func(t *testing.T) {
		node := datadir.New(t.TempDir())
		published := datadir.New(t.TempDir())
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "no erigondb.toml")
	})
	t.Run("hash", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		writePBTAttachSettings(t, published, commitment.PBinHashKeccak, 1, 8)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "trie_hash")
	})
	t.Run("variant", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		refs := false
		variant := state.TrieVariantHex
		require.NoError(t, state.WriteErigonDBSettings(published, &state.ErigonDBSettings{StepSize: 8, StepsInFrozenFile: 1, ReferencesInCommitmentBranches: &refs, TrieVariant: &variant, ConversionBlockNum: uint64Ptr(1), ConversionTxNum: uint64Ptr(8)}))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "hex+bin")
	})
	t.Run("ranges", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 8)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-accounts.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "accounts")
	})
	t.Run("conversion point", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 7)
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "extends past conversion txNum")
	})
	t.Run("published bin range", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, false)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 8)
		oldPath := filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")
		newPath := filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-0.kv")
		require.NoError(t, os.Rename(oldPath, newPath))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment-bin ranges do not match commitment ranges")
	})
	t.Run("missing bin", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 8)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment-bin.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment-bin")
	})
	t.Run("missing hex", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 8)
		require.NoError(t, dir.RemoveFile(filepath.Join(published.SnapDomain, "v1.0-commitment.0-1.kv")))
		err := attachPBT(t.Context(), node.DataDir, published.DataDir, "", log.New())
		require.ErrorContains(t, err, "commitment")
	})
	t.Run("node behind", func(t *testing.T) {
		node, published := newPBTAttachFileTrees(t, true)
		writePBTAttachSettings(t, node, commitment.PBinHashBlake3, 1, 8)
		writePBTAttachSettings(t, published, commitment.PBinHashBlake3, 1, 8)
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

func TestAttachPBTRejectsConversionPointBeyondPublishedCheckpoint(t *testing.T) {
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
	settings, err := state.ReadErigonDBSettings(datadir.Open(published))
	require.NoError(t, err)
	conversionTx := uint64(16)
	settings.ConversionTxNum = &conversionTx
	require.NoError(t, state.WriteErigonDBSettings(datadir.Open(published), settings))
	setExecutionProgress(t, source.Chaindata, 2)
	err = attachPBT(t.Context(), source.DataDir, published, "", log.New())
	require.ErrorContains(t, err, "do not end at conversion txNum")
}

func TestAttachPBTAllowsMidBlockConversionPoint(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.MkdirAll(dirs.Chaindata, 0o755))
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := rawdbv3.TxNums.Append(tx, 0, 0); err != nil {
			return err
		}
		if err := rawdbv3.TxNums.Append(tx, 1, 10); err != nil {
			return err
		}
		return stages.SaveStageProgress(tx, stages.Execution, 1)
	}))
	require.NoError(t, checkPBTNodePosition(t.Context(), db, 1, 8))
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
