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
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	app "github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
	chainpkg "github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestValidatePBTImportPointRefusesNonCanonicalBlock(t *testing.T) {
	db, _ := temporal.Open(t, 8)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesis := common.Hash{1}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chainpkg.Config{BinaryTrieTime: new(uint64)}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 0))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 1))
	header := &types.Header{Number: *uint256.NewInt(1), Time: 0}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, common.Hash{2}, 1))
	require.NoError(t, tx.Commit())
	_, _, err = validatePBTImportPoint(t.Context(), db, header.Hash())
	require.ErrorContains(t, err, "not canonical")
}

func TestConfigureImportVariantBinDoesNotEnableV3Hex(t *testing.T) {
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
	dirs := datadir.New(t.TempDir())
	variant, hash := dbstate.TrieVariantBin, commitment.PBinHashBlake3
	require.NoError(t, dbstate.WriteErigonDBSettings(dirs, &dbstate.ErigonDBSettings{TrieVariant: &variant, TrieHash: &hash}))
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	configureImportVariant(dirs)
	require.True(t, statecfg.ExperimentalBinCommitment)
	require.False(t, statecfg.ExperimentalHexBinCommitment)
	require.False(t, statecfg.ExperimentalCommitmentV3, "bin-only import must not enable v3-hex")
}

func TestPBTImportRefusesFrozenTarget(t *testing.T) {
	settings := &dbstate.ErigonDBSettings{FrozenAtTxNum: map[string]uint64{kv.CommitmentDomain.String(): 13}}
	err := validatePBTImportTargetSettings(settings)
	require.ErrorContains(t, err, "commitment is frozen at txNum 13")
}

func TestPBTImportRefusesTargetFilesPastCheckpoint(t *testing.T) {
	files := kv.VisibleFiles{pbtImportVisibleFile{end: 8}}
	err := validatePBTImportFilesFrontier(kv.AccountsDomain, files, 1)
	require.ErrorContains(t, err, "accounts files extend past txNum 1")
}

type pbtImportVisibleFile struct {
	end uint64
}

func (f pbtImportVisibleFile) Fullpath() string         { return "accounts.0-1.kv" }
func (f pbtImportVisibleFile) StartRootNum() uint64     { return 0 }
func (f pbtImportVisibleFile) EndRootNum() uint64       { return f.end }
func (f pbtImportVisibleFile) Version() version.Version { return version.V2_0 }

func TestImportPBTValidatesArtifactsBeforeReset(t *testing.T) {
	previousDatadir := datadirCli
	previousChaindata := chaindata
	previousBin := statecfg.ExperimentalBinCommitment
	previousHexBin := statecfg.ExperimentalHexBinCommitment
	previousV3 := statecfg.ExperimentalCommitmentV3
	previousSchema := statecfg.Schema
	previousHash := statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		datadirCli = previousDatadir
		chaindata = previousChaindata
		statecfg.ExperimentalBinCommitment = previousBin
		statecfg.ExperimentalHexBinCommitment = previousHexBin
		statecfg.ExperimentalCommitmentV3 = previousV3
		statecfg.Schema = previousSchema
		statecfg.BinCommitmentHash = previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment = false
	statecfg.ExperimentalHexBinCommitment = false
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.BinCommitmentHash = ""
	statecfg.InitSchemas()
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	dirs := datadir.New(t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-state.txt"), []byte{1, 2, 3, 4}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, "salt-blocks.txt"), []byte("blocks"), 0o644))
	datadirCli = dirs.DataDir
	chaindata = dirs.Chaindata
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	tx, err := rawDB.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	genesis := common.Hash{9}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chainpkg.Config{BinaryTrieTime: new(uint64)}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 0, 0))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 1))
	header := &types.Header{Number: *uint256.NewInt(1), Root: eip8297.EmptyTreeHash}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1))
	require.NoError(t, tx.Commit())
	rawDB.Close()

	var snapshot bytes.Buffer
	_, err = artifact.WriteSnapshot(&snapshot, eip8297.EmptyTreeHash, func(func([]byte, []byte) error) error { return nil })
	require.NoError(t, err)
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimages(&preimages, []artifact.Preimage{{Address: common.Address{1}}}))
	snapshotPath := filepath.Join(t.TempDir(), "snapshot")
	preimagesPath := filepath.Join(t.TempDir(), "preimages")
	require.NoError(t, os.WriteFile(snapshotPath, snapshot.Bytes(), 0o644))
	require.NoError(t, os.WriteFile(preimagesPath, preimages.Bytes(), 0o644))
	err = importPBT(t.Context(), dirs.DataDir, snapshotPath, preimagesPath, header.Hash().Hex(), "", log.New())
	require.ErrorContains(t, err, "surplus key")
	require.Equal(t, uint64(1), readExecutionStageProgress(t, dirs.Chaindata), "invalid artifacts must not reset execution")
}

func TestImportPBTRefusalsLeaveDatadirUnchanged(t *testing.T) {
	selectPBTBinaryCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, true, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := source.Tester.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()
	source.Tester.Close()
	previousDatadir, previousChaindata := datadirCli, chaindata
	t.Cleanup(func() { datadirCli, chaindata = previousDatadir, previousChaindata })
	for _, test := range []struct {
		name   string
		frozen bool
	}{
		{name: "frozen", frozen: true},
		{name: "frontier"},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, targetErr := execmoduletester.NewPBTAcceptanceChain(t, true, false)
			require.NoError(t, targetErr)
			require.NoError(t, target.Tester.InsertChain(target.Chain))
			buildPBTAcceptanceFiles(t, target)
			settings, settingsErr := dbstate.ReadErigonDBSettings(target.Tester.Dirs)
			require.NoError(t, settingsErr)
			if test.frozen {
				settings.FrozenAtTxNum = map[string]uint64{kv.CommitmentDomain.String(): 13}
				require.NoError(t, dbstate.WriteErigonDBSettings(target.Tester.Dirs, settings))
			}
			target.Tester.Close()
			before := snapshotTree(t, target.Tester.Dirs.DataDir)
			datadirCli, chaindata = target.Tester.Dirs.DataDir, target.Tester.Dirs.Chaindata
			err := importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), filepath.Join(output, "framed.bin"), source.Chain.Blocks[1].Hash().Hex(), "", log.New())
			if test.frozen {
				require.ErrorContains(t, err, "commitment is frozen")
			} else {
				require.ErrorContains(t, err, "files extend past")
			}
			require.Equal(t, before, snapshotTree(t, target.Tester.Dirs.DataDir))
		})
	}
	for _, test := range []struct {
		name string
		hash common.Hash
		want string
	}{
		{name: "no-settings-unknown-block", hash: common.Hash{0xff}, want: "not in local chaindata"},
		{name: "no-settings-frontier", hash: source.Chain.Blocks[1].Hash(), want: "files extend past"},
	} {
		t.Run(test.name, func(t *testing.T) {
			target, targetErr := execmoduletester.NewPBTAcceptanceChain(t, true, false)
			require.NoError(t, targetErr)
			require.NoError(t, target.Tester.InsertChain(target.Chain))
			buildPBTAcceptanceFiles(t, target)
			settingsPath := filepath.Join(target.Tester.Dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE)
			require.NoError(t, os.Remove(settingsPath))
			target.Tester.Close()
			before := snapshotTree(t, target.Tester.Dirs.DataDir)
			datadirCli, chaindata = target.Tester.Dirs.DataDir, target.Tester.Dirs.Chaindata
			err := importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), filepath.Join(output, "framed.bin"), test.hash.Hex(), "", log.New())
			require.ErrorContains(t, err, test.want)
			require.Equal(t, before, snapshotTree(t, target.Tester.Dirs.DataDir))
			_, statErr := os.Stat(settingsPath)
			require.ErrorIs(t, statErr, os.ErrNotExist)
		})
	}
}
