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
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
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
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestImportPBTUsesOnlySnapshotInput(t *testing.T) {
	require.NotNil(t, cmdCommitmentImportPBT.Flags().Lookup("snapshot"))
	require.Nil(t, cmdCommitmentImportPBT.Flags().Lookup("preimages"), "import-pbt must not require preimages")
	require.Nil(t, cmdCommitmentImportPBT.Flags().Lookup("block"), "import-pbt must not require a block hash")
}

func TestImportPBTReplacesConvertAndAttach(t *testing.T) {
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	sourceDB := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(source.Tester.Dirs),
		execmoduletester.WithGenesisSpec(source.Genesis),
		execmoduletester.WithKey(source.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := sourceDB.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()
	sourceDB.Close()
	converted := filepath.Join(t.TempDir(), "converted")
	require.NoError(t, convertPBT(t.Context(), source.Tester.Dirs.DataDir, converted, true, "", log.New()))
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))

	target, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, target.Tester.InsertChain(target.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), target.Tester.DB))
	require.NoError(t, target.Tester.ReExecuteTo(t.Context(), 2))
	buildPBTAcceptanceFilesAt(t, target, 7)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	before := snapshotTree(t, target.Tester.Dirs.DataDir)
	target.Tester.Close()
	t.Cleanup(func() { importPBTSwapHook = nil })
	importPBTSwapHook = func(step string) error {
		if step == "staging-built" {
			return errors.New("test staging failure")
		}
		return nil
	}
	require.ErrorContains(t, importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New()), "test staging failure")
	require.Equal(t, before, snapshotTree(t, target.Tester.Dirs.DataDir), "staging failure must not change the target")
	importPBTSwapHook = func(step string) error {
		if step == "files-moved" {
			return errors.New("test move failure")
		}
		return nil
	}
	settingsBeforeMoveFailure := snapshotTree(t, target.Tester.Dirs.DataDir)
	require.ErrorContains(t, importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New()), "test move failure")
	require.Equal(t, settingsBeforeMoveFailure, snapshotTree(t, target.Tester.Dirs.DataDir), "move failure must not change the target")
	importPBTSwapHook = nil
	require.NoError(t, importPBT(t.Context(), target.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New()))
	after := snapshotTree(t, target.Tester.Dirs.DataDir)
	require.NotEqual(t, before, after)
	for path, value := range before {
		if strings.Contains(path, "accounts") || strings.Contains(path, "storage") || strings.Contains(path, "code") {
			require.Equal(t, value, after[path], "import must not rewrite state-domain file %s", path)
		}
	}
	settings, err := dbstate.ReadErigonDBSettings(target.Tester.Dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
	require.Equal(t, commitment.PBinHashBlake3, settings.TrieHashName())
	gotBlock, gotTx := readPBTImportCheckpoint(t, target.Tester.Dirs.DataDir)
	require.Equal(t, uint64(2), gotBlock, "import must write the bin checkpoint block")
	require.Equal(t, uint64(7), gotTx, "import must write the bin checkpoint txNum")
	binFiles, err := filepath.Glob(filepath.Join(target.Tester.Dirs.SnapDomain, "*-commitment-bin.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, binFiles, "import must write commitment-bin files")
	require.Equal(t, readPBTFilesRoot(t, converted), readPBTFilesRoot(t, target.Tester.Dirs.DataDir), "import rows must equal convert-pbt rows")

	selectPBTCommandSuite(t)
	dual, err := execmoduletester.NewPBTAcceptanceChain(t, false, true)
	require.NoError(t, err)
	require.NoError(t, dual.Tester.InsertChain(dual.Chain))
	reopened := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(target.Tester.Dirs),
		execmoduletester.WithGenesisSpec(target.Genesis),
		execmoduletester.WithKey(target.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
		execmoduletester.WithEnableDomain(kv.CommitmentBinDomain),
	)
	importedRaw := reopened.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	dualRaw := dual.Tester.DB.(interface{ InternalDB() kv.RwDB }).InternalDB()
	for block := uint64(3); block <= target.Chain.TopBlock.NumberU64(); block++ {
		require.NoError(t, reopened.ReExecuteTo(t.Context(), block))
		var importedRoot, dualRoot []byte
		require.NoError(t, importedRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			importedRoot, err = rawdb.ReadShadowStateRoot(tx, target.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.NoError(t, dualRaw.View(t.Context(), func(tx kv.Tx) error {
			var err error
			dualRoot, err = rawdb.ReadShadowStateRoot(tx, dual.Chain.Blocks[block-1].Hash(), block)
			return err
		}))
		require.Equal(t, dualRoot, importedRoot, "imported bin shadow at block %d", block)
	}
	reopened.Close()
}

func TestImportPBTRefusalsLeaveDatadirUnchanged(t *testing.T) {
	selectPBTHexCommandSuite(t)
	fixture := newPBTImportFixture(t)
	originalMeta, err := os.ReadFile(fixture.metaPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, os.WriteFile(fixture.metaPath, originalMeta, 0o644)) }()

	attempt := func(name string, mutate func(*pbtImportMeta), want string) {
		t.Run(name, func(t *testing.T) {
			var meta pbtImportMeta
			require.NoError(t, json.Unmarshal(originalMeta, &meta))
			mutate(&meta)
			data, marshalErr := json.Marshal(meta)
			require.NoError(t, marshalErr)
			require.NoError(t, os.WriteFile(fixture.metaPath, data, 0o644))
			before := snapshotTree(t, fixture.dataDir)
			err := importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New())
			if want == "" {
				require.Error(t, err)
			} else {
				require.ErrorContains(t, err, want)
			}
			require.Equal(t, before, snapshotTree(t, fixture.dataDir), "refusal must leave the datadir unchanged")
			require.NoError(t, os.WriteFile(fixture.metaPath, originalMeta, 0o644))
		})
	}

	attempt("moved past block", func(meta *pbtImportMeta) {
		setImportExecutionProgress(t, fixture.dataDir, meta.Block+1)
	}, "target is at block")
	setImportExecutionProgress(t, fixture.dataDir, 2)
	attempt("mid-block checkpoint", func(meta *pbtImportMeta) { meta.TxNum-- }, "not the block end")
	attempt("wrong block hash", func(meta *pbtImportMeta) { meta.BlockHash = common.Hash{0xaa}.Hex() }, "")
	attempt("wrong txNum", func(meta *pbtImportMeta) { meta.TxNum++ }, "not the block end")
	attempt("wrong chain id", func(meta *pbtImportMeta) { meta.ChainID = "999999" }, "chain id")
	attempt("digest mismatch", func(meta *pbtImportMeta) { meta.SnapshotDigest = common.Hash{0xbb}.Hex() }, "snapshot digest")

	t.Run("leaf root mismatch", func(t *testing.T) {
		originalSnapshot, readErr := os.ReadFile(fixture.snapshot)
		require.NoError(t, readErr)
		data := append([]byte(nil), originalSnapshot...)
		data[len(data)-34] ^= 1
		require.NoError(t, os.WriteFile(fixture.snapshot, data, 0o644))
		hash := keccak.NewFastKeccak()
		_, requireErr := hash.Write(data)
		require.NoError(t, requireErr)
		var meta pbtImportMeta
		require.NoError(t, json.Unmarshal(originalMeta, &meta))
		meta.SnapshotDigest = common.BytesToHash(hash.Sum(nil)).Hex()
		metaData, marshalErr := json.Marshal(meta)
		require.NoError(t, marshalErr)
		require.NoError(t, os.WriteFile(fixture.metaPath, metaData, 0o644))
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "root")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		require.NoError(t, os.WriteFile(fixture.snapshot, originalSnapshot, 0o644))
		require.NoError(t, os.WriteFile(fixture.metaPath, originalMeta, 0o644))
	})

	t.Run("target already hex and bin", func(t *testing.T) {
		dirs := datadir.Open(fixture.dataDir)
		settings, settingsErr := dbstate.ReadErigonDBSettings(dirs)
		require.NoError(t, settingsErr)
		originalSettings, readErr := os.ReadFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE))
		require.NoError(t, readErr)
		variant := dbstate.TrieVariantHexBin
		settings.TrieVariant = &variant
		require.NoError(t, dbstate.WriteErigonDBSettings(dirs, settings))
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "hex-only")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE), originalSettings, 0o644))
	})

	t.Run("hash suite mismatch", func(t *testing.T) {
		statecfg.BinCommitmentHash = commitment.PBinHashKeccak
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "hash suite")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	})

	t.Run("frozen target", func(t *testing.T) {
		dirs := datadir.Open(fixture.dataDir)
		settings, settingsErr := dbstate.ReadErigonDBSettings(dirs)
		require.NoError(t, settingsErr)
		originalSettings, readErr := os.ReadFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE))
		require.NoError(t, readErr)
		settings.FrozenAtTxNum = map[string]uint64{kv.CommitmentDomain.String(): 7}
		require.NoError(t, dbstate.WriteErigonDBSettings(dirs, settings))
		before := snapshotTree(t, fixture.dataDir)
		require.ErrorContains(t, importPBT(t.Context(), fixture.dataDir, fixture.snapshot, "", log.New()), "frozen")
		require.Equal(t, before, snapshotTree(t, fixture.dataDir))
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, dbstate.ERIGONDB_SETTINGS_FILE), originalSettings, 0o644))
	})
}

func TestImportPBTUsesFrozenBlockFiles(t *testing.T) {
	selectPBTHexCommandSuite(t)
	chain, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, chain.Tester.InsertChain(chain.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, chain)
	sourceDB := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(chain.Tester.Dirs),
		execmoduletester.WithGenesisSpec(chain.Genesis),
		execmoduletester.WithKey(chain.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := sourceDB.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return chain.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()

	config := snapcfg.KnownCfgOrDevnet(chain.Tester.ChainConfig.ChainName)
	sourceDB.Close()
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	archive, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, archive.Tester.InsertChain(archive.Chain))
	require.NoError(t, freezeblocks.DumpBlocks(t.Context(), 0, 3, archive.Tester.ChainConfig, archive.Tester.Dirs.Tmp, archive.Tester.Dirs.Snap, archive.Tester.DB, 1, log.LvlInfo, log.New(), archive.Tester.BlockReader, config, nil))
	archive.Tester.Close()
	copyPBTBlockSnapshotFiles(t, archive.Tester.Dirs, chain.Tester.Dirs)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	block := chain.Chain.Blocks[1]
	rawDB := dbCfg(dbcfg.ChainDB, chain.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		rawdb.DeleteHeader(tx, block.Hash(), block.NumberU64())
		rawdb.DeleteBody(tx, block.Hash(), block.NumberU64())
		if err := rawdb.TruncateCanonicalHash(tx, block.NumberU64(), false); err != nil {
			return err
		}
		return rawdbv3.TxNums.Truncate(tx, block.NumberU64())
	}))
	rawDB.Close()
	setImportExecutionProgress(t, chain.Tester.Dirs.DataDir, block.NumberU64())
	require.NoError(t, importPBT(t.Context(), chain.Tester.Dirs.DataDir, filepath.Join(output, "pbt-snapshot.bin"), "", log.New()))
	settings, err := dbstate.ReadErigonDBSettings(chain.Tester.Dirs)
	require.NoError(t, err)
	require.Equal(t, dbstate.TrieVariantHexBin, settings.TrieVariantName())
}

func copyPBTBlockSnapshotFiles(t *testing.T, source, target datadir.Dirs) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(source.Snap, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(source.Snap, path)
		if err != nil {
			return err
		}
		if filepath.Dir(rel) != "." || entry.Name() == dbstate.ERIGONDB_SETTINGS_FILE || strings.HasPrefix(entry.Name(), "salt-") {
			return nil
		}
		return os.Link(path, filepath.Join(target.Snap, entry.Name()))
	}))
}

type pbtImportFixture struct {
	dataDir  string
	snapshot string
	metaPath string
}

func newPBTImportFixture(t *testing.T) pbtImportFixture {
	t.Helper()
	selectPBTHexCommandSuite(t)
	source, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, source.Tester.InsertChain(source.Chain.Slice(0, 2)))
	buildPBTAcceptanceFiles(t, source)
	sourceDB := execmoduletester.New(t,
		execmoduletester.WithExistingDataDir(source.Tester.Dirs),
		execmoduletester.WithGenesisSpec(source.Genesis),
		execmoduletester.WithKey(source.Key),
		execmoduletester.WithStepSize(1),
		execmoduletester.WithoutGenesisCommit(),
	)
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	output := filepath.Join(t.TempDir(), "export")
	tx, err := sourceDB.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, app.RunExportPBT(t.Context(), tx, func(block uint64) (*types.Header, error) {
		return source.Chain.Headers[block-1], nil
	}, output, log.New()))
	tx.Rollback()
	sourceDB.Close()
	statecfg.BinCommitmentHash = ""
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))

	target, err := execmoduletester.NewPBTAcceptanceChain(t, false, false)
	require.NoError(t, err)
	require.NoError(t, target.Tester.InsertChain(target.Chain))
	require.NoError(t, rawdbreset.ResetExec(t.Context(), target.Tester.DB))
	require.NoError(t, target.Tester.ReExecuteTo(t.Context(), 2))
	buildPBTAcceptanceFilesAt(t, target, 7)
	target.Tester.Close()
	statecfg.BinCommitmentHash = commitment.PBinHashBlake3
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashBlake3))
	return pbtImportFixture{
		dataDir:  target.Tester.Dirs.DataDir,
		snapshot: filepath.Join(output, "pbt-snapshot.bin"),
		metaPath: filepath.Join(output, "pbt-snapshot.meta.json"),
	}
}

func setImportExecutionProgress(t *testing.T, dataDir string, progress uint64) {
	t.Helper()
	dirs := datadir.Open(dataDir)
	rawDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	require.NoError(t, rawDB.Update(t.Context(), func(tx kv.RwTx) error {
		return stages.SaveStageProgress(tx, stages.Execution, progress)
	}))
	rawDB.Close()
}

func readPBTImportCheckpoint(t *testing.T, dataDir string) (uint64, uint64) {
	t.Helper()
	dirs := datadir.Open(dataDir)
	resolved, err := dbstate.ResolveErigonDBSettings(dirs, log.New(), false)
	require.NoError(t, err)
	agg := dbstate.New(dirs).WithErigonDBSettings(resolved).Logger(log.New()).MustOpen(t.Context())
	require.NoError(t, agg.OpenFolder(nil))
	defer agg.Close()
	at := agg.BeginFilesRo()
	defer at.Close()
	value, found, _, _, err := at.DebugGetLatestFromFiles(kv.CommitmentBinDomain, commitment.KeyCommitmentState, ^uint64(0))
	require.NoError(t, err)
	require.True(t, found)
	tx, block := commitmentdb.DecodeTxBlockNums(value)
	return block, tx
}
