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

package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestSegDuCommand(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	db.Close()

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	require.NoError(t, app.Run(t.Context(), []string{"capcli", "seg", "du", "--datadir", dirs.DataDir, "--chain", "mainnet"}))
	require.Contains(t, output.String(), "chain mainnet")
}

func TestCaplinRegimeCommandIsRegistered(t *testing.T) {
	var seg *cli.Command
	for _, command := range commands() {
		if command.Name == "seg" {
			seg = command
			break
		}
	}
	require.NotNil(t, seg)
	var regime *cli.Command
	for _, command := range seg.Commands {
		if command.Name == "regime" {
			regime = command
			break
		}
	}
	require.NotNil(t, regime)
	require.Len(t, regime.Commands, 2)
}

func TestSegResetCommand(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	block := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	replacement := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	blob := snaptype.BlobSidecars.FileName(version.V1_1, 200, 300)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, block), []byte(block), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, blob), []byte(blob), 0o600))
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte("'"+replacement+"' = 'replacement'\n"), 0o600))

	var dryOutput bytes.Buffer
	dryApp := &cli.Command{Name: "capcli", Commands: commands(), Writer: &dryOutput, ErrWriter: &dryOutput}
	require.NoError(t, dryApp.Run(t.Context(), []string{"capcli", "seg", "reset", "--datadir", dirs.DataDir, "--chain", "mainnet", "--dry-run"}))
	require.Contains(t, dryOutput.String(), "delete blocks "+block)
	require.Contains(t, dryOutput.String(), "only copy sidecars "+blob)
	require.FileExists(t, filepath.Join(dirs.Snap, block))
	require.FileExists(t, filepath.Join(dirs.Snap, blob))

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	err := app.Run(t.Context(), []string{"capcli", "seg", "reset", "--datadir", dirs.DataDir, "--chain", "mainnet"})
	require.ErrorContains(t, err, "only copy")
	require.FileExists(t, filepath.Join(dirs.Snap, block))
	require.FileExists(t, filepath.Join(dirs.Snap, blob))

	require.NoError(t, app.Run(t.Context(), []string{"capcli", "seg", "reset", "--datadir", dirs.DataDir, "--chain", "mainnet", "--force"}))
	require.NoFileExists(t, filepath.Join(dirs.Snap, block))
	require.NoFileExists(t, filepath.Join(dirs.Snap, blob))
	require.Contains(t, output.String(), "deleted blocks "+block)
}

func TestSegDuCommandManifest(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	db.Close()
	local := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	missing := snaptype.BeaconBlocks.FileName(version.V1_1, 100, 200)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, local), bytes.Repeat([]byte{'x'}, 11), 0o644))
	manifestPath := filepath.Join(t.TempDir(), "manifest.toml")
	require.NoError(t, os.WriteFile(manifestPath, []byte("'"+local+"' = 'local'\n'"+missing+"' = 'missing'\n"), 0o644))

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	require.NoError(t, app.Run(t.Context(), []string{"capcli", "seg", "du", "--datadir", dirs.DataDir, "--chain", "mainnet", "--manifest", manifestPath, "--json"}))
	var result map[string]any
	require.NoError(t, json.Unmarshal(output.Bytes(), &result))
	estimates := result["estimates"].([]any)
	archive := estimates[0].(map[string]any)
	require.Equal(t, float64(11), archive["bytes"])
	require.Equal(t, float64(1), archive["not_local"])
}

func runSegDu(t *testing.T, dir string, flags ...string) ([]byte, error) {
	t.Helper()
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args := append([]string{"capcli", "seg", "du", "--datadir", dir}, flags...)
	err := app.Run(t.Context(), args)
	return output.Bytes(), err
}

func TestDuWithoutIndexDB(t *testing.T) {
	dir := t.TempDir()
	_, runErr := runSegDu(t, dir, "--json")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
	if runErr != nil {
		t.Fatalf("empty datadir: want a report, got %v", runErr)
	}
}

func TestDuDefaultManifest(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	db.Close()
	local := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	missing := snaptype.BeaconBlocks.FileName(version.V1_1, 100, 200)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, local), []byte("local"), 0o600))
	data := []byte("'" + local + "' = '0000000000000000000000000000000000000001'\n'" + missing + "' = '0000000000000000000000000000000000000002'\n")
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), data, 0o600))
	t.Setenv(snapcfg.RemotePreverifiedEnvKey, dirs.PreverifiedPath())
	output, err := runSegDu(t, dirs.DataDir, "--chain", "mainnet", "--json")
	require.NoError(t, err)
	var r antiquary.CaplinDuResult
	require.NoError(t, json.Unmarshal(output, &r))
	e := r.Estimates[0]
	if e.NotLocal != 1 || e.Bytes != 5 {
		t.Fatalf("%s = %d bytes, %d not local; manifest has one missing file", e.Regime, e.Bytes, e.NotLocal)
	}
}

func TestDuOlderCaplinSchema(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dirs.CaplinIndexing).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg {
			tables := maps.Clone(kv.ChaindataTablesCfg)
			delete(tables, kv.PtcWindowEpochTable)
			return tables
		}).MustOpen()
	db.Close()
	if _, err := runSegDu(t, dirs.DataDir); err != nil {
		t.Fatalf("older schema: want a report, got %v", err)
	}
}

func TestDuDatadirChain(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	el := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	require.NoError(t, el.Update(t.Context(), func(tx kv.RwTx) error {
		hash := common.Hash{1}
		if err := rawdb.WriteCanonicalHash(tx, hash, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, hash, &chain.Config{ChainName: "gnosis", ChainID: uint256.NewInt(100)})
	}))
	el.Close()
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(2_000_000, common.Hash{2}), []byte{1}); err != nil {
			return err
		}
		return beacon_indicies.WriteCaplinRetention(tx, clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec})
	}))
	db.Close()
	output, err := runSegDu(t, dirs.DataDir, "--json")
	require.NoError(t, err)
	var r antiquary.CaplinDuResult
	require.NoError(t, json.Unmarshal(output, &r))
	if r.Chain != "gnosis" || r.Floors.Blocks != 1_470_000 {
		t.Fatalf("chain = %s, block floor = %d; want gnosis, 1470000", r.Chain, r.Floors.Blocks)
	}
}

func TestSegRetireBlocks(t *testing.T) {
	dirs, roots := prepareCaplinRetireDatadir(t, 50_000, 40_000)

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args := []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet"}
	require.NoError(t, app.Run(t.Context(), args))

	entries, err := os.ReadDir(dirs.Snap)
	require.NoError(t, err)
	require.NotEmpty(t, entries)
	require.NoError(t, func() error {
		db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
		if err != nil {
			return err
		}
		defer db.Close()
		return db.View(t.Context(), func(tx kv.Tx) error {
			body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(1_000, roots[1_000]))
			if err != nil {
				return err
			}
			require.Empty(t, body)
			body, err = tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(30_000, roots[30_000]))
			if err != nil {
				return err
			}
			require.NotEmpty(t, body)
			return nil
		})
	}())
	first := make(map[string]os.FileInfo, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)
		first[entry.Name()] = info
	}

	require.NoError(t, app.Run(t.Context(), args))
	entries, err = os.ReadDir(dirs.Snap)
	require.NoError(t, err)
	second := make(map[string]os.FileInfo, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		require.NoError(t, err)
		second[entry.Name()] = info
	}
	require.Len(t, second, len(first))
	for name, before := range first {
		after, ok := second[name]
		require.True(t, ok, name)
		require.True(t, os.SameFile(before, after), name)
		require.Equal(t, before.ModTime(), after.ModTime(), name)
	}
}

func TestSegRetireSkipsBlockGapWithoutSegments(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Slot = 50_000
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
			return err
		}
		return beacon_indicies.WriteHighestFinalized(tx, 50_000)
	}))
	db.Close()

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	require.NoError(t, app.Run(t.Context(), []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet", "--no-compact"}))
	require.Contains(t, output.String(), "skip blocks: hot beacon blocks start at 50000")
	files, err := snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	for _, file := range files {
		if file.Type != nil && file.Type.Enum() == snaptype.BeaconBlocks.Enum() {
			t.Fatalf("retire wrote block segment %s", file.Path)
		}
	}
}

func TestSegRetireCompactsCaplinDB(t *testing.T) {
	dirs, roots := prepareCaplinCompactDatadir(t)
	dataPath := filepath.Join(dirs.CaplinIndexing, "mdbx.dat")
	before, err := os.Stat(dataPath)
	require.NoError(t, err)

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args := []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet"}
	require.NoError(t, app.Run(t.Context(), args))

	after, err := os.Stat(dataPath)
	require.NoError(t, err)
	require.Less(t, after.Size(), before.Size())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(30_000, roots[30_000]))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
	db.Close()
}

func TestSegRetireNoCompact(t *testing.T) {
	dirs, _ := prepareCaplinCompactDatadir(t)
	dataPath := filepath.Join(dirs.CaplinIndexing, "mdbx.dat")
	before, err := os.Stat(dataPath)
	require.NoError(t, err)

	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args := []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet", "--no-compact"}
	require.NoError(t, app.Run(t.Context(), args))

	after, err := os.Stat(dataPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, after.Size(), before.Size())
}

func TestRetireRefusesUnindexedBlockSegments(t *testing.T) {
	dirs := prepareCaplinStateRetireDatadirWithProgress(t, 0)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.WriteHighestFinalized(tx, 50_000); err != nil {
			return err
		}
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 0)
	}))
	db.Close()
	segments, err := snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	require.NotEmpty(t, segments)
	path := segments[0].Path
	before, err := os.ReadFile(path)
	require.NoError(t, err)

	err = runRetireFixture(t, dirs, "mainnet", "--no-compact")
	require.ErrorContains(t, err, "snapshot index progress")
	require.ErrorContains(t, err, "start the node")
	after, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, before, after)
	db, err = caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		progress, err := beacon_indicies.ReadLastBeaconSnapshot(tx)
		require.NoError(t, err)
		require.Zero(t, progress)
		return nil
	}))
}

func prepareCaplinCompactDatadir(t *testing.T) (datadir.Dirs, map[uint64]common.Hash) {
	t.Helper()
	dirs, roots := prepareCaplinRetireDatadir(t, 50_000, 40_000)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		value := bytes.Repeat([]byte{1}, 4096)
		for i := range 10_000 {
			root := common.Hash{}
			binary.BigEndian.PutUint64(root[:], uint64(i)+1)
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(uint64(i%30_000), root), value); err != nil {
				return err
			}
		}
		return nil
	}))
	db.Close()
	return dirs, roots
}

func TestSegRetireStopsAtCanonicalHead(t *testing.T) {
	dirs, _ := prepareCaplinRetireDatadir(t, 50_000, 25_000)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	require.NoError(t, app.Run(t.Context(), []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet"}))
	require.Contains(t, output.String(), "freeze blocks 0..20000")
	files, err := snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	var ranges []string
	for _, file := range files {
		if file.Type != nil && file.Type.Enum() == snaptype.CaplinEnums.BeaconBlocks {
			ranges = append(ranges, fmt.Sprintf("%d..%d", file.From, file.To))
		}
	}
	require.ElementsMatch(t, []string{"0..10000", "10000..20000"}, ranges)
	require.NotContains(t, ranges, "0..30000")
}

func prepareCaplinRetireDatadir(t *testing.T, finalized, head uint64) (datadir.Dirs, map[uint64]common.Hash) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	roots := make(map[uint64]common.Hash)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		var parent common.Hash
		for slot := uint64(0); slot <= head; slot += 500 {
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Slot = slot
			block.Block.ParentRoot = parent
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
				return err
			}
			root, err := block.Block.HashSSZ()
			if err != nil {
				return err
			}
			roots[slot], parent = root, root
		}
		return beacon_indicies.WriteHighestFinalized(tx, finalized)
	}))
	db.Close()
	return dirs, roots
}

func prepareCaplinBackfillGapDatadir(t *testing.T, linked bool) datadir.Dirs {
	t.Helper()
	dirs, roots := prepareCaplinRetireDatadir(t, 60_000, 50_000)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	var frozenRoot common.Hash
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
		block.Block.Slot = 29_999
		block.Block.ParentRoot = roots[29_500]
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
			return err
		}
		var err error
		frozenRoot, err = block.Block.HashSSZ()
		return err
	}))
	require.NoError(t, freezeblocks.DumpBeaconBlocks(t.Context(), db, 0, 30_000, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Delete(kv.BeaconBlocks, dbutils.BlockBodyKey(30_000, roots[30_000])); err != nil {
			return err
		}
		if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(30_000)); err != nil {
			return err
		}
		parent := frozenRoot
		if !linked {
			parent = roots[30_000]
		}
		for slot := uint64(30_500); slot <= 50_000; slot += 500 {
			if err := tx.Delete(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, roots[slot])); err != nil {
				return err
			}
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Slot = slot
			block.Block.ParentRoot = parent
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
				return err
			}
			var err error
			parent, err = block.Block.HashSSZ()
			if err != nil {
				return err
			}
		}
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 29_999)
	}))
	db.Close()
	return dirs
}

func prepareCaplinBackfillChainGapDatadir(t *testing.T) datadir.Dirs {
	t.Helper()
	dirs, roots := prepareCaplinRetireDatadir(t, 70_000, 60_000)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	var frozenRoot common.Hash
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
		block.Block.Slot = 29_999
		block.Block.ParentRoot = roots[29_500]
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
			return err
		}
		frozenRoot, err = block.Block.HashSSZ()
		if err != nil {
			return err
		}
		return nil
	}))
	require.NoError(t, freezeblocks.DumpBeaconBlocks(t.Context(), db, 0, 30_000, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		parent := frozenRoot
		for slot := uint64(30_000); slot <= 60_000; slot += 500 {
			if err := tx.Delete(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, roots[slot])); err != nil {
				return err
			}
			if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot)); err != nil {
				return err
			}
			if slot == 39_500 {
				continue
			}
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Slot = slot
			block.Block.ParentRoot = parent
			if slot == 40_000 {
				block.Block.ParentRoot = common.Hash{1}
			}
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
				return err
			}
			parent, err = block.Block.HashSSZ()
			if err != nil {
				return err
			}
		}
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 29_999)
	}))
	db.Close()
	return dirs
}

func prepareCaplinBackfillTailGapDatadir(t *testing.T) datadir.Dirs {
	t.Helper()
	dirs, roots := prepareCaplinRetireDatadir(t, 60_000, 60_000)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	var frozenRoot common.Hash
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
		block.Block.Slot = 29_999
		block.Block.ParentRoot = roots[29_500]
		if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
			return err
		}
		frozenRoot, err = block.Block.HashSSZ()
		return err
	}))
	require.NoError(t, freezeblocks.DumpBeaconBlocks(t.Context(), db, 0, 30_000, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		parent := frozenRoot
		for slot := uint64(30_000); slot <= 60_000; slot += 500 {
			if err := tx.Delete(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, roots[slot])); err != nil {
				return err
			}
			if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot)); err != nil {
				return err
			}
			if slot > 39_000 && slot != 40_500 {
				continue
			}
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Slot = slot
			block.Block.ParentRoot = parent
			if slot == 40_500 {
				block.Block.ParentRoot = common.Hash{2}
			}
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
				return err
			}
			parent, err = block.Block.HashSSZ()
			if err != nil {
				return err
			}
		}
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 29_999)
	}))
	db.Close()
	return dirs
}

func prepareCaplinNoSegmentChainGapDatadir(t *testing.T) datadir.Dirs {
	t.Helper()
	dirs, roots := prepareCaplinRetireDatadir(t, 40_000, 30_000)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, slot := range []uint64{9_500, 10_000, 10_500} {
			if err := tx.Delete(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, roots[slot])); err != nil {
				return err
			}
			if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot)); err != nil {
				return err
			}
		}
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
		block.Block.Slot = 10_500
		block.Block.ParentRoot = common.Hash{3}
		return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true)
	}))
	db.Close()
	return dirs
}

func TestSegRetireBlobs(t *testing.T) {
	dirs, roots := prepareCaplinBlobRetireDatadir(t)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args := []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "hoodi"}
	require.NoError(t, app.Run(t.Context(), args))
	require.Contains(t, output.String(), "15000")
	files, err := snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	var blobRanges []string
	for _, file := range files {
		if file.Type != nil && file.Type.Enum() == snaptype.CaplinEnums.BlobSidecars {
			blobRanges = append(blobRanges, fmt.Sprintf("%d..%d", file.From, file.To))
		}
	}
	require.Equal(t, []string{"0..10000"}, blobRanges)

	blobDB := openCaplinBlobDB(t, dirs)
	store := blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs))
	count, err := store.KzgCommitmentsCount(t.Context(), roots[5_000])
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = store.KzgCommitmentsCount(t.Context(), roots[14_500])
	require.NoError(t, err)
	require.Equal(t, uint32(1), count)
	blobDB.Close()

	before := map[string]os.FileInfo{}
	for _, file := range files {
		info, err := os.Stat(file.Path)
		require.NoError(t, err)
		before[file.Path] = info
	}
	output.Reset()
	require.NoError(t, app.Run(t.Context(), args))
	files, err = snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	for _, file := range files {
		if beforeInfo, ok := before[file.Path]; ok {
			afterInfo, err := os.Stat(file.Path)
			require.NoError(t, err)
			require.True(t, os.SameFile(beforeInfo, afterInfo), file.Path)
			require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime(), file.Path)
		}
	}
}

func TestSegRetireState(t *testing.T) {
	dirs := prepareCaplinStateRetireDatadir(t)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args := []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet"}
	require.NoError(t, app.Run(t.Context(), args))
	require.Contains(t, output.String(), "state")

	files, err := snaptype.Segments(dirs.SnapCaplin)
	require.NoError(t, err)
	types := snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil).KeyValueGetters
	for table := range types {
		enum, ok := snaptype.ParseEnum(table)
		require.True(t, ok, table)
		var ranges []string
		for _, file := range files {
			if file.Type != nil && file.Type.Enum() == enum {
				ranges = append(ranges, fmt.Sprintf("%d..%d", file.From, file.To))
			}
		}
		require.Equal(t, []string{"0..50000"}, ranges, table)
	}

	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		for table := range types {
			below, err := tx.GetOne(table, base_encoding.Encode64ToBytes4(49_999))
			if err != nil {
				return err
			}
			require.Empty(t, below, table)
			atBoundary, err := tx.GetOne(table, base_encoding.Encode64ToBytes4(50_000))
			if err != nil {
				return err
			}
			require.NotEmpty(t, atBoundary, table)
		}
		return nil
	}))
	db.Close()

	before := make(map[string]os.FileInfo, len(files))
	for _, file := range files {
		info, err := os.Stat(file.Path)
		require.NoError(t, err)
		before[file.Path] = info
	}
	output.Reset()
	require.NoError(t, app.Run(t.Context(), args))
	files, err = snaptype.Segments(dirs.SnapCaplin)
	require.NoError(t, err)
	for _, file := range files {
		beforeInfo, ok := before[file.Path]
		require.True(t, ok, file.Path)
		afterInfo, err := os.Stat(file.Path)
		require.NoError(t, err)
		require.True(t, os.SameFile(beforeInfo, afterInfo), file.Path)
		require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime(), file.Path)
	}
}

func TestSegRetireStateSafetyMargin(t *testing.T) {
	dirs := prepareCaplinStateRetireDatadirWithProgress(t, 150_000)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	args := []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet"}
	require.NoError(t, app.Run(t.Context(), args))

	files, err := snaptype.Segments(dirs.SnapCaplin)
	require.NoError(t, err)
	blockRoot, ok := snaptype.ParseEnum(kv.BlockRoot)
	require.True(t, ok)
	var ranges []string
	for _, file := range files {
		if file.Type != nil && file.Type.Enum() == blockRoot {
			ranges = append(ranges, fmt.Sprintf("%d..%d", file.From, file.To))
		}
	}
	require.Equal(t, []string{"0..50000"}, ranges)
}

func prepareCaplinStateRetireDatadir(t *testing.T) datadir.Dirs {
	return prepareCaplinStateRetireDatadirWithProgress(t, 120_000)
}

func prepareCaplinStateRetireDatadirWithProgress(t *testing.T, progress uint64) datadir.Dirs {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	types := snapshotsync.MakeCaplinStateSnapshotsTypes(db, &clparams.MainnetBeaconConfig).KeyValueGetters
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		var parent common.Hash
		for slot := range snaptype.CaplinMergeLimit {
			slot := uint64(slot)
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Slot = slot
			block.Block.ParentRoot = parent
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
				return err
			}
			root, err := block.Block.HashSSZ()
			if err != nil {
				return err
			}
			parent = root
		}
		if err := state_accessors.SetStateProcessingProgress(tx, progress); err != nil {
			return err
		}
		for table := range types {
			if table == kv.BlockRoot || table == kv.StateRoot {
				for slot := range progress {
					slot := uint64(slot)
					value := make([]byte, 32)
					value[0] = byte(slot)
					if err := tx.Put(table, base_encoding.Encode64ToBytes4(slot), value); err != nil {
						return err
					}
				}
				continue
			}
			for _, slot := range []uint64{0, 50_000, 100_000} {
				if err := tx.Put(table, base_encoding.Encode64ToBytes4(slot), []byte{byte(slot)}); err != nil {
					return err
				}
			}
		}
		return nil
	}))
	require.NoError(t, freezeblocks.DumpBeaconBlocks(t.Context(), db, 0, snaptype.CaplinMergeLimit, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteLastBeaconSnapshot(tx, snaptype.CaplinMergeLimit-1)
	}))
	db.Close()
	return dirs
}

func prepareCaplinBlobRetireDatadir(t *testing.T) (datadir.Dirs, map[uint64]common.Hash) {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	_, cfg, _, err := clparams.GetConfigsByNetworkName("hoodi")
	require.NoError(t, err)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	roots := make(map[uint64]common.Hash)
	blocks := make(map[uint64]*cltypes.SignedBeaconBlock)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		var parent common.Hash
		for slot := uint64(0); slot <= 30_000; slot += 500 {
			block := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
			block.Block.Slot = slot
			block.Block.ParentRoot = parent
			block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true); err != nil {
				return err
			}
			root, err := block.Block.HashSSZ()
			if err != nil {
				return err
			}
			roots[slot], blocks[slot], parent = root, block, root
		}
		return nil
	}))
	require.NoError(t, freezeblocks.DumpBeaconBlocks(t.Context(), db, 0, 30_000, 0, dirs, 1, log.LvlDebug, log.New()))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 29_999)
	}))
	db.Close()

	blobDB := openCaplinBlobDB(t, dirs)
	store := blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs))
	for slot, block := range blocks {
		if slot > 20_000 || slot == 15_000 {
			continue
		}
		sidecar := cltypes.NewBlobSidecar(0, &cltypes.Blob{}, common.Bytes48{}, common.Bytes48{}, block.SignedBeaconBlockHeader(), solid.NewHashVector(cltypes.CommitmentBranchSize))
		require.NoError(t, store.WriteBlobSidecars(t.Context(), roots[slot], []*cltypes.BlobSidecar{sidecar}))
	}
	blobDB.Close()
	return dirs, roots
}

func openCaplinBlobDB(t *testing.T, dirs datadir.Dirs) kv.RwDB {
	t.Helper()
	db, err := mdbx.New(dbcfg.CaplinDB, log.New()).Path(filepath.Join(dirs.CaplinBlobs, "chaindata")).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).Open(t.Context())
	require.NoError(t, err)
	return db
}

func runRetireFixture(t *testing.T, dirs datadir.Dirs, chain string, flags ...string) error {
	t.Helper()
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	return app.Run(t.Context(), append([]string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", chain}, flags...))
}

func TestRetireLockedDirectory(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	unlock, err := dirs.TryFlock()
	require.NoError(t, err)
	defer unlock()
	require.NoError(t, dir.RemoveAll(dirs.Migrations))
	require.ErrorIs(t, runRetireFixture(t, dirs, "mainnet"), datadir.ErrDataDirLocked)
	_, err = os.Stat(dirs.Migrations)
	require.ErrorIs(t, err, os.ErrNotExist, "retire created migrations while the node held LOCK")
}

func TestRetireBeforeFirstBlockSegment(t *testing.T) {
	dirs, _ := prepareCaplinRetireDatadir(t, 8_000, 9_000)
	require.NoError(t, runRetireFixture(t, dirs, "mainnet"), "no block segment is due")
}

func TestRetireChecksFrozenBlockLink(t *testing.T) {
	for _, test := range []struct {
		name   string
		linked bool
	}{
		{name: "backfill gap", linked: false},
		{name: "linked after missed slot", linked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dirs := prepareCaplinBackfillGapDatadir(t, test.linked)
			var output bytes.Buffer
			app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
			commandErr := app.Run(t.Context(), []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet", "--no-compact"})
			segments, err := snaptype.Segments(dirs.Snap)
			require.NoError(t, err)
			var frozen []string
			for _, segment := range segments {
				if segment.Type != nil && segment.Type.Enum() == snaptype.BeaconBlocks.Enum() && segment.From >= 30_000 {
					frozen = append(frozen, segment.Path)
				}
			}
			if test.linked {
				require.NoError(t, commandErr)
				require.Contains(t, output.String(), "freeze blocks 30000..40000")
				require.NotEmpty(t, frozen)
			} else {
				if commandErr == nil {
					require.Contains(t, output.String(), "skip blocks")
					require.Contains(t, output.String(), "30500")
				}
				require.Empty(t, frozen)
			}
		})
	}
}

func TestRetireChecksFrozenBlockChain(t *testing.T) {
	dirs := prepareCaplinBackfillChainGapDatadir(t)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	commandErr := app.Run(t.Context(), []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet", "--no-compact"})
	segments, err := snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	var frozen []string
	for _, segment := range segments {
		if segment.Type != nil && segment.Type.Enum() == snaptype.BeaconBlocks.Enum() && segment.From >= 30_000 {
			frozen = append(frozen, segment.Path)
		}
	}
	if commandErr == nil {
		require.Contains(t, output.String(), "skip blocks")
		require.Contains(t, output.String(), "40000")
	}
	require.Empty(t, frozen)
}

func TestRetireChecksFrozenBlockTail(t *testing.T) {
	dirs := prepareCaplinBackfillTailGapDatadir(t)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	commandErr := app.Run(t.Context(), []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet", "--no-compact"})
	segments, err := snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	var frozen []string
	for _, segment := range segments {
		if segment.Type != nil && segment.Type.Enum() == snaptype.BeaconBlocks.Enum() && segment.From >= 30_000 {
			frozen = append(frozen, segment.Path)
		}
	}
	if commandErr == nil {
		require.Contains(t, output.String(), "skip blocks")
		require.Contains(t, output.String(), "40500")
	}
	require.Empty(t, frozen)
}

func TestRetireChecksFrozenBlockChainWithoutSegments(t *testing.T) {
	dirs := prepareCaplinNoSegmentChainGapDatadir(t)
	var output bytes.Buffer
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output, ErrWriter: &output}
	commandErr := app.Run(t.Context(), []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet", "--no-compact"})
	segments, err := snaptype.Segments(dirs.Snap)
	require.NoError(t, err)
	var frozen []string
	for _, segment := range segments {
		if segment.Type != nil && segment.Type.Enum() == snaptype.BeaconBlocks.Enum() {
			frozen = append(frozen, segment.Path)
		}
	}
	if commandErr == nil {
		require.Contains(t, output.String(), "skip blocks")
		require.Contains(t, output.String(), "10500")
	}
	require.Empty(t, frozen)
}

func TestRetireKeepsCompactDatabaseOnRerun(t *testing.T) {
	dirs, _ := prepareCaplinRetireDatadir(t, 50_000, 50_000)
	require.NoError(t, runRetireFixture(t, dirs, "mainnet"))
	path := filepath.Join(dirs.CaplinIndexing, "mdbx.dat")
	before, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, runRetireFixture(t, dirs, "mainnet"))
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime(), "second retire modified mdbx.dat")
}

func TestRetireCompactsIdleDatabase(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	value := bytes.Repeat([]byte{1}, 4096)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for slot := range uint64(5000) {
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, common.Hash{byte(slot)}), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for slot := range uint64(5000) {
			if err := tx.Delete(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, common.Hash{byte(slot)})); err != nil {
				return err
			}
		}
		return nil
	}))
	db.Close()

	path := filepath.Join(dirs.CaplinIndexing, "mdbx.dat")
	before, err := os.Stat(path)
	require.NoError(t, err)
	previous := caplinNeedsCompaction
	calls := 0
	caplinNeedsCompaction = func(string) (bool, error) {
		calls++
		return calls == 1, nil
	}
	t.Cleanup(func() { caplinNeedsCompaction = previous })
	require.NoError(t, runRetireFixture(t, dirs, "mainnet"))
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.Less(t, after.Size(), before.Size())
	require.NoError(t, runRetireFixture(t, dirs, "mainnet"))
	afterSecond, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, after.ModTime(), afterSecond.ModTime())
	require.Equal(t, 2, calls)
}

type failBlockPruneDB struct {
	kv.RwDB
	err error
}

func (db failBlockPruneDB) Update(context.Context, func(kv.RwTx) error) error {
	return db.err
}

func TestRetireRetriesPublishedBlockPrune(t *testing.T) {
	dirs, roots := prepareCaplinRetireDatadir(t, 30_000, 30_000)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &clparams.MainnetBeaconConfig, dirs, log.New())
	require.NoError(t, sn.OpenFolder())
	fault := errors.New("block prune unavailable")
	_, err = antiquary.RetireBeaconBlocks(t.Context(), failBlockPruneDB{db, fault}, sn, dirs, log.New(), nil, nil)
	require.ErrorIs(t, err, fault)
	header, _, _, err := sn.ReadHeader(500, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(500), header.Header.Slot)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteLastBeaconSnapshot(tx, sn.BlocksAvailable())
	}))
	sn.Close()
	db.Close()

	require.NoError(t, runRetireFixture(t, dirs, "mainnet"))
	db, err = caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		progress, err := beacon_indicies.ReadLastBeaconSnapshot(tx)
		require.NoError(t, err)
		var hotBodies int
		for slot, root := range roots {
			if slot >= 10_000 {
				continue
			}
			body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, root))
			require.NoError(t, err)
			if len(body) != 0 {
				hotBodies++
			}
		}
		if progress != 9_999 || hotBodies != 0 {
			t.Errorf("after retry: progress=%d covered hot bodies=%d, want 9999 and 0", progress, hotBodies)
		}
		return nil
	}))
}

func TestRetireRetriesPublishedBlobPrune(t *testing.T) {
	dirs, roots := prepareCaplinBlobRetireDatadir(t)
	_, cfg, _, err := clparams.GetConfigsByNetworkName("hoodi")
	require.NoError(t, err)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	store, closeStore, err := openCaplinBlobStore(t.Context(), dirs, false)
	require.NoError(t, err)
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "hoodi"}, cfg, dirs, log.New())
	require.NoError(t, sn.OpenFolder())
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hooks := antiquary.BlobRetireHooks{Seed: func([]string) error { cancel(); return nil }}
	_, err = antiquary.RetireBlobSidecars(ctx, db, store, sn, freezeblocks.NewBeaconSnapshotReader(sn, nil, cfg), cfg, dirs, sn.Salt, log.New(), hooks)
	require.ErrorIs(t, err, context.Canceled)
	sidecars, err := sn.ReadBlobSidecars(500)
	require.NoError(t, err)
	require.Len(t, sidecars, 1)
	sn.Close()
	closeStore()
	db.Close()

	require.NoError(t, runRetireFixture(t, dirs, "hoodi"))
	store, closeStore, err = openCaplinBlobStore(t.Context(), dirs, false)
	require.NoError(t, err)
	defer closeStore()
	count, err := store.KzgCommitmentsCount(t.Context(), roots[500])
	require.NoError(t, err)
	require.Zero(t, count, "retry left the published sidecar in hot storage")
}

type stopRepeatedStateDump struct {
	cancel context.CancelFunc
	passes int
}

func (w *stopRepeatedStateDump) Write(p []byte) (int, error) {
	if bytes.HasPrefix(p, []byte("retire state ")) {
		w.passes++
		if w.passes == 2 {
			w.cancel()
		}
	}
	return len(p), nil
}

func TestRetireStopsAtIncompleteStateRange(t *testing.T) {
	dirs := prepareCaplinStateRetireDatadir(t)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Delete(kv.BlockRoot, base_encoding.Encode64ToBytes4(1))
	}))
	db.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	writer := &stopRepeatedStateDump{cancel: cancel}
	app := &cli.Command{Name: "capcli", Commands: commands(), Writer: writer}
	err = app.Run(ctx, []string{"capcli", "seg", "retire", "--datadir", dirs.DataDir, "--chain", "mainnet", "--no-compact"})
	require.LessOrEqual(t, writer.passes, 1, "retire reported the same incomplete range a second time")
	if err != nil {
		require.NotErrorIs(t, err, context.Canceled)
	}
}

func TestRetireBlockOverlapBeforeBlobFailure(t *testing.T) {
	dirs, _ := prepareCaplinRetireDatadir(t, 30_000, 30_000)
	writeCapcliSnapshotSegment(t, dirs, snaptype.BeaconBlocks, 0, 5_000, func(uint64) []byte { return nil })
	small := snaptype.BeaconBlocks.FileInfo(dirs.Snap, 0, 5_000)
	for _, idx := range small.Type.IdxFileNames(small.From, small.To) {
		require.NoError(t, dir.RemoveFile(filepath.Join(dirs.Snap, idx)))
	}
	obstacle := snaptype.BlobSidecars.FileInfo(dirs.Snap, 0, 10_000).Path
	require.NoError(t, os.Mkdir(obstacle, 0o700))
	err := runRetireFixture(t, dirs, "hoodi", "--no-compact")
	require.ErrorContains(t, err, filepath.Base(obstacle))
	require.FileExists(t, snaptype.BeaconBlocks.FileInfo(dirs.Snap, 0, 10_000).Path)
	_, statErr := os.Stat(small.Path)
	require.NoError(t, dir.RemoveAll(obstacle))
	require.NoError(t, runRetireFixture(t, dirs, "hoodi", "--no-compact"))
	require.NoFileExists(t, small.Path)
	require.ErrorIs(t, statErr, os.ErrNotExist, "block cleanup was deferred past a failing blob step")
}

type failSidecarPrune struct {
	blob_storage.BlobStorage
	err error
}

func (s failSidecarPrune) RemoveBlobSidecars(ctx context.Context, slot uint64, root common.Hash) error {
	if slot == 500 {
		return s.err
	}
	return s.BlobStorage.RemoveBlobSidecars(ctx, slot, root)
}

func TestRetireReportsBlobPruneError(t *testing.T) {
	dirs, _ := prepareCaplinBlobRetireDatadir(t)
	_, cfg, _, err := clparams.GetConfigsByNetworkName("hoodi")
	require.NoError(t, err)
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	defer db.Close()
	store, closeStore, err := openCaplinBlobStore(t.Context(), dirs, false)
	require.NoError(t, err)
	defer closeStore()
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "hoodi"}, cfg, dirs, log.New())
	defer sn.Close()
	require.NoError(t, sn.OpenFolder())
	fault := errors.New("hot store write failed")
	reader := freezeblocks.NewBeaconSnapshotReader(sn, nil, cfg)
	result, err := antiquary.RetireBlobSidecars(t.Context(), db, failSidecarPrune{store, fault}, sn, reader, cfg, dirs, sn.Salt, log.New(), antiquary.BlobRetireHooks{})
	require.ErrorIs(t, err, fault, "reported retirement through %d despite the hot prune failure", result.To)
}

func TestSegResetIndexAccessMode(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteLastBeaconSnapshot(tx, 100)
	}))
	db.Close()
	local := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	replacement := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, local), []byte(local), 0o600))
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte("'"+replacement+"' = 'replacement'\n"), 0o600))
	app := &cli.Command{Name: "capcli", Commands: commands()}
	args := []string{"capcli", "seg", "reset", "--chain", "mainnet", "--datadir", dirs.DataDir}
	t.Run("dry run", func(t *testing.T) {
		path := filepath.Join(dirs.CaplinIndexing, "mdbx.dat")
		marker := time.Unix(1500000000, 0)
		require.NoError(t, os.Chtimes(path, marker, marker))
		require.NoError(t, app.Run(t.Context(), append(args, "--dry-run")))
		stat, err := os.Stat(path)
		require.NoError(t, err)
		if !stat.ModTime().Equal(marker) {
			t.Fatal("dry-run changed indexing/mdbx.dat mtime")
		}
	})
	t.Run("execute", func(t *testing.T) {
		app := &cli.Command{Name: "capcli", Commands: commands()}
		err := app.Run(t.Context(), args)
		if err != nil {
			_, fileErr := os.Stat(filepath.Join(dirs.Snap, local))
			t.Fatalf("segment still present = %t, reset error = %v", fileErr == nil, err)
		}
		db, err = caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
		require.NoError(t, err)
		defer db.Close()
		require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
			progress, err := beacon_indicies.ReadLastBeaconSnapshot(tx)
			require.NoError(t, err)
			require.Zero(t, progress)
			return nil
		}))
	})
}

func TestSegResetDryRunKeepsDatadir(t *testing.T) {
	for _, locked := range []bool{false, true} {
		t.Run(fmt.Sprintf("locked=%t", locked), func(t *testing.T) {
			dirs := datadir.Open(t.TempDir())
			unlock, err := dirs.TryFlock()
			require.NoError(t, err)
			if locked {
				defer unlock()
			} else {
				unlock()
			}
			app := &cli.Command{Name: "capcli", Commands: commands()}
			err = app.Run(t.Context(), []string{"capcli", "seg", "reset", "--chain", "mainnet", "--datadir", dirs.DataDir, "--dry-run"})
			if locked {
				require.ErrorIs(t, err, datadir.ErrDataDirLocked)
			} else {
				require.NoError(t, err)
			}
			entries, err := os.ReadDir(dirs.DataDir)
			require.NoError(t, err)
			if len(entries) != 1 {
				t.Fatalf("dry-run changed datadir: %d entries, want 1", len(entries))
			}
		})
	}
}

func TestSegResetUsesSupportedReplacements(t *testing.T) {
	preserveResetManifest(t)
	dirs := datadir.New(t.TempDir())
	local := snaptype.StateEvents.FileName(version.V1_1, 0, 100)
	future := "caplin/" + snaptype.StateEvents.FileName(version.Version{Major: 9}, 0, 200)
	data := []byte("'" + future + "' = '0000000000000000000000000000000000000001'\n")
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapCaplin, local), []byte(local), 0o600))
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), data, 0o600))
	require.NoError(t, snapcfg.SetToml("mainnet", data, true))
	cfg, _ := snapcfg.KnownCfg("mainnet")
	_, available := cfg.Preverified.Items.Get(future)
	require.False(t, available)
	app := &cli.Command{Name: "capcli", Commands: commands()}
	err := app.Run(t.Context(), []string{"capcli", "seg", "reset", "--chain", "mainnet", "--datadir", dirs.DataDir})
	if err == nil {
		_, statErr := os.Stat(filepath.Join(dirs.SnapCaplin, local))
		t.Fatalf("reset accepted unreadable replacement: local StateEvents still present = %t", statErr == nil)
	}
	require.ErrorContains(t, err, "only copy")
}

func TestSegResetLoadsManifestWithoutLock(t *testing.T) {
	preserveResetManifest(t)
	dirs := datadir.New(t.TempDir())
	file := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	source := filepath.Join(t.TempDir(), "mainnet.toml")
	require.NoError(t, os.WriteFile(source, []byte("'"+file+"' = '0000000000000000000000000000000000000001'\n"), 0o600))
	t.Setenv(snapcfg.RemotePreverifiedEnvKey, source)
	require.NoError(t, snapcfg.LoadRemotePreverified(t.Context(), "mainnet", ""))
	cfg, _ := snapcfg.KnownCfg("mainnet")
	_, published := cfg.Preverified.Items.Get(file)
	require.True(t, published)
	require.NoError(t, snapcfg.SetToml("mainnet", []byte{}, false))
	path := filepath.Join(dirs.Snap, file)
	require.NoError(t, os.WriteFile(path, []byte(file), 0o600))
	app := &cli.Command{Name: "capcli", Commands: commands()}
	require.NoError(t, app.Run(t.Context(), []string{"capcli", "seg", "reset", "--chain", "mainnet", "--datadir", dirs.DataDir, "--force"}))
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		t.Fatal("published beacon segment was removed with --force")
	}
	require.NoError(t, err)
}

func preserveResetManifest(t *testing.T) {
	t.Helper()
	cfg, _ := snapcfg.KnownCfg("mainnet")
	raw := append([]byte{}, snapcfg.GetToml("mainnet")...)
	local := cfg.Local
	t.Cleanup(func() { require.NoError(t, snapcfg.SetToml("mainnet", raw, local)) })
}

func TestRegimeShowOlderSchema(t *testing.T) {
	for _, store := range []string{"index", "blobs"} {
		t.Run(store, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			path := dirs.CaplinIndexing
			if store == "blobs" {
				path = filepath.Join(dirs.CaplinBlobs, "chaindata")
			}
			db := mdbx.New(dbcfg.CaplinDB, log.New()).Path(path).
				WithTableCfg(func(kv.TableCfg) kv.TableCfg {
					tables := maps.Clone(kv.ChaindataTablesCfg)
					delete(tables, kv.PtcWindowEpochTable)
					return tables
				}).MustOpen()
			db.Close()
			output, err := runRegime(t, dirs, "show")
			if err != nil {
				t.Fatalf("show with older %s DB: %v", store, err)
			}
			require.Contains(t, output, "stored regime none stored")
			require.Contains(t, output, "oldest states none")
		})
	}
}

func writeRegimeExecutionChain(t *testing.T, dirs datadir.Dirs) {
	t.Helper()
	el := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	defer el.Close()
	require.NoError(t, el.Update(t.Context(), func(tx kv.RwTx) error {
		hash := chainspec.Mainnet.GenesisHash
		if err := rawdb.WriteCanonicalHash(tx, hash, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, hash, chainspec.Mainnet.Config)
	}))
}

func TestRegimeShowBlobFiles(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := openCaplinBlobDB(t, dirs)
	store := blob_storage.NewBlobStore(db, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs))
	header := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 10_000_000}}
	sidecar := cltypes.NewBlobSidecar(0, &cltypes.Blob{}, common.Bytes48{}, common.Bytes48{}, header, solid.NewHashVector(cltypes.CommitmentBranchSize))
	require.NoError(t, store.WriteBlobSidecars(t.Context(), common.Hash{1}, []*cltypes.BlobSidecar{sidecar}))
	db.Close()
	want := "oldest sidecars 10000000"
	out, err := runRegime(t, dirs, "show")
	require.NoError(t, err)
	require.Contains(t, out, want+"\n")

	check := func(t *testing.T, out string, err error) {
		t.Helper()
		require.NoError(t, err)
		_, rest, ok := strings.Cut(out, "oldest sidecars ")
		require.True(t, ok)
		slot, _, _ := strings.Cut(rest, "\n")
		if slot != "10000000" {
			t.Fatalf("oldest sidecars %s, want 10000000", slot)
		}
	}

	t.Run("empty index with known chain", func(t *testing.T) {
		index, err := caplin1.OpenCaplinIndexDb(t.Context(), dirs.CaplinIndexing)
		require.NoError(t, err)
		index.Close()
		writeRegimeExecutionChain(t, dirs)
		var output bytes.Buffer
		app := &cli.Command{Name: "capcli", Commands: commands(), Writer: &output}
		err = app.Run(t.Context(), []string{"capcli", "seg", "regime", "show", "--datadir", dirs.DataDir})
		check(t, output.String(), err)
	})

	t.Run("missing blob metadata", func(t *testing.T) {
		require.NoError(t, dir.RemoveAll(filepath.Join(dirs.CaplinBlobs, "chaindata")))
		out, err := runRegime(t, dirs, "show")
		check(t, out, err)
	})
}
