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
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
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
