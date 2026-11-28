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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/datadir/reset"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
)

// writeChainMetadata is a test helper that writes a genesis canonical hash and chain config
// to the database, mimicking what a real Erigon node writes during initialisation.
func writeChainMetadata(t *testing.T, ctx context.Context, db kv.RwDB, chainName string, chainID uint64) common.Hash {
	t.Helper()
	genesisHash := common.HexToHash("0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
	chainCfg := &chain.Config{
		ChainName: chainName,
		ChainID:   uint256.NewInt(chainID),
	}
	err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := rawdb.WriteCanonicalHash(tx, genesisHash, 0); err != nil {
			return err
		}
		return rawdb.WriteChainConfig(tx, genesisHash, chainCfg)
	})
	require.NoError(t, err)
	return genesisHash
}

// TestGetChainNameFromChainData_OldSchema verifies that chain-name detection works on a
// database that contains only the metadata tables (simulating an older Erigon version's
// schema). Without the WithTableCfg fix, opening such a DB in Accede+Readonly mode would
// fail because the MDBX wrapper tries to validate every table in ChaindataTablesCfg.
func TestGetChainNameFromChainData_OldSchema(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	// Create a database with ONLY the two tables needed for chain-name discovery.
	// This is the most extreme old-schema scenario: everything except the metadata
	// tables is missing. If getChainNameFromChainData depends on any other table,
	// this test catches it.
	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		WithTableCfg(ChaindataSchemaForGenesis).
		Open(ctx)
	require.NoError(t, err)

	writeChainMetadata(t, ctx, db, "mainnet", 1)
	db.Close()

	result, err := getChainNameFromChainData(ctx, logger, chainDataDir)
	require.NoError(t, err, "should not fail on a DB with only metadata tables")
	require.True(t, result.Ok, "chain name should be detected")
	require.Equal(t, "mainnet", result.Value)
}

// TestGetChainNameFromChainData_CurrentSchema verifies the happy path: a fully migrated
// database with all current tables must still work correctly after the fix.
func TestGetChainNameFromChainData_CurrentSchema(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	// Create a database with the full current ChaindataTablesCfg — this is what a
	// fully migrated node would have.
	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		Open(ctx)
	require.NoError(t, err)

	writeChainMetadata(t, ctx, db, "mainnet", 1)
	db.Close()

	result, err := getChainNameFromChainData(ctx, logger, chainDataDir)
	require.NoError(t, err, "should not fail on a current-schema DB")
	require.True(t, result.Ok)
	require.Equal(t, "mainnet", result.Value)
}

// TestGetChainNameFromChainData_NonMainnet verifies that chain detection is not hardcoded to
// mainnet — it must correctly read whatever chain name is stored in the database.
func TestGetChainNameFromChainData_NonMainnet(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		WithTableCfg(ChaindataSchemaForGenesis).
		Open(ctx)
	require.NoError(t, err)

	writeChainMetadata(t, ctx, db, "sepolia", 11155111)
	db.Close()

	result, err := getChainNameFromChainData(ctx, logger, chainDataDir)
	require.NoError(t, err)
	require.True(t, result.Ok)
	require.Equal(t, "sepolia", result.Value)
}

// TestGetChainNameFromChainData_EmptyDB verifies behaviour when the database exists but
// contains no chain metadata (no genesis hash, no config). This represents a freshly
// created but never-initialised datadir.
func TestGetChainNameFromChainData_EmptyDB(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "chaindata")

	db, err := mdbx.New(dbcfg.ChainDB, logger).
		Path(chainDataDir).
		WithTableCfg(ChaindataSchemaForGenesis).
		Open(ctx)
	require.NoError(t, err)
	db.Close()

	// No chain metadata written — the function should return Ok=false with no error.
	result, err := getChainNameFromChainData(ctx, logger, chainDataDir)
	require.NoError(t, err)
	require.False(t, result.Ok, "empty DB should not yield a chain name")
}

// TestGetChainNameFromChainData_NonExistentDir verifies graceful handling when the
// chaindata directory does not exist.
func TestGetChainNameFromChainData_NonExistentDir(t *testing.T) {
	ctx := context.Background()
	logger := log.New()
	chainDataDir := filepath.Join(t.TempDir(), "nonexistent", "chaindata")

	result, err := getChainNameFromChainData(ctx, logger, chainDataDir)
	require.Error(t, err)
	require.False(t, result.Ok)
}

func TestResetKeepLocalSnapshotsFlag(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		keep    bool
		wantErr bool
	}{
		{args: nil, keep: false},
		{args: []string{"--keep-local-snapshots"}, keep: true},
		{args: []string{"--local=false"}, keep: true},
		{args: []string{"--local"}, keep: false},
		{args: []string{"-l=false"}, keep: true},
		{args: []string{"--local=false", "--keep-local-snapshots"}, wantErr: true},
	} {
		local, keepFlag := removeLocalFlag, keepLocalSnapshotsFlag
		var keep bool
		var err error
		cmd := &cli.Command{
			Name:  "reset",
			Flags: []cli.Flag{&local, &keepFlag},
			Action: func(_ context.Context, cmd *cli.Command) error {
				keep, err = keepLocalSnapshots(cmd)
				return nil
			},
		}
		require.NoError(t, cmd.Run(context.Background(), append([]string{"reset"}, tc.args...)), "%v", tc.args)
		if tc.wantErr {
			require.Error(t, err, "%v", tc.args)
			continue
		}
		require.NoError(t, err, "%v", tc.args)
		require.Equal(t, tc.keep, keep, "%v", tc.args)
	}
}

func TestResetCaplinDryRun(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	block := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	replacement := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	blob := snaptype.BlobSidecars.FileName(version.V1_1, 200, 300)
	require.NoError(t, os.MkdirAll(dirs.Snap, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, block), []byte(block), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, blob), []byte(blob), 0o600))
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte("'"+replacement+"' = 'replacement'\n"), 0o600))
	chainDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).WithTableCfg(ChaindataSchemaForGenesis).MustOpen()
	writeChainMetadata(t, t.Context(), chainDB, "mainnet", 1)
	chainDB.Close()

	report, err := os.CreateTemp(t.TempDir(), "reset-report")
	require.NoError(t, err)
	report.Close()
	stdout := os.Stdout
	os.Stdout, err = os.OpenFile(report.Name(), os.O_WRONLY|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	defer func() {
		os.Stdout.Close()
		os.Stdout = stdout
	}()

	require.NoError(t, runSnapshotsReset(t, "--datadir", dirs.DataDir, "--caplin", "--dry-run"))
	data, err := os.ReadFile(report.Name())
	require.NoError(t, err)
	require.Contains(t, string(data), "caplin reset plan")
	require.Contains(t, string(data), "delete blocks "+block)
	require.Contains(t, string(data), "only copy sidecars "+blob)
	require.FileExists(t, filepath.Join(dirs.Snap, block))
	require.FileExists(t, dirs.PreverifiedPath())
}

func TestResetWithoutCaplinKeepsExistingRemoval(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	block := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	replacement := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	require.NoError(t, os.MkdirAll(dirs.Snap, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, block), []byte(block), 0o600))
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte("'"+replacement+"' = 'replacement'\n"), 0o600))
	chainDB := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).WithTableCfg(ChaindataSchemaForGenesis).MustOpen()
	writeChainMetadata(t, t.Context(), chainDB, "mainnet", 1)
	chainDB.Close()

	require.NoError(t, runSnapshotsReset(t, "--datadir", dirs.DataDir, "--preverified", "local"))
	require.NoFileExists(t, filepath.Join(dirs.Snap, block))
	require.NoFileExists(t, dirs.PreverifiedPath())
}

func runSnapshotsReset(t *testing.T, args ...string) error {
	t.Helper()
	for _, command := range snapshotCommand.Commands {
		if command.Name == "reset" {
			app := &cli.Command{Name: "erigon", Commands: []*cli.Command{{Name: "reset", Action: command.Action, Flags: command.Flags}}}
			return app.Run(t.Context(), append([]string{"erigon", "reset"}, args...))
		}
	}
	t.Fatal("snapshots reset command not found")
	return nil
}

func resetReviewDatadir(t *testing.T) datadir.Dirs {
	t.Helper()
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).WithTableCfg(ChaindataSchemaForGenesis).MustOpen()
	writeChainMetadata(t, t.Context(), db, "mainnet", 1)
	db.Close()
	return dirs
}

type resetDownloadCounter struct {
	dbservices.NoopSeederClient
	files int
}

func (d *resetDownloadCounter) Download(_ context.Context, req *downloaderproto.DownloadRequest) error {
	d.files += len(req.Items)
	return nil
}

func TestResetCaplinRefreshesLockOnRestart(t *testing.T) {
	preserveResetManifest(t)
	dirs := resetReviewDatadir(t)
	local := []byte("'erigondb.toml' = '0000000000000000000000000000000000000001'\n")
	remote := []byte("'erigondb.toml' = '0000000000000000000000000000000000000002'\n")
	source := filepath.Join(t.TempDir(), "remote.toml")
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), local, 0o600))
	require.NoError(t, os.WriteFile(source, remote, 0o600))
	t.Setenv(snapcfg.RemotePreverifiedEnvKey, source)
	require.NoError(t, runSnapshotsReset(t, "--datadir", dirs.DataDir, "--caplin"))
	restart := func(d datadir.Dirs) (bool, string, int) {
		require.NoError(t, downloadercfg.LoadSnapshotsHashes(t.Context(), d, "mainnet", ""))
		cfg, ok := snapcfg.KnownCfg("mainnet")
		require.True(t, ok)
		item, found := cfg.Preverified.Items.Get("erigondb.toml")
		require.True(t, found)
		var requests resetDownloadCounter
		require.NoError(t, snapshotsync.SyncSnapshots(t.Context(), "test", "headers", true, false, false, snapshotsync.BlobRetention{}, prune.ArchiveMode, snapshotsync.NoCaplin, nil, duFakeBlockReader{}, &chain.Config{ChainName: "mainnet"}, &requests, ethconfig.Sync{}, 16, d))
		return cfg.Local, item.Hash, requests.files
	}
	_, remoteHash, files := restart(datadir.New(t.TempDir()))
	require.Equal(t, 1, files)
	fromLock, hash, files := restart(dirs)
	if fromLock || hash != remoteHash || files != 1 {
		t.Fatalf("restart loaded local=%t hash=%s requested files=%d, want remote hash ending in 2 and 1 file", fromLock, hash, files)
	}
}

func TestResetCaplinUsesELManifest(t *testing.T) {
	preserveResetManifest(t)
	for _, lock := range []string{"stale-lock", "no-lock"} {
		t.Run(lock, func(t *testing.T) {
			dirs := resetReviewDatadir(t)
			if lock == "stale-lock" {
				require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte("'salt-state.txt' = '0000000000000000000000000000000000000001'\n"), 0o600))
			}
			source := filepath.Join(t.TempDir(), "remote.toml")
			require.NoError(t, os.WriteFile(source, []byte("'erigondb.toml' = '0000000000000000000000000000000000000002'\n"), 0o600))
			t.Setenv(snapcfg.RemotePreverifiedEnvKey, source)
			require.NoError(t, snapcfg.SetToml("mainnet", []byte{}, false))
			path := filepath.Join(dirs.Snap, "erigondb.toml")
			require.NoError(t, os.WriteFile(path, []byte("step_size = 32\n"), 0o600))
			require.NoError(t, runSnapshotsReset(t, "--datadir", dirs.DataDir, "--caplin", "--preverified", "remote"))
			_, err := os.Stat(path)
			if os.IsNotExist(err) {
				t.Fatal("remote-listed EL file erigondb.toml was deleted")
			}
			require.NoError(t, err)
		})
	}
}

func TestResetSkipsUnplannedCaplinStateFile(t *testing.T) {
	dirs := resetReviewDatadir(t)
	name := "v1.1-000000-000100-BlockProposers.seg"
	path := filepath.Join(dirs.SnapCaplin, name)
	require.NoError(t, os.WriteFile(path, []byte(name), 0o600))
	require.NoError(t, os.WriteFile(dirs.PreverifiedPath(), []byte("'erigondb.toml' = '0000000000000000000000000000000000000001'\n"), 0o600))
	plan, err := reset.BuildCaplinResetPlan(t.Context(), dirs, nil, nil, true, log.New())
	require.NoError(t, err)
	require.Empty(t, plan.Files)
	require.NoError(t, runSnapshotsReset(t, "--caplin", "--datadir", dirs.DataDir))
	_, err = os.Stat(path)
	if os.IsNotExist(err) {
		t.Fatal("Caplin plan listed 0 files, EL pass deleted caplin/" + name)
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
