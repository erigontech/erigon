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
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/migrations"
)

const uncleanShutdownPathEnv = "ERIGON_TEST_UNCLEAN_SHUTDOWN_PATH"

func TestOpenRawDBCreatesTableForPendingMigration(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	dataDir := useTestDatadir(t)
	chaindataPath := filepath.Join(dataDir, "chaindata")

	createDBWithoutPtcTable(t, dbcfg.ChainDB, chaindataPath, kv.ChaindataTablesCfg)

	markMigrationsAppliedExcept(t, dataDir, logger, "create_ptc_window_epoch_table")

	db, err := openRawDB(dbCfg(dbcfg.ChainDB, chaindataPath), true, logger)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		_, err := tx.GetOne(kv.PtcWindowEpochTable, nil)
		return err
	}))

	requirePendingMigrations(t, dataDir, logger, false)
}

func TestOpenRawDBCreatesTableAfterUncleanShutdown(t *testing.T) {
	ctx := t.Context()
	logger := log.New()
	dataDir := useTestDatadir(t)
	chaindataPath := filepath.Join(dataDir, "chaindata")

	createDBWithoutPtcTable(t, dbcfg.ChainDB, chaindataPath, kv.ChaindataTablesCfg)
	leaveChaindataUnclean(t, chaindataPath)
	probe, err := dbCfg(dbcfg.ChainDB, chaindataPath).
		Readonly(true).
		Exclusive(false).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.TableCfg{} }).
		Open(ctx)
	if probe != nil {
		probe.Close()
	}
	require.ErrorIs(t, err, mdbxWannaRecovery)
	markMigrationsAppliedExcept(t, dataDir, logger, "create_ptc_window_epoch_table")

	db, err := openRawDB(dbCfg(dbcfg.ChainDB, chaindataPath), true, logger)
	require.NoError(t, err)
	t.Cleanup(db.Close)
	require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
		_, err := tx.GetOne(kv.PtcWindowEpochTable, nil)
		return err
	}))

	requirePendingMigrations(t, dataDir, logger, false)
}

func TestOpenRawDBUncleanShutdownHelperProcess(t *testing.T) {
	path := os.Getenv(uncleanShutdownPathEnv)
	if path == "" {
		return
	}
	tables := maps.Clone(kv.ChaindataTablesCfg)
	delete(tables, kv.PtcWindowEpochTable)
	db := kv2.New(dbcfg.ChainDB, log.New()).
		Path(path).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return tables }).
		SafeNoSync().
		PollerSyncPeriod(time.Hour).
		SyncBytes(10 * datasize.GB).
		SyncInCommitIfUnsyncedFor(time.Hour).
		MustOpen()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.DatabaseInfo, []byte("unclean"), []byte("shutdown"))
	}))
	os.Exit(0)
}

func TestOpenRawDBRejectsMissingDatabaseForPendingMigration(t *testing.T) {
	logger := log.New()
	dataDir := useTestDatadir(t)
	missingPath := filepath.Join(dataDir, "missing-chaindata")

	db, err := openRawDB(dbCfg(dbcfg.ChainDB, missingPath), true, logger)
	if db != nil {
		t.Cleanup(db.Close)
	}
	require.ErrorIs(t, err, kv2.ErrDBDoesNotExists)
	require.Equal(t, 1, strings.Count(err.Error(), missingPath))
	require.NoDirExists(t, missingPath)

	requirePendingMigrations(t, dataDir, logger, true)
}

func TestOpenRawDBRejectsEmptyDatabaseForPendingMigration(t *testing.T) {
	logger := log.New()
	dataDir := useTestDatadir(t)
	databasePath := filepath.Join(dataDir, "chaindata")
	require.NoError(t, os.Mkdir(databasePath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(databasePath, "mdbx.dat"), nil, 0o600))
	markMigrationsAppliedExcept(t, dataDir, logger, "create_ptc_window_epoch_table")

	db, err := openRawDB(dbCfg(dbcfg.ChainDB, databasePath), true, logger)
	if db != nil {
		db.Close()
	}
	require.Error(t, err)
	require.Zero(t, databaseFileSize(t, databasePath))
	requirePendingMigrations(t, dataDir, logger, true)
}

func TestOpenRawDBRejectsNonChaindataForPendingMigration(t *testing.T) {
	logger := log.New()
	dataDir := useTestDatadir(t)
	databasePath := filepath.Join(dataDir, "chaindata")
	inspectOpts := createSmallTxPoolDB(t, databasePath)
	before := tableNames(t, inspectOpts)
	beforeSize := databaseFileSize(t, databasePath)
	markMigrationsAppliedExcept(t, dataDir, logger, "create_ptc_window_epoch_table")

	db, err := openRawDB(dbCfg(dbcfg.ChainDB, databasePath), true, logger)
	if db != nil {
		db.Close()
	}
	require.Error(t, err)
	require.NotContains(t, err.Error(), "run_migrations")
	require.Contains(t, err.Error(), databasePath)
	require.Equal(t, beforeSize, databaseFileSize(t, databasePath))
	require.Equal(t, before, tableNames(t, inspectOpts))
	requirePendingMigrations(t, dataDir, logger, true)
}

func markMigrationsAppliedExcept(t *testing.T, dataDir string, logger log.Logger, excluded ...string) {
	t.Helper()
	migrationsDB, err := migrations.OpenMigrationsDB(filepath.Join(dataDir, "migrations"), logger)
	require.NoError(t, err)
	defer migrationsDB.Close()
	require.NoError(t, migrationsDB.Update(t.Context(), func(tx kv.RwTx) error {
		for _, migration := range migrations.NewMigrator(dbcfg.ChainDB).Migrations {
			if slices.Contains(excluded, migration.Name) {
				continue
			}
			if err := tx.Put(kv.Migrations, []byte(migration.Name), nil); err != nil {
				return err
			}
		}
		return nil
	}))
}

func requirePendingMigrations(t *testing.T, dataDir string, logger log.Logger, want bool) {
	t.Helper()
	migrationsDB, err := migrations.OpenMigrationsDB(filepath.Join(dataDir, "migrations"), logger)
	require.NoError(t, err)
	defer migrationsDB.Close()
	hasPending, err := migrations.NewMigrator(dbcfg.ChainDB).HasPendingMigrations(migrationsDB)
	require.NoError(t, err)
	require.Equal(t, want, hasPending)
}

func createDBWithoutPtcTable(t *testing.T, label kv.Label, path string, base kv.TableCfg) kv2.MdbxOpts {
	t.Helper()
	tables := maps.Clone(base)
	delete(tables, kv.PtcWindowEpochTable)
	opts := kv2.New(label, log.New()).
		Path(path).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return tables })
	db, err := opts.Open(t.Context())
	require.NoError(t, err)
	db.Close()
	return opts
}

func createSmallTxPoolDB(t *testing.T, path string) kv2.MdbxOpts {
	t.Helper()
	opts := kv2.New(dbcfg.TxPoolDB, log.New()).
		Path(path).
		MapSize(64 * datasize.MB).
		GrowthStep(2 * datasize.MB)
	db, err := opts.Open(t.Context())
	require.NoError(t, err)
	db.Close()
	return opts
}

func databaseFileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(filepath.Join(path, "mdbx.dat"))
	require.NoError(t, err)
	return info.Size()
}

func leaveChaindataUnclean(t *testing.T, path string) {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestOpenRawDBUncleanShutdownHelperProcess$")
	cmd.Env = append(os.Environ(), uncleanShutdownPathEnv+"="+path)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
}

func useTestDatadir(t *testing.T) string {
	t.Helper()
	dataDir := t.TempDir()
	previousDatadir := datadirCli
	datadirCli = dataDir
	t.Cleanup(func() { datadirCli = previousDatadir })
	return dataDir
}

func tableNames(t *testing.T, opts kv2.MdbxOpts) []string {
	t.Helper()
	db, err := opts.Accede(true).Open(t.Context())
	require.NoError(t, err)
	defer db.Close()

	var names []string
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		names, err = tx.ListTables()
		return err
	}))
	slices.Sort(names)
	return names
}
