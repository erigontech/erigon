// Copyright 2024 The Erigon Authors
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
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/migrations"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/logging"
)

// mdbxWannaRecovery is MDBX_WANNA_RECOVERY from libmdbx mdbx.h, which mdbx-go does not export.
const mdbxWannaRecovery = mdbx.Errno(-30419)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for Erigon",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if chaindata == "" {
			chaindata = filepath.Join(datadirCli, "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		defer debug.Exit()
	},
}

func RootCommand() *cobra.Command {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)
	return rootCmd
}

func dbCfg(label kv.Label, path string) kv2.MdbxOpts {
	const ThreadsLimit = 9_000
	limiterB := semaphore.NewWeighted(ThreadsLimit)
	opts := kv2.New(label, log.New()).
		Path(path).
		RoTxsLimiter(limiterB).
		WriteMap(dbWriteMap).
		Accede(true) // integration tool: must not create db. must open db without stoping erigon.

	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts
}

func checkExistingDB(opts kv2.MdbxOpts) error {
	probeOpts := opts.Accede(true).Exclusive(false).WithTableCfg(func(kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{}
	})
	db, err := probeOpts.Readonly(true).Open(context.Background())
	if errors.Is(err, mdbxWannaRecovery) {
		// A read-write retry lets MDBX recover metadata left by an unclean shutdown.
		db, err = probeOpts.Readonly(false).Open(context.Background())
	}
	if err != nil {
		return err
	}
	defer db.Close()

	return db.View(context.Background(), func(tx kv.Tx) error {
		tables, err := tx.ListTables()
		if err != nil {
			return fmt.Errorf("list tables in database %s: %w", db.Path(), err)
		}
		if !slices.Contains(tables, kv.DatabaseInfo) {
			return fmt.Errorf("database %s has no %s table, so it is not a chain database", db.Path(), kv.DatabaseInfo)
		}
		return nil
	})
}

func openRawDB(opts kv2.MdbxOpts, applyMigrations bool, logger log.Logger) (kv.RwDB, error) {
	migrationDBs := map[kv.Label]bool{
		dbcfg.ChainDB:     true,
		dbcfg.ConsensusDB: true,
	}
	if _, ok := migrationDBs[opts.GetLabel()]; !ok {
		panic(opts.GetLabel())
	}
	// Apply migrations BEFORE the accede-mode open. In accede mode MDBX cannot
	// create new tables, so if a table was added to the schema after the DB was
	// originally created (e.g. BlockAccessList) the open would panic. The
	// exclusive-mode open used during migration creates any missing tables as a
	// side-effect, preventing the subsequent accede-mode open from panicking.
	if applyMigrations {
		dirs := datadir.New(datadirCli)
		migrationsDB, err := migrations.OpenMigrationsDB(dirs.Migrations, logger)
		if err != nil {
			return nil, fmt.Errorf("open migrations db: %w", err)
		}
		defer migrationsDB.Close()
		migrator := migrations.NewMigrator(opts.GetLabel())
		has, err := migrator.HasPendingMigrations(migrationsDB)
		if err != nil {
			return nil, err
		}
		if has {
			if err := checkExistingDB(opts); err != nil {
				return nil, err
			}
			logger.Info("Re-Opening DB in exclusive mode to apply DB migrations")
			rawDBExcl := opts.Accede(false).Exclusive(true).MustOpen()
			if err := migrator.Apply(rawDBExcl, migrationsDB, datadirCli, "", logger); err != nil {
				rawDBExcl.Close()
				return nil, err
			}
			rawDBExcl.Close()
		}
	}
	db := opts.MustOpen()
	if applyMigrations && opts.GetLabel() == dbcfg.ChainDB {
		if err := migrations.NewMigrator(dbcfg.ChainDB).StampVersion(db, ""); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

// isDefaultChaindata compares the flags as given: dirs.Chaindata is absolute and never matches a relative --datadir.
func isDefaultChaindata(chaindata, datadir string) bool {
	return chaindata == filepath.Join(datadir, "chaindata")
}

func openDB(ctx context.Context, opts kv2.MdbxOpts, applyMigrations bool, logger log.Logger) (tdb kv.TemporalRwDB, err error) {
	dirs := datadir.New(datadirCli)
	if applyMigrations && isDefaultChaindata(chaindata, datadirCli) {
		if err := backup.ApplyMigrations(ctx, dirs, logger); err != nil {
			return nil, err
		}
	}
	rawDB, err := openRawDB(opts, applyMigrations, logger)
	if err != nil {
		return nil, err
	}
	if err := CheckSaltFilesExist(dirs); err != nil {
		return nil, err
	}
	db, err := newTemporalDB(ctx, rawDB, logger)
	if err != nil {
		return nil, err
	}
	blockSnaps, _, err := allSnapshots(ctx, db, logger)
	if err != nil {
		return nil, err
	}
	logSnapshotStats(ctx, db, blockSnaps, logger)
	return db, nil
}
