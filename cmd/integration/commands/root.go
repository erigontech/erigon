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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/migrations"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/kv"
	kv2 "github.com/erigontech/erigon-lib/kv/mdbx"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"
)

func expandHomeDir(dirpath string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return dirpath
	}
	prefix := fmt.Sprintf("~%c", os.PathSeparator)
	if strings.HasPrefix(dirpath, prefix) {
		return filepath.Join(home, dirpath[len(prefix):])
	} else if dirpath == "~" {
		return home
	}
	return dirpath
}

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for Erigon",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		datadirCli = expandHomeDir(datadirCli)
		if chaindata == "" {
			chaindata = filepath.Join(datadirCli, "chaindata")
		} else {
			chaindata = expandHomeDir(chaindata)
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
	opts := kv2.NewMDBX(log.New()).Path(path).Label(label).RoTxsLimiter(limiterB).WriteMap(dbWriteMap)

	// integration tool don't intent to create db, then easiest way to open db - it's pass mdbx.Accede flag, which allow
	// to read all options from DB, instead of overriding them
	opts = opts.Accede()

	if databaseVerbosity != -1 {
		opts = opts.DBVerbosity(kv.DBVerbosityLvl(databaseVerbosity))
	}
	return opts
}

func openDB(opts kv2.MdbxOpts, applyMigrations bool, logger log.Logger) (kv.RwDB, error) {
	db := opts.MustOpen()
	if opts.GetLabel() == kv.ChainDB {
		if applyMigrations {
			migrator := migrations.NewMigrator(opts.GetLabel())
			has, err := migrator.HasPendingMigrations(db)
			if err != nil {
				return nil, err
			}
			if has {
				logger.Info("Re-Opening DB in exclusive mode to apply DB migrations")
				db.Close()
				db = opts.Exclusive().MustOpen()
				if err := migrator.Apply(db, datadirCli, "", logger); err != nil {
					return nil, err
				}
				db.Close()
				db = opts.MustOpen()
			}
		}

		_, _, agg, _ := allSnapshots(context.Background(), db, logger)
		tdb, err := temporal.New(db, agg)
		if err != nil {
			return nil, err
		}
		db = tdb
	}

	return db, nil
}
