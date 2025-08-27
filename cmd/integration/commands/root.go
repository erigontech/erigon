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

	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/migrations"
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

func openDB(opts kv2.MdbxOpts, applyMigrations bool, logger log.Logger) (tdb kv.TemporalRwDB, err error) {
	migrationDBs := map[kv.Label]bool{
		kv.ChainDB:         true,
		kv.ConsensusDB:     true,
		kv.HeimdallDB:      true,
		kv.PolygonBridgeDB: true,
	}
	if _, ok := migrationDBs[opts.GetLabel()]; !ok {
		panic(opts.GetLabel())
	}

	rawDB := opts.MustOpen()
	if applyMigrations {
		migrator := migrations.NewMigrator(opts.GetLabel())
		has, err := migrator.HasPendingMigrations(rawDB)
		if err != nil {
			return nil, err
		}
		if has {
			logger.Info("Re-Opening DB in exclusive mode to apply DB migrations")
			rawDB.Close()
			rawDB = opts.Exclusive(true).MustOpen()
			if err := migrator.Apply(rawDB, datadirCli, "", logger); err != nil {
				return nil, err
			}
			rawDB.Close()
			rawDB = opts.MustOpen()
		}
	}

	dirs := datadir.New(datadirCli)
	if err := CheckSaltFilesExist(dirs); err != nil {
		return nil, err
	}

	_, _, agg, _, _, _, err := allSnapshots(context.Background(), rawDB, logger)
	if err != nil {
		return nil, err
	}
	return temporal.New(rawDB, agg)
}
