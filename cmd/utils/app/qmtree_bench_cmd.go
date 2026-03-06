// Copyright 2025 The Erigon Authors
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
	"fmt"
	"math"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/execution/commitment/qmtree/poc"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/ethconfig"
)

var qmtreeBenchCommand = cli.Command{
	Name:        "qmtree-bench",
	Description: "Build a qmtree over historical state changes and measure performance. Requires a synced datadir with state history (accounts + storage domains). Commitment domain is NOT required.",
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.Uint64Flag{Name: "from", Value: 1, Usage: "block number to start from"},
		&cli.Uint64Flag{Name: "to", Value: math.MaxUint64, Usage: "block number to end at (0 = latest available)"},
		&cli.StringFlag{Name: "output-dir", Usage: "directory for CSV results and metrics"},
		&cli.StringFlag{Name: "tree-dir", Usage: "directory for persisted qmtree data (entries + twigs). If empty, uses a temp dir that is cleaned up."},
	}),
	Action: func(cliCtx *cli.Context) error {
		ctx := cliCtx.Context
		logger, err := debug.SetupSimple(cliCtx, true /* root logger */)
		if err != nil {
			return fmt.Errorf("qmtree-bench: could not setup logger: %w", err)
		}
		args := qmtreeBenchArgs{
			from:      cliCtx.Uint64("from"),
			to:        cliCtx.Uint64("to"),
			dataDir:   cliCtx.String(utils.DataDirFlag.Name),
			outputDir: cliCtx.String("output-dir"),
			treeDir:   cliCtx.String("tree-dir"),
		}
		if args.dataDir == "" {
			return fmt.Errorf("--datadir must be specified")
		}
		if args.outputDir == "" {
			return fmt.Errorf("--output-dir must be specified")
		}
		return doQmtreeBench(ctx, args, logger)
	},
}

type qmtreeBenchArgs struct {
	from      uint64
	to        uint64
	dataDir   string
	outputDir string
	treeDir   string
}

func doQmtreeBench(ctx context.Context, args qmtreeBenchArgs, logger log.Logger) error {
	dirs, l, err := datadir.New(args.dataDir).MustFlock()
	if err != nil {
		return err
	}
	defer func() {
		if err := l.Unlock(); err != nil {
			logger.Error("failed to unlock datadir", "err", err)
		}
	}()

	// Pre-check: verify chaindata mdbx file exists before MustOpen panics.
	mdbxPath := dirs.Chaindata + "/mdbx.dat"
	if _, err := os.Stat(mdbxPath); os.IsNotExist(err) {
		return fmt.Errorf("chaindata database not found at %s\nEnsure --datadir points to a synced Erigon datadir with state history", mdbxPath)
	}

	chainDB := dbCfg(dbcfg.ChainDB, dirs.Chaindata).MustOpen()
	defer chainDB.Close()

	chainConfig := fromdb.ChainConfig(chainDB)
	cfg := ethconfig.NewSnapCfg(false, true, true, chainConfig.ChainName)
	snaps, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()

	blockReader, _ := snaps.BlockRetire.IO()
	db, err := temporal.New(chainDB, snaps.Aggregator)
	if err != nil {
		return err
	}
	defer db.Close()

	toBlock := args.to
	if toBlock == math.MaxUint64 {
		toBlock = 0 // Runner interprets 0 as "latest"
	}

	runner := poc.NewRunner(logger, db, blockReader, args.outputDir)
	if args.treeDir != "" {
		runner = runner.WithDataDir(args.treeDir)
	}

	_, err = runner.Run(ctx, args.from, toBlock)
	return err
}
