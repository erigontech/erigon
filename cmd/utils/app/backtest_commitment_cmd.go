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

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/execution/commitment/backtester"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/node/ethconfig"
)

var backtestCommitmentCommand = cli.Command{
	Name:        "backtest-commitment",
	Description: "Perform historical backtesting of commitment calculation. Requires an archive node datadir with historical commitment.",
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.Uint64Flag{Name: "from", Value: 1, Usage: "block number to start historical backtesting from. Defaults to first block."},
		&cli.Uint64Flag{Name: "to", Value: math.MaxUint64, Usage: "block number to end historical backtesting at. Defaults to latest block with historical data in files."},
		&cli.Int64Flag{Name: "tMinusN", Value: -1, Usage: "number of blocks to backtest starting from latest block minus N. Alternative to [from,to). Defaults to -1, i.e. by default use [from,to)"},
		&cli.StringFlag{Name: "output-dir", Usage: "directory to store all backtesting result artefacts such as graphs, metrics, profiling, etc."},
		&cli.BoolFlag{Name: "para-trie", Value: false, Usage: "use para trie, defaults to false"},
		&cli.BoolFlag{Name: "trie-warmup", Value: false, Usage: "enable trie warmup, defaults to false"},
		&cli.Uint64Flag{Name: "metrics-top-n", Usage: "override the number of top blocks to show in the overview metrics page"},
		&cli.Uint64Flag{Name: "metrics-page-size", Usage: "override the number of blocks to show in the detailed block range metrics page"},
	}),
	Action: func(cliCtx *cli.Context) error {
		ctx := cliCtx.Context
		logger, err := debug.SetupSimple(cliCtx, true /* root logger */)
		if err != nil {
			panic(fmt.Errorf("backtest-commitment: could not setup logger: %w", err))
		}
		if cliCtx.IsSet("tMinusN") && (cliCtx.IsSet("from") || cliCtx.IsSet("to")) {
			return fmt.Errorf("cannot specify both [from,to) and tMinusN")
		}
		args := backtestCommitmentArgs{
			from:       cliCtx.Uint64("from"),
			to:         cliCtx.Uint64("to"),
			tMinusN:    cliCtx.Int64("tMinusN"),
			dataDir:    cliCtx.String(utils.DataDirFlag.Name),
			outputDir:  cliCtx.String("output-dir"),
			paraTrie:   cliCtx.Bool("para-trie"),
			trieWarmup: cliCtx.Bool("trie-warmup"),
		}
		if args.outputDir == "" {
			return fmt.Errorf("output-dir must be specified")
		}
		if cliCtx.IsSet("metrics-top-n") {
			v := cliCtx.Uint64("metrics-top-n")
			args.metricsTopN = &v
		}
		if cliCtx.IsSet("metrics-page-size") {
			v := cliCtx.Uint64("metrics-page-size")
			args.metricsPageSize = &v
		}
		err = doBacktestCommitment(ctx, args, logger)
		if err != nil {
			logger.Error("encountered an issue while backtesting", "err", err)
			return err
		}
		return nil
	},
	Subcommands: []*cli.Command{
		{
			Name: "compare-runs",
			Flags: joinFlags([]cli.Flag{
				&cli.StringSliceFlag{Name: "run-output-dirs", Required: true, Usage: "comma separated list of directories containing output of backtest-commitment runs to compare"},
				&cli.StringFlag{Name: "output-dir", Required: true, Usage: "directory to store comparison.html file"},
			}),
			Action: func(cliCtx *cli.Context) error {
				logger, err := debug.SetupSimple(cliCtx, true /* root logger */)
				if err != nil {
					panic(fmt.Errorf("backtest-commitment: compare-runs: could not setup logger: %w", err))
				}
				runOutputDirs := cliCtx.StringSlice("run-output-dirs")
				outputDir := cliCtx.String("output-dir")
				err = backtester.CompareRuns(runOutputDirs, outputDir, logger)
				if err != nil {
					logger.Error("encountered an issue while comparing backtest runs", "err", err)
					return err
				}
				return nil
			},
		},
	},
}

type backtestCommitmentArgs struct {
	from            uint64
	to              uint64
	tMinusN         int64
	dataDir         string
	outputDir       string
	paraTrie        bool
	trieWarmup      bool
	metricsTopN     *uint64
	metricsPageSize *uint64
}

func doBacktestCommitment(ctx context.Context, args backtestCommitmentArgs, logger log.Logger) error {
	dirs, l, err := datadir.New(args.dataDir).MustFlock()
	if err != nil {
		return err
	}
	defer func() {
		err := l.Unlock()
		if err != nil {
			logger.Error("failed to unlock datadir", "err", err)
		}
	}()
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
	var opts []backtester.Opt
	if args.paraTrie {
		opts = append(opts, backtester.WithParaTrie(true))
	}
	if args.trieWarmup {
		opts = append(opts, backtester.WithTrieWarmup(true))
	}
	if args.metricsTopN != nil {
		opts = append(opts, backtester.WithChartsTopN(*args.metricsTopN))
	}
	if args.metricsPageSize != nil {
		opts = append(opts, backtester.WithChartsPageSize(*args.metricsPageSize))
	}
	bt := backtester.New(logger, db, blockReader, args.outputDir, opts...)
	if args.tMinusN >= 0 {
		return bt.RunTMinusN(ctx, uint64(args.tMinusN))
	}
	return bt.Run(ctx, args.from, args.to)
}
