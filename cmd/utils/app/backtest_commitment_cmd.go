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

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/fromdb"
	"github.com/erigontech/erigon/db/kv/dbcfg"
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
		&cli.StringFlag{Name: "output-dir", Usage: "directory to store all backtesting result artefacts such as cpu profiles"},
		&cli.BoolFlag{Name: "para-trie", Value: false, Usage: "use para trie, defaults to false"},
		&cli.BoolFlag{Name: "trie-warmup", Value: false, Usage: "enable trie warmup, defaults to false"},
	}),
	Action: func(ctx context.Context, cliCtx *cli.Command) error {
		logger, err := debug.SetupSimple(ctx, cliCtx, true /* root logger */)
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
		err = doBacktestCommitment(ctx, args, logger)
		if err != nil {
			logger.Error("encountered an issue while backtesting", "err", err)
			return err
		}
		return nil
	},
}

type backtestCommitmentArgs struct {
	from       uint64
	to         uint64
	tMinusN    int64
	dataDir    string
	outputDir  string
	paraTrie   bool
	trieWarmup bool
}

func doBacktestCommitment(ctx context.Context, args backtestCommitmentArgs, logger log.Logger) error {
	dirs, l, err := datadir.New(args.dataDir).MustFlock()
	if err != nil {
		return err
	}
	defer unlockDatadir(logger, l)
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
	db := snaps.TemporalDB
	defer db.Close()
	var opts []backtester.Opt
	if args.paraTrie {
		opts = append(opts, backtester.WithParaTrie(true))
	}
	if args.trieWarmup {
		opts = append(opts, backtester.WithTrieWarmup(true))
	}
	bt := backtester.New(logger, db, blockReader, args.outputDir, opts...)
	if args.tMinusN >= 0 {
		return bt.RunTMinusN(ctx, uint64(args.tMinusN))
	}
	return bt.Run(ctx, args.from, args.to)
}
