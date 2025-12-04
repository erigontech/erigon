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
		&cli.StringFlag{Name: "output-dir", Required: true, Usage: "directory to store all backtesting result artefacts such as graphs, metrics, profiling, etc."},
	}),
	Action: func(cliCtx *cli.Context) error {
		ctx := cliCtx.Context
		logger, _, _, _, err := debug.Setup(cliCtx, true /* root logger */)
		if err != nil {
			panic(fmt.Errorf("rollback snapshots to block: could not setup logger: %w", err))
		}
		dataDir := cliCtx.String(utils.DataDirFlag.Name)
		from := cliCtx.Uint64("from")
		to := cliCtx.Uint64("to")
		outputDir := cliCtx.String("output-dir")
		err = doBacktestCommitment(ctx, from, to, dataDir, outputDir, logger)
		if err != nil {
			logger.Error("encountered an issue while backtesting", "err", err)
			return err
		}
		return nil
	},
}

func doBacktestCommitment(ctx context.Context, from uint64, to uint64, dataDir string, outputDir string, logger log.Logger) error {
	dirs, l, err := datadir.New(dataDir).MustFlock()
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
	_, _, _, br, agg, _, clean, err := openSnaps(ctx, cfg, dirs, chainDB, logger)
	if err != nil {
		return err
	}
	defer clean()
	blockReader, _ := br.IO()
	db, err := temporal.New(chainDB, agg)
	if err != nil {
		return err
	}
	defer db.Close()
	bt := backtester.New(logger, db, blockReader, outputDir)
	return bt.Run(ctx, from, to)
}
