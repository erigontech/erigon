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
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"

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

var execHashCommand = cli.Command{
	Name:        "exec-hash",
	Description: "Re-execute historical transactions with proof-of-execution hashing. Produces a per-transaction execution hash (keccak256 of the EVM instruction trace) for each block in the specified range.",
	Flags: joinFlags([]cli.Flag{
		&utils.DataDirFlag,
		&cli.Uint64Flag{Name: "from", Value: 1, Usage: "block number to start from"},
		&cli.Uint64Flag{Name: "to", Value: math.MaxUint64, Usage: "block number to end at (0 = latest available)"},
		&cli.StringFlag{Name: "output-dir", Usage: "directory for CSV results"},
	}),
	Action: func(cliCtx *cli.Context) error {
		ctx := cliCtx.Context
		logger, err := debug.SetupSimple(cliCtx, true)
		if err != nil {
			return fmt.Errorf("exec-hash: could not setup logger: %w", err)
		}
		args := execHashArgs{
			from:      cliCtx.Uint64("from"),
			to:        cliCtx.Uint64("to"),
			dataDir:   cliCtx.String(utils.DataDirFlag.Name),
			outputDir: cliCtx.String("output-dir"),
		}
		if args.dataDir == "" {
			return fmt.Errorf("--datadir must be specified")
		}
		return doExecHash(ctx, args, logger)
	},
}

type execHashArgs struct {
	from      uint64
	to        uint64
	dataDir   string
	outputDir string
}

func doExecHash(ctx context.Context, args execHashArgs, logger log.Logger) error {
	dirs, l, err := datadir.New(args.dataDir).MustFlock()
	if err != nil {
		return err
	}
	defer func() {
		if err := l.Unlock(); err != nil {
			logger.Error("failed to unlock datadir", "err", err)
		}
	}()

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

	runner := poc.NewExecRunner(logger, db, blockReader, chainConfig, poc.NilEngine())
	results, err := runner.RunBlocks(ctx, args.from, toBlock)
	if err != nil {
		return err
	}

	if args.outputDir != "" {
		return writeExecHashCSV(args.outputDir, results)
	}

	return nil
}

func writeExecHashCSV(outputDir string, results []poc.ExecBlockResult) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(outputDir, "exec_hashes.csv"))
	if err != nil {
		return err
	}
	defer f.Close()

	cw := csv.NewWriter(f)
	defer cw.Flush()

	if err := cw.Write([]string{"block_num", "tx_index", "exec_hash", "transition_hash", "ops", "block_elapsed_ns"}); err != nil {
		return err
	}
	for _, br := range results {
		for i, h := range br.ExecHashes {
			if err := cw.Write([]string{
				fmt.Sprintf("%d", br.BlockNum),
				fmt.Sprintf("%d", i),
				fmt.Sprintf("%x", h),
				fmt.Sprintf("%x", br.TransitionHashes[i]),
				fmt.Sprintf("%d", br.TxOps[i]),
				fmt.Sprintf("%d", br.Elapsed.Nanoseconds()),
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
