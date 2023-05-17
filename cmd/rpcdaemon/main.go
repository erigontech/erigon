package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := cli.RootCommand()
	rootCtx, rootCancel := common.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "rpcdaemon"); err != nil {
			logger.Error("Setting up", "error", err)
			return err
		}
		db, borDb, backend, txPool, mining, stateCache, blockReader, ff, agg, err := cli.RemoteServices(ctx, *cfg, logger, rootCancel)
		if err != nil {
			logger.Error("Could not connect to DB", "err", err)
			return nil
		}
		defer db.Close()
		if borDb != nil {
			defer borDb.Close()
		}

		// TODO: Replace with correct consensus Engine
		engine := ethash.NewFaker()
		apiList := commands.APIList(db, borDb, backend, txPool, mining, ff, stateCache, blockReader, agg, *cfg, engine, logger)
		if err := cli.StartRpcServer(ctx, *cfg, apiList, nil, logger); err != nil {
			logger.Error(err.Error())
			return nil
		}

		return nil
	}

	if err := cmd.ExecuteContext(rootCtx); err != nil {
		fmt.Printf("ExecuteContext: %v\n", err)
		os.Exit(1)
	}
}
