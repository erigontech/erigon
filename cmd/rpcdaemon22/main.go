package main

import (
	"os"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon22/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon22/commands"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := cli.RootCommand()
	rootCtx, rootCancel := common.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		logger := log.New()
		db, borDb, backend, txPool, mining, starknet, stateCache, blockReader, ff, agg, txNums, err := cli.RemoteServices(ctx, *cfg, logger, rootCancel)
		if err != nil {
			log.Error("Could not connect to DB", "err", err)
			return nil
		}
		defer db.Close()
		if borDb != nil {
			defer borDb.Close()
		}

		apiList := commands.APIList(db, borDb, backend, txPool, mining, starknet, ff, stateCache, blockReader, agg, txNums, *cfg)
		if err := cli.StartRpcServer(ctx, *cfg, apiList, nil); err != nil {
			log.Error(err.Error())
			return nil
		}

		return nil
	}

	if err := cmd.ExecuteContext(rootCtx); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
