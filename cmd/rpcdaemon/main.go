package main

import (
	"os"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := cli.RootCommand()
	rootCtx, rootCancel := common.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		logging.SetupLoggerCmd("rpcdaemon", cmd)
		db, borDb, backend, txPool, mining, stateCache, blockReader, ff, agg, err := cli.RemoteServices(ctx, *cfg, log.Root(), rootCancel)
		if err != nil {
			log.Error("Could not connect to DB", "err", err)
			return nil
		}
		defer db.Close()
		if borDb != nil {
			defer borDb.Close()
		}

		ethConfig := ethconfig.Defaults
		ethConfig.L2RpcUrl = cfg.L2RpcUrl

		// TODO: Replace with correct consensus Engine
		engine := ethash.NewFaker()
		// zkevm: the raw pool needed for limbo calls will not work if rpcdaemon is running as a standalone process.  Only the sequencer would have this detail
		// so we pass a nil raw pool here
		apiList := commands.APIList(db, borDb, backend, txPool, nil, mining, ff, stateCache, blockReader, agg, *cfg, engine, &ethconfig.Defaults, nil)
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
