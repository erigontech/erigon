package rpcdaemon

import (
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func RunDaemon() {
	cmd, cfg := cli.RootCommand()
	setupCfg(cfg)
	rootCtx, rootCancel := common.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		logger := log.New()
		time.Sleep(100 * time.Millisecond)
		db, borDb, backend, txPool, mining, starknet, stateCache, blockReader, ff, err := cli.RemoteServices(ctx, *cfg, logger, rootCancel)
		if err != nil {
			log.Error("Could not connect to DB", "err", err)
			return nil
		}
		defer db.Close()
		if borDb != nil {
			defer borDb.Close()
		}

		apiList := commands.APIList(db, borDb, backend, txPool, mining, starknet, ff, stateCache, blockReader, *cfg)
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

func setupCfg(cfg *httpcfg.HttpCfg) {
	cfg.WebsocketEnabled = true
	cfg.API = []string{"eth", "erigon", "web3", "net", "debug", "trace", "txpool"}
}
