package main

import (
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

func main() {
	cli.SetupDefaultLogger(log.LvlInfo)

	cmd, cfg := cli.RootCommand()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		db, txPool, err := cli.DefaultConnection(cfg)
		if err != nil {
			log.Error("Could not connect to remoteDb", "error", err)
			return nil
		}

		var rpcAPI = commands.GetAPI(db, txPool, cfg.API)
		cli.StartRpcServer(cfg, rpcAPI)
		sig := <-cmd.Context().Done()
		log.Info("Exiting...", "signal", sig)
		return nil
	}

	if err := cmd.ExecuteContext(cli.RootContext()); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
