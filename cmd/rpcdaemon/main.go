package main

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/rpc"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := rpc.RootCommand()
	if err := utils.SetupCobra(cmd); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		db, txPool, err := rpc.OpenDB(cfg)
		if err != nil {
			log.Error("Could not connect to remoteDb", "error", err)
			return nil
		}

		var rpcAPI = commands.GetAPI(db, txPool, cfg.API, cfg.Gascap)
		rpc.StartRpcServer(cfg, rpcAPI)
		sig := <-cmd.Context().Done()
		log.Info("Exiting...", "signal", sig)
		return nil
	}

	if err := cmd.ExecuteContext(utils.RootContext()); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
