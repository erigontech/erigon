package main

import (
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := cli.RootCommand()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		db, backend, err := cli.OpenDB(*cfg)
		if err != nil {
			log.Error("Could not connect to DB", "error", err)
			return nil
		}
		defer db.Close()

		var apiList = commands.APIList(db, backend, *cfg, nil)
		return cli.StartRpcServer(cmd.Context(), *cfg, apiList)
	}

	if err := cmd.ExecuteContext(utils.RootContext()); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
