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

	cmd := cli.RootCommand()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		commands.Daemon(cmd, cli.DefaultConfig)
		return nil
	}

	if err := cmd.ExecuteContext(cli.RootContext()); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
