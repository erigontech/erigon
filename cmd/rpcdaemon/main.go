package main

import (
	"os"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := cli.RootCommand()
	rootCtx, rootCancel := utils.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		db, borDb, backend, txPool, mining, stateCache, blockReader, err := cli.RemoteServices(cmd.Context(), *cfg, logger, rootCancel)
		if err != nil {
			log.Error("Could not connect to DB", "error", err)
			return nil
		}
		defer db.Close()

		var ff *filters.Filters
		if backend != nil {
			ff = filters.New(rootCtx, backend, txPool, mining)
		} else {
			log.Info("filters are not supported in chaindata mode")
		}

		if err := cli.StartRpcServer(cmd.Context(), *cfg, commands.APIList(cmd.Context(), db, borDb, backend, txPool, mining, ff, stateCache, blockReader, *cfg, nil)); err != nil {
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
