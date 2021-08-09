package main

import (
	"os"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/fdlimit"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	raiseFdLimit()
	cmd, cfg := cli.RootCommand()
	rootCtx, rootCancel := utils.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		db, backend, txPool, mining, err := cli.RemoteServices(*cfg, logger, rootCancel)
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

		if err := cli.StartRpcServer(cmd.Context(), *cfg, commands.APIList(cmd.Context(), db, backend, txPool, mining, ff, *cfg, nil)); err != nil {
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

// raiseFdLimit raises out the number of allowed file handles per process
func raiseFdLimit() {
	limit, err := fdlimit.Maximum()
	if err != nil {
		log.Error("Failed to retrieve file descriptor allowance", "error", err)
		return
	}
	if _, err = fdlimit.Raise(uint64(limit)); err != nil {
		log.Error("Failed to raise file descriptor allowance", "error", err)
	}
}
