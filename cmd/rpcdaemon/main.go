package main

import (
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/fdlimit"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

func main() {
	raiseFdLimit()
	cmd, cfg := cli.RootCommand()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		db, backend, err := cli.OpenDB(*cfg)
		if err != nil {
			log.Error("Could not connect to DB", "error", err)
			return nil
		}
		defer db.Close()

		var ff *filters.Filters
		if backend != nil {
			ff = filters.New(backend)
		} else {
			log.Info("filters are not supported in chaindata mode")
		}

		if err := cli.StartRpcServer(cmd.Context(), *cfg, commands.APIList(ethdb.NewObjectDatabase(db), backend, ff, *cfg, nil)); err != nil {
			log.Error(err.Error())
			return nil
		}
		return nil
	}

	if err := cmd.ExecuteContext(utils.RootContext()); err != nil {
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
