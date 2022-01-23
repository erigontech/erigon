package main

import (
	"os"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func main() {
	cmd, cfg := cli.RootCommand()
	rootCtx, rootCancel := common.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		db, backend, txPool, mining, starknet, stateCache, blockReader, err := cli.RemoteServices(cmd.Context(), *cfg, logger, rootCancel)
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


		defaultAPIList, engineRpcApi :=commands.APIList(cmd.Context(), db, backend, txPool, mining, starknet, ff, stateCache, blockReader, *cfg, nil)

		var newAPIFlags []string
		
		for _, apiFlags := range(cfg.API){
			if apiFlags != "engine"{
				newAPIFlags = append(newAPIFlags, apiFlags)
			}
		}

		cfg.API = newAPIFlags
		if err := cli.StartRpcServer(cmd.Context(), *cfg, defaultAPIList); err != nil {
			log.Error(err.Error())
			return nil
		}

		if len(engineRpcApi) != 0{
			cfg.API = []string{"engine"}
			cfg.HttpPort = 8550
			if err := cli.StartRpcServer(cmd.Context(), *cfg, engineRpcApi); err != nil{
				log.Error(err.Error())
			return nil
			}
		}

		return nil
	}

	if err := cmd.ExecuteContext(rootCtx); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}
}
