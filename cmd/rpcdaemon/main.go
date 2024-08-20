// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/jsonrpc"

	_ "github.com/erigontech/erigon/core/snaptype"        //hack
	_ "github.com/erigontech/erigon/polygon/bor/snaptype" //hack
)

func main() {
	cmd, cfg, polygonRPCCfg := cli.RootCommand()
	rootCtx, rootCancel := common.RootContext()
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		logger := debug.SetupCobra(cmd, "sentry")
		db, backend, txPool, mining, stateCache, blockReader, engine, ff, err := cli.RemoteServices(ctx, cfg, polygonRPCCfg, logger, rootCancel)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error("Could not connect to DB", "err", err)
			}
			return nil
		}
		defer db.Close()
		defer engine.Close()

		var bridgeReader bridge.ReaderService
		if cfg.WithDatadir && polygonRPCCfg.Enabled {
			bridgeReader, err = bridge.AssembleReader(ctx, cfg.DataDir, logger, polygonRPCCfg.StateReceiverContractAddress)
			if err != nil {
				return err
			}
			defer bridgeReader.Close()
		}

		apiList := jsonrpc.APIList(db, backend, txPool, mining, ff, stateCache, blockReader, cfg, engine, logger, bridgeReader)
		rpc.PreAllocateRPCMetricLabels(apiList)
		if err := cli.StartRpcServer(ctx, cfg, apiList, logger); err != nil {
			logger.Error(err.Error())
			return nil
		}

		return nil
	}

	if err := cmd.ExecuteContext(rootCtx); err != nil {
		fmt.Printf("ExecuteContext: %v\n", err)
		os.Exit(1)
	}
}
