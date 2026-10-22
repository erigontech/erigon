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
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"

	"github.com/erigontech/erigon/cmd/erigon/node"
	"github.com/erigontech/erigon/cmd/utils"
	erigonapp "github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/diagnostics/syscheck"
	"github.com/erigontech/erigon/eez"
	eezconfig "github.com/erigontech/erigon/eez/config"
	eezrpc "github.com/erigontech/erigon/eez/rpc"
	"github.com/erigontech/erigon/execution/types"
	erigoncli "github.com/erigontech/erigon/node/cli"
	"github.com/erigontech/erigon/node/debug"
)

func main() {
	var err error
	common.WithProfilersMain(func() {
		app := erigonapp.MakeApp("erigon", runErigon, slices.Concat(erigoncli.DefaultFlags, eez.Flags))
		erigonapp.InitGenesisHook = eezGenesis
		err = app.Run(context.Background(), os.Args)
		if err != nil {
			_, printErr := fmt.Fprintln(os.Stderr, err)
			if printErr != nil {
				log.Warn("Fprintln error", "err", printErr)
			}
		}
	})
	if err != nil {
		os.Exit(1)
	}
}

// eezGenesis writes the l2 section the EEZ flags set into the genesis init
// commits, and an EEZ genesis under <datadir>/eez.
func eezGenesis(cliCtx *cli.Command, genesis *types.Genesis) error {
	cfg, err := eez.ConfigFromCLI(cliCtx)
	if err != nil || cfg == nil {
		return err
	}
	if err := cfg.L2.ApplyGenesis(genesis.Config, genesis.GasLimit); err != nil {
		return err
	}
	if _, ok := genesis.Config.L2.(*eezconfig.L2Config); !ok {
		return nil
	}
	return cliCtx.Set(utils.DataDirFlag.Name, eezDataDir(cliCtx.String(utils.DataDirFlag.Name)))
}

func runErigon(ctx context.Context, cliCtx *cli.Command) (err error) {
	eezCfg, err := eez.ConfigFromCLI(cliCtx)
	if err != nil {
		return err
	}
	follows := eezCfg != nil && eezCfg.Follower

	logger, tracer, metricsMux, pprofMux, err := debug.Setup(ctx, cliCtx, true /* rootLogger */)
	if err != nil {
		return
	}

	syscheck.CheckKernelAllocationHints(ctx, logger)

	debugMux := cmp.Or(metricsMux, pprofMux)

	// Log the full command used to start the program (with sensitive info like URLs and IP addresses redacted)
	logger.Info("Startup command", "cmd", log.RedactArgs(os.Args))

	// initializing the node and providing the current git commit there

	logger.Info("Build info", "git_branch", version.GitBranch, "git_tag", version.GitTag, "git_commit", version.GitCommit)
	erigonInfoGauge := metrics.GetOrCreateGauge(fmt.Sprintf(`erigon_info{version=%q,commit=%q}`, version.VersionNoMeta, version.GitCommit))
	erigonInfoGauge.Set(1)

	nodeCfg, err := node.NewNodConfigUrfave(cliCtx, debugMux, logger)
	if err != nil {
		return err
	}
	if eezCfg != nil {
		nodeCfg.Http.RenderRegisteredTxTypes = true
	}
	if follows {
		if err := configureFollowerNode(nodeCfg); err != nil {
			return err
		}
	}
	if err := backup.ApplyMigrations(ctx, nodeCfg.Dirs, logger); err != nil {
		return err
	}

	ethCfg := node.NewEthConfigUrfave(ctx, cliCtx, nodeCfg, logger)
	var eezAPI *eezrpc.API
	if eezCfg != nil {
		eezAPI = eezrpc.NewAPI()
		ethCfg.ExtraAPIs = append(ethCfg.ExtraAPIs, eezAPI.API())
	}
	if follows {
		configureFollowerEth(ethCfg)
	}

	ethNode, err := node.New(ctx, nodeCfg, ethCfg, logger, tracer)
	if err != nil {
		log.Error("Erigon startup", "err", err)
		return err
	}
	var followerSvc interface{ Err() error }
	if follows {
		if followerSvc, err = startFollower(ctx, ethNode, eezCfg, eezAPI, nodeCfg.Dirs, logger); err != nil {
			ethNode.Close()
			return err
		}
	}

	//diagnostics.Setup(cliCtx, ethNode, metricsMux, pprofMux)

	err = ethNode.Serve()
	if err != nil {
		log.Error("error while serving an Erigon node", "err", err)
	}
	if followerSvc != nil && err == nil {
		err = haltError(followerSvc.Err())
	}
	return err
}
