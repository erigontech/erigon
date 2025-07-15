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

package tests

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/networks"
	"github.com/erigontech/erigon/cmd/devnet/services"
	"github.com/erigontech/erigon/cmd/devnet/services/polygon"
	"github.com/erigontech/erigon/turbo/debug"
)

func initDevnet(chainName string, dataDir string, producerCount int, gasLimit uint64, logger log.Logger, consoleLogLevel log.Lvl, dirLogLevel log.Lvl) (devnet.Devnet, error) {
	const baseRpcHost = "localhost"
	const baseRpcPort = 9545

	switch chainName {
	case networkname.BorDevnet:
		heimdallURL := polygon.HeimdallURLDefault
		const sprintSize uint64 = 0
		return networks.NewBorDevnetWithLocalHeimdall(dataDir, baseRpcHost, baseRpcPort, heimdallURL, sprintSize, producerCount, gasLimit, logger, consoleLogLevel, dirLogLevel), nil

	case networkname.Dev:
		return networks.NewDevDevnet(dataDir, baseRpcHost, baseRpcPort, producerCount, gasLimit, logger, consoleLogLevel, dirLogLevel), nil

	case "":
		envChainName, _ := os.LookupEnv("DEVNET_CHAIN")
		if envChainName == "" {
			envChainName = networkname.Dev
		}
		return initDevnet(envChainName, dataDir, producerCount, gasLimit, logger, consoleLogLevel, dirLogLevel)

	default:
		return nil, fmt.Errorf("unknown network: '%s'", chainName)
	}
}

func ContextStart(t *testing.T, chainName string) (devnet.Context, error) {
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		t.Skip("FIXME: TempDir RemoveAll cleanup error: remove dev-0\\clique\\db\\clique\\mdbx.dat: The process cannot access the file because it is being used by another process")
	}

	debug.RaiseFdLimit()
	logger := log.New()
	dataDir := t.TempDir()

	envProducerCount, _ := os.LookupEnv("PRODUCER_COUNT")
	if envProducerCount == "" {
		envProducerCount = "1"
	}

	producerCount, _ := strconv.ParseUint(envProducerCount, 10, 64)

	// TODO get log levels from env
	var dirLogLevel log.Lvl = log.LvlTrace
	var consoleLogLevel log.Lvl = log.LvlCrit

	var network devnet.Devnet
	network, err := initDevnet(chainName, dataDir, int(producerCount), 0, logger, consoleLogLevel, dirLogLevel)
	if err != nil {
		return nil, fmt.Errorf("ContextStart initDevnet failed: %w", err)
	}

	runCtx, err := network.Start(logger)
	if err != nil {
		return nil, fmt.Errorf("ContextStart devnet start failed: %w", err)
	}

	t.Cleanup(services.UnsubscribeAll)
	t.Cleanup(network.Stop)

	return runCtx, nil
}
