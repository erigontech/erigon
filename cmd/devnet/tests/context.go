package tests

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/networks"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/cmd/devnet/services/polygon"
	"github.com/ledgerwatch/erigon/turbo/debug"
)

func initDevnet(chainName string, dataDir string, producerCount int, gasLimit uint64, logger log.Logger, consoleLogLevel log.Lvl, dirLogLevel log.Lvl) (devnet.Devnet, error) {
	const baseRpcHost = "localhost"
	const baseRpcPort = 9545

	switch chainName {
	case networkname.BorDevnetChainName:
		heimdallURL := polygon.HeimdallURLDefault
		const sprintSize uint64 = 0
		return networks.NewBorDevnetWithLocalHeimdall(dataDir, baseRpcHost, baseRpcPort, heimdallURL, sprintSize, producerCount, gasLimit, logger, consoleLogLevel, dirLogLevel), nil

	case networkname.DevChainName:
		return networks.NewDevDevnet(dataDir, baseRpcHost, baseRpcPort, producerCount, gasLimit, logger, consoleLogLevel, dirLogLevel), nil

	case "":
		envChainName, _ := os.LookupEnv("DEVNET_CHAIN")
		if envChainName == "" {
			envChainName = networkname.DevChainName
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
