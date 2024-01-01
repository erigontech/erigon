package tests

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/networks"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/cmd/devnet/services/polygon"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/log/v3"
)

func initDevnet(chainName string, dataDir string, producerCount int, logger log.Logger) (devnet.Devnet, error) {
	const baseRpcHost = "localhost"
	const baseRpcPort = 8545

	switch chainName {
	case networkname.BorDevnetChainName:
		heimdallGrpcAddr := polygon.HeimdallGrpcAddressDefault
		const sprintSize uint64 = 0
		return networks.NewBorDevnetWithLocalHeimdall(dataDir, baseRpcHost, baseRpcPort, heimdallGrpcAddr, sprintSize, producerCount, logger), nil

	case networkname.DevChainName:
		return networks.NewDevDevnet(dataDir, baseRpcHost, baseRpcPort, producerCount, logger), nil

	case "":
		envChainName, _ := os.LookupEnv("DEVNET_CHAIN")
		if envChainName == "" {
			envChainName = networkname.DevChainName
		}
		return initDevnet(envChainName, dataDir, producerCount, logger)

	default:
		return nil, fmt.Errorf("unknown network: '%s'", chainName)
	}
}

func ContextStart(t *testing.T, chainName string) (devnet.Context, error) {
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

	var network devnet.Devnet
	network, err := initDevnet(chainName, dataDir, int(producerCount), logger)
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
