package main

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/console/prompt"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/internal/flags"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/params"

	"github.com/urfave/cli"
)

func main() {
	app := MakeApp(runTurboGeth)
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runTurboGeth(ctx *cli.Context) {
	sync := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
	)

	tg := MakeTurboGethNode(ctx, sync)
	tg.Serve()
}

func MakeApp(action func(*cli.Context)) *cli.App {
	app := flags.NewApp("", "", "turbo-geth experimental cli")
	app.Action = action
	app.Flags = DefaultFlags
	app.Before = func(ctx *cli.Context) error {
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		prompt.Stdin.Close() // Resets terminal mode.
		return nil
	}
	return app
}

var DefaultFlags = []cli.Flag{
	utils.DataDirFlag,
	utils.KeyStoreDirFlag,
	utils.EthashDatasetDirFlag,
	utils.TxPoolLocalsFlag,
	utils.TxPoolNoLocalsFlag,
	utils.TxPoolJournalFlag,
	utils.TxPoolRejournalFlag,
	utils.TxPoolPriceLimitFlag,
	utils.TxPoolPriceBumpFlag,
	utils.TxPoolAccountSlotsFlag,
	utils.TxPoolGlobalSlotsFlag,
	utils.TxPoolAccountQueueFlag,
	utils.TxPoolGlobalQueueFlag,
	utils.TxPoolLifetimeFlag,
	utils.TxLookupLimitFlag,
	utils.StorageModeFlag,
	utils.HddFlag,
	utils.DatabaseFlag,
	utils.LMDBMapSizeFlag,
	utils.PrivateApiAddr,
	utils.ListenPortFlag,
	utils.NATFlag,
	utils.NoDiscoverFlag,
	utils.DiscoveryV5Flag,
	utils.NetrestrictFlag,
	utils.NodeKeyFileFlag,
	utils.NodeKeyHexFlag,
	utils.DNSDiscoveryFlag,
	utils.RopstenFlag,
	utils.RinkebyFlag,
	utils.GoerliFlag,
	utils.YoloV1Flag,
	utils.VMEnableDebugFlag,
	utils.NetworkIdFlag,
	utils.EthStatsURLFlag,
	utils.FakePoWFlag,
	utils.NoCompactionFlag,
	utils.GpoBlocksFlag,
	utils.GpoPercentileFlag,
	utils.EWASMInterpreterFlag,
	utils.EVMInterpreterFlag,
	utils.DebugProtocolFlag,
	utils.IPCDisabledFlag,
	utils.IPCPathFlag,
	utils.InsecureUnlockAllowedFlag,
	utils.MetricsEnabledFlag,
	utils.MetricsEnabledExpensiveFlag,
	utils.MetricsHTTPFlag,
	utils.MetricsPortFlag,
	configFileFlag,
}

var configFileFlag = cli.StringFlag{
	Name:  "config",
	Usage: "TOML configuration file",
}

type TurboGeth struct {
	stack   *node.Node
	backend *eth.Ethereum
}

func (tg *TurboGeth) Serve() error {
	defer tg.stack.Close()

	tg.run()

	tg.stack.Wait()

	return nil
}

func (tg *TurboGeth) run() {
	utils.StartNode(tg.stack)
	// we don't have accounts locally and we don't do mining
	// so these parts are ignored
	// see cmd/geth/main.go#startNode for full implementation
}

func MakeTurboGethNode(ctx *cli.Context, sync *stagedsync.StagedSync) *TurboGeth {
	node := MakeConfigNode(ctx)

	ethConfig := eth.DefaultConfig
	utils.SetEthConfig(ctx, node, &ethConfig)
	ethConfig.StagedSync = sync

	ethereum := utils.RegisterEthService(node, &ethConfig)

	return &TurboGeth{stack: node, backend: ethereum}
}

func MakeConfigNode(ctx *cli.Context) *node.Node {
	nodeConfig := node.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	nodeConfig.Version = params.Version
	nodeConfig.IPCPath = "tg.ipc"
	nodeConfig.Name = "turbo-geth"

	utils.SetNodeConfig(ctx, &nodeConfig)

	stack, err := node.New(&nodeConfig)
	if err != nil {
		utils.Fatalf("Failed to create turbo-geth node: %v", err)
	}

	return stack
}
