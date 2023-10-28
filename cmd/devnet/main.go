package main

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"os"
	"os/signal"
	"path/filepath"
	dbg "runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	_ "github.com/ledgerwatch/erigon/cmd/devnet/accounts/steps"
	_ "github.com/ledgerwatch/erigon/cmd/devnet/admin"
	_ "github.com/ledgerwatch/erigon/cmd/devnet/contracts/steps"
	account_services "github.com/ledgerwatch/erigon/cmd/devnet/services/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/services/polygon"
	"github.com/ledgerwatch/erigon/cmd/devnet/transactions"
	"github.com/ledgerwatch/erigon/core/types"

	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon/cmd/devnet/args"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/app"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/urfave/cli/v2"
)

var (
	DataDirFlag = flags.DirectoryFlag{
		Name:     "datadir",
		Usage:    "Data directory for the devnet",
		Value:    flags.DirectoryString(""),
		Required: true,
	}

	ChainFlag = cli.StringFlag{
		Name:  "chain",
		Usage: "The devnet chain to run (dev,bor-devnet)",
		Value: networkname.DevChainName,
	}

	ScenariosFlag = cli.StringFlag{
		Name:  "scenarios",
		Usage: "Scenarios to be run on the devnet chain",
		Value: "dynamic-tx-node-0",
	}

	BaseRpcHostFlag = cli.StringFlag{
		Name:  "rpc.host",
		Usage: "The host of the base RPC service",
		Value: "localhost",
	}

	BaseRpcPortFlag = cli.IntFlag{
		Name:  "rpc.port",
		Usage: "The port of the base RPC service",
		Value: 8545,
	}

	WithoutHeimdallFlag = cli.BoolFlag{
		Name:  "bor.withoutheimdall",
		Usage: "Run without Heimdall service",
	}

	LocalHeimdallFlag = cli.BoolFlag{
		Name:  "bor.localheimdall",
		Usage: "Run with a devnet local Heimdall service",
	}

	HeimdallgRPCAddressFlag = cli.StringFlag{
		Name:  "bor.heimdallgRPC",
		Usage: "Address of Heimdall gRPC service",
		Value: "localhost:8540",
	}

	BorSprintSizeFlag = cli.IntFlag{
		Name:  "bor.sprintsize",
		Usage: "The bor sprint size to run",
	}

	MetricsEnabledFlag = cli.BoolFlag{
		Name:  "metrics",
		Usage: "Enable metrics collection and reporting",
	}

	MetricsNodeFlag = cli.IntFlag{
		Name:  "metrics.node",
		Usage: "Which node of the cluster to attach to",
		Value: 0,
	}

	MetricsPortFlag = cli.IntFlag{
		Name:  "metrics.port",
		Usage: "Metrics HTTP server listening port",
		Value: metrics.DefaultConfig.Port,
	}

	DiagnosticsURLFlag = cli.StringFlag{
		Name:  "diagnostics.url",
		Usage: "URL of the diagnostics system provided by the support team, include unique session PIN",
	}

	insecureFlag = cli.BoolFlag{
		Name:  "insecure",
		Usage: "Allows communication with diagnostics system using self-signed TLS certificates",
	}

	metricsURLsFlag = cli.StringSliceFlag{
		Name:  "debug.urls",
		Usage: "internal flag",
	}

	WaitFlag = cli.BoolFlag{
		Name:  "wait",
		Usage: "Wait until interrupted after all scenarios have run",
	}
)

type PanicHandler struct {
}

func (ph PanicHandler) Log(r *log.Record) error {
	fmt.Printf("Msg: %s\nStack: %s\n", r.Msg, dbg.Stack())
	os.Exit(1)
	return nil
}

func main() {

	debug.RaiseFdLimit()

	app := cli.NewApp()
	app.Version = params.VersionWithCommit(params.GitCommit)
	app.Action = func(ctx *cli.Context) error {
		return action(ctx)
	}
	app.Flags = []cli.Flag{
		&DataDirFlag,
		&ChainFlag,
		&ScenariosFlag,
		&BaseRpcHostFlag,
		&BaseRpcPortFlag,
		&WithoutHeimdallFlag,
		&LocalHeimdallFlag,
		&HeimdallgRPCAddressFlag,
		&BorSprintSizeFlag,
		&MetricsEnabledFlag,
		&MetricsNodeFlag,
		&MetricsPortFlag,
		&DiagnosticsURLFlag,
		&insecureFlag,
		&metricsURLsFlag,
		&WaitFlag,
		&logging.LogVerbosityFlag,
		&logging.LogConsoleVerbosityFlag,
		&logging.LogDirVerbosityFlag,
	}

	app.After = func(ctx *cli.Context) error {
		// unsubscribe from all the subscriptions made
		services.UnsubscribeAll()
		return nil
	}
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
}

const (
	recipientAddress        = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	sendValue        uint64 = 10000
)

func action(ctx *cli.Context) error {
	dataDir := ctx.String("datadir")
	logsDir := filepath.Join(dataDir, "logs")

	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return err
	}

	logger := logging.SetupLoggerCtx("devnet", ctx, false /* rootLogger */)

	// Make root logger fail
	log.Root().SetHandler(PanicHandler{})

	// clear all the dev files
	if err := devnetutils.ClearDevDB(dataDir, logger); err != nil {
		return err
	}

	network, err := initDevnet(ctx, logger)

	if err != nil {
		return err
	}

	metrics := ctx.Bool("metrics")

	if metrics {
		// TODO should get this from the network as once we have multiple nodes we'll need to iterate the
		// nodes and create a series of urls - for the moment only one is supported
		ctx.Set("metrics.urls", fmt.Sprintf("http://localhost:%d/debug/", ctx.Int("metrics.port")))
	}

	// start the network with each node in a go routine
	logger.Info("Starting Devnet")

	runCtx, err := network.Start(ctx, logger)

	if err != nil {
		return fmt.Errorf("Devnet start failed: %w", err)
	}

	go func() {
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

		switch s := <-signalCh; s {
		case syscall.SIGTERM:
			logger.Info("Stopping networks")
			network.Stop()
		case syscall.SIGINT:
			logger.Info("Terminating network")
			os.Exit(-int(syscall.SIGINT))
		}
	}()

	diagnosticsUrl := ctx.String("diagnostics.url")

	if metrics && len(diagnosticsUrl) > 0 {
		go func() {
			app.ConnectDiagnostics(ctx, logger)
		}()
	}

	if ctx.String(ChainFlag.Name) == networkname.DevChainName {
		transactions.MaxNumberOfEmptyBlockChecks = 30
	}

	scenarios.Scenarios{
		"dynamic-tx-node-0": {
			Context: runCtx.
				WithCurrentNetwork(0).
				WithCurrentNode(0),
			Steps: []*scenarios.Step{
				{Text: "InitSubscriptions", Args: []any{[]requests.SubMethod{requests.Methods.ETHNewHeads}}},
				{Text: "PingErigonRpc"},
				{Text: "CheckTxPoolContent", Args: []any{0, 0, 0}},
				{Text: "SendTxWithDynamicFee", Args: []any{recipientAddress, accounts.DevAddress, sendValue}},
				{Text: "AwaitBlocks", Args: []any{2 * time.Second}},
			},
		},
		"dynamic-tx-any-node": {
			Context: runCtx.WithCurrentNetwork(0),
			Steps: []*scenarios.Step{
				{Text: "InitSubscriptions", Args: []any{[]requests.SubMethod{requests.Methods.ETHNewHeads}}},
				{Text: "PingErigonRpc"},
				{Text: "CheckTxPoolContent", Args: []any{0, 0, 0}},
				{Text: "SendTxWithDynamicFee", Args: []any{recipientAddress, accounts.DevAddress, sendValue}},
				{Text: "AwaitBlocks", Args: []any{2 * time.Second}},
			},
		},
		"call-contract": {
			Context: runCtx.WithCurrentNetwork(0),
			Steps: []*scenarios.Step{
				{Text: "InitSubscriptions", Args: []any{[]requests.SubMethod{requests.Methods.ETHNewHeads}}},
				{Text: "DeployAndCallLogSubscriber", Args: []any{accounts.DevAddress}},
			},
		},
		"state-sync": {
			Steps: []*scenarios.Step{
				{Text: "InitSubscriptions", Args: []any{[]requests.SubMethod{requests.Methods.ETHNewHeads}}},
				{Text: "CreateAccountWithFunds", Args: []any{networkname.DevChainName, "root-funder", 200.0}},
				{Text: "CreateAccountWithFunds", Args: []any{networkname.BorDevnetChainName, "child-funder", 200.0}},
				{Text: "DeployChildChainReceiver", Args: []any{"child-funder"}},
				{Text: "DeployRootChainSender", Args: []any{"root-funder"}},
				{Text: "GenerateSyncEvents", Args: []any{"root-funder", 10, 2, 2}},
				{Text: "ProcessRootTransfers", Args: []any{"root-funder", 10, 2, 2}},
				{Text: "BatchProcessRootTransfers", Args: []any{"root-funder", 1, 10, 2, 2}},
			},
		},
		"child-chain-exit": {
			Steps: []*scenarios.Step{
				{Text: "CreateAccountWithFunds", Args: []any{networkname.DevChainName, "root-funder", 200.0}},
				{Text: "CreateAccountWithFunds", Args: []any{networkname.BorDevnetChainName, "child-funder", 200.0}},
				{Text: "DeployRootChainReceiver", Args: []any{"root-funder"}},
				{Text: "DeployChildChainSender", Args: []any{"child-funder"}},
				{Text: "ProcessChildTransfers", Args: []any{"child-funder", 1, 2, 2}},
				//{Text: "BatchProcessTransfers", Args: []any{"child-funder", 1, 10, 2, 2}},
			},
		},
	}.Run(runCtx, strings.Split(ctx.String("scenarios"), ",")...)

	if ctx.Bool("wait") || (metrics && len(diagnosticsUrl) > 0) {
		logger.Info("Waiting")
		network.Wait()
	} else {
		logger.Info("Stopping Networks")
		network.Stop()
	}

	return nil
}

func initDevnet(ctx *cli.Context, logger log.Logger) (devnet.Devnet, error) {
	dataDir := ctx.String(DataDirFlag.Name)
	chain := ctx.String(ChainFlag.Name)
	baseRpcHost := ctx.String(BaseRpcHostFlag.Name)
	baseRpcPort := ctx.Int(BaseRpcPortFlag.Name)

	faucetSource := accounts.NewAccount("faucet-source")

	switch chain {
	case networkname.BorDevnetChainName:
		if ctx.Bool(WithoutHeimdallFlag.Name) {
			return []*devnet.Network{
				{
					DataDir:            dataDir,
					Chain:              networkname.BorDevnetChainName,
					Logger:             logger,
					BasePort:           40303,
					BasePrivateApiAddr: "localhost:10090",
					BaseRPCHost:        baseRpcHost,
					BaseRPCPort:        baseRpcPort,
					//Snapshots:          true,
					Alloc: types.GenesisAlloc{
						faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
					},
					Services: []devnet.Service{
						account_services.NewFaucet(networkname.BorDevnetChainName, faucetSource),
					},
					Nodes: []devnet.Node{
						&args.BlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "5",
								WithoutHeimdall:  true,
							},
							AccountSlots: 200,
						},
						&args.NonBlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "5",
								WithoutHeimdall:  true,
							},
						},
					},
				}}, nil
		} else {
			var heimdallGrpc string
			var services []devnet.Service
			var withMilestones = utils.WithHeimdallMilestones.Value

			checkpointOwner := accounts.NewAccount("checkpoint-owner")

			if ctx.Bool(LocalHeimdallFlag.Name) {
				config := *params.BorDevnetChainConfig
				// milestones are not supported yet on the local heimdall
				withMilestones = false

				if sprintSize := uint64(ctx.Int(BorSprintSizeFlag.Name)); sprintSize > 0 {
					config.Bor.Sprint = map[string]uint64{"0": sprintSize}
				}

				services = append(services, polygon.NewHeimdall(&config,
					&polygon.CheckpointConfig{
						CheckpointBufferTime: 60 * time.Second,
						CheckpointAccount:    checkpointOwner,
					},
					logger))

				heimdallGrpc = polygon.HeimdallGRpc(devnet.WithCliContext(context.Background(), ctx))
			}

			return []*devnet.Network{
				{
					DataDir:            dataDir,
					Chain:              networkname.BorDevnetChainName,
					Logger:             logger,
					BasePort:           40303,
					BasePrivateApiAddr: "localhost:10090",
					BaseRPCHost:        baseRpcHost,
					BaseRPCPort:        baseRpcPort,
					BorStateSyncDelay:  5 * time.Second,
					BorWithMilestones:  &withMilestones,
					Services:           append(services, account_services.NewFaucet(networkname.BorDevnetChainName, faucetSource)),
					Alloc: types.GenesisAlloc{
						faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
					},
					Nodes: []devnet.Node{
						&args.BlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "5",
								HeimdallGRpc:     heimdallGrpc,
							},
							AccountSlots: 200,
						},
						&args.BlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "5",
								HeimdallGRpc:     heimdallGrpc,
							},
							AccountSlots: 200,
						},
						/*&args.BlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "5",
								HeimdallGRpc:     heimdallGrpc,
							},
							AccountSlots: 200,
						},*/
						&args.NonBlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "5",
								HeimdallGRpc:     heimdallGrpc,
							},
						},
					},
				},
				{
					DataDir:            dataDir,
					Chain:              networkname.DevChainName,
					Logger:             logger,
					BasePort:           30403,
					BasePrivateApiAddr: "localhost:10190",
					BaseRPCHost:        baseRpcHost,
					BaseRPCPort:        baseRpcPort + 1000,
					Services:           append(services, account_services.NewFaucet(networkname.DevChainName, faucetSource)),
					Alloc: types.GenesisAlloc{
						faucetSource.Address:    {Balance: accounts.EtherAmount(200_000)},
						checkpointOwner.Address: {Balance: accounts.EtherAmount(10_000)},
					},
					Nodes: []devnet.Node{
						&args.BlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "5",
								VMDebug:          true,
								HttpCorsDomain:   "*",
							},
							DevPeriod:    5,
							AccountSlots: 200,
						},
						&args.NonBlockProducer{
							Node: args.Node{
								ConsoleVerbosity: "0",
								DirVerbosity:     "3",
							},
						},
					},
				}}, nil
		}

	case networkname.DevChainName:
		return []*devnet.Network{
			{
				DataDir:            dataDir,
				Chain:              networkname.DevChainName,
				Logger:             logger,
				BasePrivateApiAddr: "localhost:10090",
				BaseRPCHost:        baseRpcHost,
				BaseRPCPort:        baseRpcPort,
				Alloc: types.GenesisAlloc{
					faucetSource.Address: {Balance: accounts.EtherAmount(200_000)},
				},
				Services: []devnet.Service{
					account_services.NewFaucet(networkname.DevChainName, faucetSource),
				},
				Nodes: []devnet.Node{
					&args.BlockProducer{
						Node: args.Node{
							ConsoleVerbosity: "0",
							DirVerbosity:     "5",
						},
						AccountSlots: 200,
					},
					&args.NonBlockProducer{
						Node: args.Node{
							ConsoleVerbosity: "0",
							DirVerbosity:     "5",
						},
					},
				},
			}}, nil
	}

	return nil, fmt.Errorf(`Unknown network: "%s"`, chain)
}
