package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	dbg "runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/cmd/devnet/services/polygon"

	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	_ "github.com/ledgerwatch/erigon/cmd/devnet/accounts/steps"
	_ "github.com/ledgerwatch/erigon/cmd/devnet/admin"
	_ "github.com/ledgerwatch/erigon/cmd/devnet/contracts/steps"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/tests"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/params"
	erigon_app "github.com/ledgerwatch/erigon/turbo/app"
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

	HeimdallGrpcAddressFlag = cli.StringFlag{
		Name:  "bor.heimdallgRPC",
		Usage: "Address of Heimdall gRPC service",
		Value: polygon.HeimdallGrpcAddressDefault,
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
		Name:  "diagnostics.addr",
		Usage: "Address of the diagnostics system provided by the support team, include unique session PIN",
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
	os.Exit(2)
	return nil
}

func main() {
	app := cli.NewApp()
	app.Version = params.VersionWithCommit(params.GitCommit)
	app.Action = mainContext

	app.Flags = []cli.Flag{
		&DataDirFlag,
		&ChainFlag,
		&ScenariosFlag,
		&BaseRpcHostFlag,
		&BaseRpcPortFlag,
		&WithoutHeimdallFlag,
		&LocalHeimdallFlag,
		&HeimdallGrpcAddressFlag,
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

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setupLogger(ctx *cli.Context) (log.Logger, error) {
	dataDir := ctx.String(DataDirFlag.Name)
	logsDir := filepath.Join(dataDir, "logs")

	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, err
	}

	logger := logging.SetupLoggerCtx("devnet", ctx, false /* rootLogger */)

	// Make root logger fail
	log.Root().SetHandler(PanicHandler{})

	return logger, nil
}

func handleTerminationSignals(stopFunc func(), logger log.Logger) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	switch s := <-signalCh; s {
	case syscall.SIGTERM:
		logger.Info("Stopping networks")
		stopFunc()
	case syscall.SIGINT:
		logger.Info("Terminating network")
		os.Exit(-int(syscall.SIGINT))
	}
}

func connectDiagnosticsIfEnabled(ctx *cli.Context, logger log.Logger) {
	metricsEnabled := ctx.Bool(MetricsEnabledFlag.Name)
	diagnosticsUrl := ctx.String(DiagnosticsURLFlag.Name)
	if metricsEnabled && len(diagnosticsUrl) > 0 {
		err := erigon_app.ConnectDiagnostics(ctx, logger)
		if err != nil {
			logger.Error("app.ConnectDiagnostics failed", "err", err)
		}
	}
}

func mainContext(ctx *cli.Context) error {
	debug.RaiseFdLimit()

	logger, err := setupLogger(ctx)
	if err != nil {
		return err
	}

	// clear all the dev files
	dataDir := ctx.String(DataDirFlag.Name)
	if err := devnetutils.ClearDevDB(dataDir, logger); err != nil {
		return err
	}

	network, err := initDevnet(ctx, logger)
	if err != nil {
		return err
	}

	if err = initDevnetMetrics(ctx, network); err != nil {
		return err
	}

	logger.Info("Starting Devnet")
	runCtx, err := network.Start(logger)
	if err != nil {
		return fmt.Errorf("devnet start failed: %w", err)
	}

	go handleTerminationSignals(network.Stop, logger)
	go connectDiagnosticsIfEnabled(ctx, logger)

	enabledScenarios := strings.Split(ctx.String(ScenariosFlag.Name), ",")
	if err = allScenarios(runCtx).Run(runCtx, enabledScenarios...); err != nil {
		return err
	}

	if ctx.Bool(WaitFlag.Name) {
		logger.Info("Waiting")
		network.Wait()
	} else {
		logger.Info("Stopping Networks")
		network.Stop()
	}

	return nil
}

func allScenarios(runCtx devnet.Context) scenarios.Scenarios {
	// unsubscribe from all the subscriptions made
	defer services.UnsubscribeAll()

	const recipientAddress = "0x71562b71999873DB5b286dF957af199Ec94617F7"
	const sendValue uint64 = 10000

	return scenarios.Scenarios{
		"dynamic-tx-node-0": {
			Context: runCtx.WithCurrentNetwork(0).WithCurrentNode(0),
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
	}
}

func initDevnet(ctx *cli.Context, logger log.Logger) (devnet.Devnet, error) {
	dataDir := ctx.String(DataDirFlag.Name)
	chainName := ctx.String(ChainFlag.Name)
	baseRpcHost := ctx.String(BaseRpcHostFlag.Name)
	baseRpcPort := ctx.Int(BaseRpcPortFlag.Name)

	switch chainName {
	case networkname.BorDevnetChainName:
		if ctx.Bool(WithoutHeimdallFlag.Name) {
			return tests.NewBorDevnetWithoutHeimdall(dataDir, baseRpcHost, baseRpcPort, logger), nil
		} else if ctx.Bool(LocalHeimdallFlag.Name) {
			heimdallGrpcAddr := ctx.String(HeimdallGrpcAddressFlag.Name)
			sprintSize := uint64(ctx.Int(BorSprintSizeFlag.Name))
			return tests.NewBorDevnetWithLocalHeimdall(dataDir, baseRpcHost, baseRpcPort, heimdallGrpcAddr, sprintSize, logger), nil
		} else {
			return tests.NewBorDevnetWithRemoteHeimdall(dataDir, baseRpcHost, baseRpcPort, logger), nil
		}

	case networkname.DevChainName:
		return tests.NewDevDevnet(dataDir, baseRpcHost, baseRpcPort, logger), nil

	default:
		return nil, fmt.Errorf("unknown network: '%s'", chainName)
	}
}

func initDevnetMetrics(ctx *cli.Context, network devnet.Devnet) error {
	metricsEnabled := ctx.Bool(MetricsEnabledFlag.Name)
	metricsNode := ctx.Int(MetricsNodeFlag.Name)
	metricsPort := ctx.Int(MetricsPortFlag.Name)

	if !metricsEnabled {
		return nil
	}

	for _, nw := range network {
		for i, nodeArgs := range nw.Nodes {
			if metricsEnabled && (metricsNode == i) {
				nodeArgs.EnableMetrics(metricsPort)
				return nil
			}
		}
	}

	return fmt.Errorf("initDevnetMetrics: not found %s=%d", MetricsNodeFlag.Name, metricsNode)
}
