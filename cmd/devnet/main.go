package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	dbg "runtime/debug"
	"time"

	_ "github.com/ledgerwatch/erigon/cmd/devnet/commands"

	"github.com/ledgerwatch/erigon/cmd/devnet/args"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/cmd/devnet/scenarios"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
	"github.com/ledgerwatch/erigon/params/networkname"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/params"
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

	WithoutHeimdallFlag = cli.BoolFlag{
		Name:  "bor.withoutheimdall",
		Usage: "Run without Heimdall service",
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
		&WithoutHeimdallFlag,
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

	network, err := selectNetwork(ctx, logger)

	if err != nil {
		return err
	}

	// start the network with each node in a go routine
	logger.Info("Starting Network")
	if err := network.Start(); err != nil {
		return fmt.Errorf("Network start failed: %w", err)
	}

	runCtx := devnet.WithCliContext(context.Background(), ctx)

	if ctx.String(ChainFlag.Name) == networkname.DevChainName {
		// the dev network currently inserts blocks very slowly when run in multi node mode - needs investigaton
		// this effectively makes it a ingle node network by routing all traffic to node 0
		// devnet.WithCurrentNode(devnet.WithCliContext(context.Background(), ctx), 0)
		services.MaxNumberOfEmptyBlockChecks = 30
	}

	network.Run(
		runCtx,
		scenarios.Scenario{
			Name: "all",
			Steps: []*scenarios.Step{
				{Text: "InitSubscriptions", Args: []any{[]requests.SubMethod{requests.Methods.ETHNewHeads}}},
				{Text: "PingErigonRpc"},
				{Text: "CheckTxPoolContent", Args: []any{0, 0, 0}},
				{Text: "SendTxWithDynamicFee", Args: []any{recipientAddress, services.DevAddress, sendValue}},
				{Text: "AwaitBlocks", Args: []any{2 * time.Second}},
			},
		})

	logger.Info("Stopping Network")
	network.Stop()

	return nil
}

func selectNetwork(ctx *cli.Context, logger log.Logger) (*devnet.Network, error) {
	dataDir := ctx.String(DataDirFlag.Name)
	chain := ctx.String(ChainFlag.Name)

	switch chain {
	case networkname.BorDevnetChainName:
		if ctx.Bool(WithoutHeimdallFlag.Name) {
			return &devnet.Network{
				DataDir:            dataDir,
				Chain:              networkname.BorDevnetChainName,
				Logger:             logger,
				BasePrivateApiAddr: "localhost:10090",
				BaseRPCAddr:        "localhost:8545",
				Nodes: []devnet.Node{
					args.Miner{
						Node: args.Node{
							ConsoleVerbosity: "0",
							DirVerbosity:     "5",
							WithoutHeimdall:  true,
						},
						AccountSlots: 200,
					},
					args.NonMiner{
						Node: args.Node{
							ConsoleVerbosity: "0",
							DirVerbosity:     "5",
							WithoutHeimdall:  true,
						},
					},
				},
			}, nil
		} else {
			return &devnet.Network{
				DataDir:            dataDir,
				Chain:              networkname.BorDevnetChainName,
				Logger:             logger,
				BasePrivateApiAddr: "localhost:10090",
				BaseRPCAddr:        "localhost:8545",
				Nodes: []devnet.Node{
					args.Miner{
						Node: args.Node{
							ConsoleVerbosity: "0",
							DirVerbosity:     "5",
						},
						AccountSlots: 200,
					},
					args.Miner{
						Node: args.Node{
							ConsoleVerbosity: "0",
							DirVerbosity:     "5",
						},
						AccountSlots: 200,
					},
					args.NonMiner{
						Node: args.Node{
							ConsoleVerbosity: "0",
							DirVerbosity:     "5",
						},
					},
				},
			}, nil
		}

	case networkname.DevChainName:
		return &devnet.Network{
			DataDir:            dataDir,
			Chain:              networkname.DevChainName,
			Logger:             logger,
			BasePrivateApiAddr: "localhost:10090",
			BaseRPCAddr:        "localhost:8545",
			Nodes: []devnet.Node{
				args.Miner{
					Node: args.Node{
						ConsoleVerbosity: "0",
						DirVerbosity:     "5",
					},
					AccountSlots: 200,
				},
				args.NonMiner{
					Node: args.Node{
						ConsoleVerbosity: "0",
						DirVerbosity:     "5",
					},
				},
			},
		}, nil
	}

	return nil, fmt.Errorf(`Unknown network: "%s"`, chain)
}
