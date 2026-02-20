package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/arb/l1sync"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/nitro-erigon/execution/erigon/ethclient"
)

var (
	l1RpcFlag = &cli.StringFlag{
		Name:     "l1-rpc",
		Usage:    "L1 RPC URL",
		Required: true,
	}
	beaconUrlFlag = &cli.StringFlag{
		Name:     "beacon-url",
		Usage:    "Beacon chain RPC URL (for blob resolution)",
		Required: true,
	}
	dataDirFlag = &cli.StringFlag{
		Name:  "datadir",
		Usage: "Data directory for l1sync database",
		Value: "l1sync-data",
	}
	chainFlag = &cli.StringFlag{
		Name:  "chain",
		Usage: "Chain preset: arbitrum-one, arbitrum-sepolia",
		Value: "arbitrum-sepolia",
	}
)

func main() {
	app := &cli.App{
		Name:  "l1sync",
		Usage: "Fetch and store L1 sequencer batches for Arbitrum",
		Flags: []cli.Flag{
			l1RpcFlag,
			beaconUrlFlag,
			dataDirFlag,
			chainFlag,
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(cliCtx *cli.Context) error {
	logger := log.New()

	// Select chain preset
	var cfg l1sync.Config
	switch cliCtx.String(chainFlag.Name) {
	case "arbitrum-one":
		cfg = l1sync.ArbitrumOneConfig
	case "arbitrum-sepolia":
		cfg = l1sync.ArbitrumSepoliaConfig
	default:
		cfg = l1sync.DefaultConfig
	}
	cfg.Enable = true

	logger.Info("l1sync starting",
		"chain", cliCtx.String(chainFlag.Name),
		"sequencerInbox", cfg.SequencerInboxAddr,
		"startL1Block", cfg.StartL1Block,
		"datadir", cliCtx.String(dataDirFlag.Name),
	)

	// Connect to L1
	l1RpcUrl := cliCtx.String(l1RpcFlag.Name)
	l1Client, err := ethclient.Dial(l1RpcUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to L1 RPC: %w", err)
	}
	logger.Info("connected to L1", "url", l1RpcUrl)

	// Connect blob L1 client
	blobL1Client, err := ethclient.Dial(cliCtx.String(beaconUrlFlag.Name))
	if err != nil {
		return fmt.Errorf("failed to connect to blob L1 RPC: %w", err)
	}
	logger.Info("connected to blob L1", "url", cliCtx.String(beaconUrlFlag.Name))

	// Open database
	db, err := openDB(cliCtx.String(dataDirFlag.Name))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	// Create l1sync service (nil exec for now)
	svc, err := l1sync.New(
		&cfg,
		l1Client,
		cliCtx.String(beaconUrlFlag.Name),
		blobL1Client,
		nil, // no execution sequencer for now
		db,
	)
	if err != nil {
		return fmt.Errorf("failed to create l1sync service: %w", err)
	}

	ctx, cancel := context.WithCancel(cliCtx.Context)
	defer cancel()

	svc.Start(ctx)
	logger.Info("l1sync service started, waiting for interrupt...")

	// Wait for interrupt
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)
	<-sigint

	logger.Info("shutting down...")
	svc.StopAndWait()
	logger.Info("l1sync stopped")
	return nil
}

func openDB(path string) (kv.RwDB, error) {
	return mdbx.New(kv.Label(dbcfg.ArbitrumDB), log.New()).
		Path(path).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
			return kv.ChaindataTablesCfg
		}).
		Open(context.Background())
}
