package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/observer/database"
	"github.com/ledgerwatch/erigon/cmd/observer/observer"
	"github.com/ledgerwatch/erigon/cmd/observer/reports"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

func mainWithFlags(ctx context.Context, flags observer.CommandFlags, logger log.Logger) error {
	server, err := observer.NewServer(flags, logger)
	if err != nil {
		return err
	}

	db, err := database.NewDBSQLite(filepath.Join(flags.DataDir, "observer.sqlite"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	discV4, err := server.Listen(ctx)
	if err != nil {
		return err
	}

	networkID := uint(params.NetworkIDByChainName(flags.Chain))
	go observer.StatusLoggerLoop(ctx, db, networkID, flags.StatusLogPeriod, log.Root())

	crawlerConfig := observer.CrawlerConfig{
		Chain:            flags.Chain,
		Bootnodes:        server.Bootnodes(),
		PrivateKey:       server.PrivateKey(),
		ConcurrencyLimit: flags.CrawlerConcurrency,
		RefreshTimeout:   flags.RefreshTimeout,
		MaxPingTries:     flags.MaxPingTries,
		StatusLogPeriod:  flags.StatusLogPeriod,

		HandshakeRefreshTimeout: flags.HandshakeRefreshTimeout,
		HandshakeRetryDelay:     flags.HandshakeRetryDelay,
		HandshakeMaxTries:       flags.HandshakeMaxTries,

		KeygenTimeout:     flags.KeygenTimeout,
		KeygenConcurrency: flags.KeygenConcurrency,

		ErigonLogPath: flags.ErigonLogPath,
	}

	crawler, err := observer.NewCrawler(discV4, db, crawlerConfig, log.Root())
	if err != nil {
		return err
	}

	return crawler.Run(ctx)
}

func reportWithFlags(ctx context.Context, flags reports.CommandFlags) error {
	db, err := database.NewDBSQLite(filepath.Join(flags.DataDir, "observer.sqlite"))
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	networkID := uint(params.NetworkIDByChainName(flags.Chain))

	if flags.Estimate {
		report, err := reports.CreateClientsEstimateReport(ctx, db, flags.ClientsLimit, flags.MaxPingTries, networkID)
		if err != nil {
			return err
		}
		fmt.Println(report)
		return nil
	}

	if flags.SentryCandidates {
		report, err := reports.CreateSentryCandidatesReport(ctx, db, flags.ErigonLogPath)
		if err != nil {
			return err
		}
		fmt.Println(report)
		return nil
	}

	statusReport, err := reports.CreateStatusReport(ctx, db, flags.MaxPingTries, networkID)
	if err != nil {
		return err
	}
	clientsReport, err := reports.CreateClientsReport(ctx, db, flags.ClientsLimit, flags.MaxPingTries, networkID)
	if err != nil {
		return err
	}

	fmt.Println(statusReport)
	fmt.Println(clientsReport)
	return nil
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()

	command := observer.NewCommand()

	reportCommand := reports.NewCommand()
	reportCommand.OnRun(reportWithFlags)
	command.AddSubCommand(reportCommand.RawCommand())

	err := command.ExecuteContext(ctx, mainWithFlags)
	if (err != nil) && !errors.Is(err, context.Canceled) {
		utils.Fatalf("%v", err)
	}
}
