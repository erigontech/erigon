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
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/v3/cmd/observer/database"
	"github.com/erigontech/erigon/v3/cmd/observer/observer"
	"github.com/erigontech/erigon/v3/cmd/observer/reports"
	"github.com/erigontech/erigon/v3/cmd/utils"
	"github.com/erigontech/erigon/v3/params"
)

func mainWithFlags(ctx context.Context, flags observer.CommandFlags, logger log.Logger) error {
	server, err := observer.NewServer(ctx, flags, logger)
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
