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
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common/disk"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/snapshots/genfromrpc"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/diagnostics/mem"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"
)

func main() {
	logging.LogVerbosityFlag.Value = log.LvlError.String()
	logging.LogConsoleVerbosityFlag.Value = log.LvlError.String()

	app := cli.NewApp()
	app.Name = "snapshots"
	app.Version = version.VersionWithCommit(version.GitCommit)

	app.Commands = []*cli.Command{
		&genfromrpc.Command,
	}

	app.Flags = []cli.Flag{}

	app.UsageText = app.Name + ` [command] [flags]`

	app.Action = func(context *cli.Context) error {
		if context.Args().Present() {
			var goodNames []string
			for _, c := range app.VisibleCommands() {
				goodNames = append(goodNames, c.Name)
			}
			_, _ = fmt.Fprintf(os.Stderr, "Command '%s' not found. Available commands: %s\n", context.Args().First(), goodNames)
			cli.ShowAppHelpAndExit(context, 1)
		}

		return nil
	}

	for _, command := range app.Commands {
		command.Before = func(ctx *cli.Context) error {
			debug.RaiseFdLimit()

			logger, err := setupLogger(ctx)

			if err != nil {
				return err
			}

			var cancel context.CancelFunc

			ctx.Context, cancel = context.WithCancel(ctx.Context) //nolint

			// setup periodic logging and prometheus updates
			go mem.LogMemStats(ctx.Context, logger)
			go disk.UpdateDiskStats(ctx.Context, logger)

			go handleTerminationSignals(cancel, logger)

			return nil
		}
	}

	if err := app.Run(os.Args); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setupLogger(ctx *cli.Context) (log.Logger, error) {
	dataDir := ctx.String(utils.DataDirFlag.Name)

	if len(dataDir) > 0 {
		logsDir := filepath.Join(dataDir, "logs")

		if err := os.MkdirAll(logsDir, 0755); err != nil {
			return nil, err
		}
	}

	logger := logging.SetupLoggerCtx("snapshots-"+ctx.Command.Name, ctx, log.LvlError, log.LvlInfo, false)

	return logger, nil
}

func handleTerminationSignals(stopFunc func(), logger log.Logger) {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGTERM, syscall.SIGINT)

	switch s := <-signalCh; s {
	case syscall.SIGTERM:
		logger.Info("Stopping")
		stopFunc()
	case syscall.SIGINT:
		logger.Info("Terminating")
		os.Exit(-int(syscall.SIGINT))
	}
}
