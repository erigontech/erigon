package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/diag/db"
	"github.com/ledgerwatch/erigon/cmd/diag/downloader"
	"github.com/ledgerwatch/erigon/cmd/diag/stages"
	"github.com/ledgerwatch/erigon/cmd/diag/ui"
	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
)

func main() {
	logging.LogVerbosityFlag.Value = log.LvlError.String()
	logging.LogConsoleVerbosityFlag.Value = log.LvlError.String()

	app := cli.NewApp()
	app.Name = "diagnostics"
	app.Version = params.VersionWithCommit(params.GitCommit)
	app.EnableBashCompletion = true

	app.Commands = []*cli.Command{
		&downloader.Command,
		&stages.Command,
		&db.Command,
		&ui.Command,
	}

	app.Flags = []cli.Flag{}

	app.HelpName = `Erigon Diagnostics`
	app.Usage = "Display diagnostic output for a running erigon node"
	app.UsageText = `diag [command] [flags]`

	app.Action = func(context *cli.Context) error {
		var goodNames []string
		for _, c := range app.VisibleCommands() {
			goodNames = append(goodNames, c.Name)
		}
		_, _ = fmt.Fprintf(os.Stderr, "Command '%s' not found. Available commands: %s\n", context.Args().First(), goodNames)
		cli.ShowAppHelpAndExit(context, 1)

		return nil
	}

	for _, command := range app.Commands {
		command.Before = func(ctx *cli.Context) error {
			logger, err := setupLogger(ctx)

			if err != nil {
				return err
			}

			var cancel context.CancelFunc

			ctx.Context, cancel = context.WithCancel(sync.WithLogger(ctx.Context, logger))

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

	logger := logging.SetupLoggerCtx("diagnostics-"+ctx.Command.Name, ctx, log.LvlError, log.LvlInfo, false)

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
