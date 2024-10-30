// Package app contains framework for building a command-line based Erigon node.
package app

import (
	"fmt"

	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params"
	cli2 "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/debug"
)

// MakeApp creates a cli application (based on `github.com/urlfave/cli` package).
// The application exits when `action` returns.
// Parameters:
// * action: the main function for the application. receives `*cli.Context` with parsed command-line flags.
// * cliFlags: the list of flags `cli.Flag` that the app should set and parse. By default, use `DefaultFlags()`. If you want to specify your own flag, use `append(DefaultFlags(), myFlag)` for this parameter.
func MakeApp_zkEvm(name string, action cli.ActionFunc, cliFlags []cli.Flag) *cli.App {
	app := cli2.NewApp(params.GitCommit, "erigon")
	app.Name = name
	app.UsageText = app.Name + ` [command] [flags]`
	app.Action = func(context *cli.Context) error {
		// handle case: unknown sub-command
		if context.Args().Present() {
			var goodNames []string
			for _, c := range app.VisibleCommands() {
				goodNames = append(goodNames, c.Name)
			}
			log.Error(fmt.Sprintf("Command '%s' not found. Available commands: %s", context.Args().First(), goodNames))
			cli.ShowAppHelpAndExit(context, 1)
		}

		// handle case: config flag
		configFilePath := context.String(utils.ConfigFlag.Name)
		if configFilePath != "" {
			if err := cli2.SetFlagsFromConfigFile(context, configFilePath); err != nil {
				log.Error("failed setting config flags from yaml/toml file", "err", err)
				return err
			}
		}

		// run default action
		return action(context)
	}

	app.Flags = appFlags(cliFlags)
	app.Before = func(ctx *cli.Context) error {
		_, _, _, err := debug.Setup(ctx, true)
		return err
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		return nil
	}
	app.Commands = []*cli.Command{
		&initCommand,
		&importCommand,
		&snapshotCommand,
		&supportCommand,
		//&backupCommand,
	}
	return app
}
