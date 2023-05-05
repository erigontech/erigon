// Package app contains framework for building a command-line based Erigon node.
package app

import (
	"fmt"
	"strings"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	cli2 "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/debug"
)

// MakeApp creates a cli application (based on `github.com/urlfave/cli` package).
// The application exits when `action` returns.
// Parameters:
// * action: the main function for the application. receives `*cli.Context` with parsed command-line flags.
// * cliFlags: the list of flags `cli.Flag` that the app should set and parse. By default, use `DefaultFlags()`. If you want to specify your own flag, use `append(DefaultFlags(), myFlag)` for this parameter.
func MakeApp(name string, action cli.ActionFunc, cliFlags []cli.Flag) *cli.App {
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

		// run default action
		return action(context)
	}
	app.Flags = append(cliFlags, debug.Flags...) // debug flags are required
	app.Flags = append(app.Flags, utils.MetricFlags...)
	app.Flags = append(app.Flags, logging.Flags...)
	app.Before = func(ctx *cli.Context) error {
		return debug.Setup(ctx)
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

// MigrateFlags makes all global flag values available in the
// context. This should be called as early as possible in app.Before.
//
// Example:
//
//	geth account new --keystore /tmp/mykeystore --lightkdf
//
// is equivalent after calling this method with:
//
//	geth --keystore /tmp/mykeystore --lightkdf account new
//
// i.e. in the subcommand Action function of 'account new', ctx.Bool("lightkdf)
// will return true even if --lightkdf is set as a global option.
//
// This function may become unnecessary when https://github.com/urfave/cli/pull/1245 is merged.
func MigrateFlags(action cli.ActionFunc) cli.ActionFunc {
	return func(ctx *cli.Context) error {
		doMigrateFlags(ctx)
		return action(ctx)
	}
}

func doMigrateFlags(ctx *cli.Context) {
	// Figure out if there are any aliases of commands. If there are, we want
	// to ignore them when iterating over the flags.
	var aliases = make(map[string]bool)
	for _, fl := range ctx.Command.Flags {
		for _, alias := range fl.Names()[1:] {
			aliases[alias] = true
		}
	}
	for _, name := range ctx.FlagNames() {
		for _, parent := range ctx.Lineage()[1:] {
			if parent.IsSet(name) {
				// When iterating across the lineage, we will be served both
				// the 'canon' and alias formats of all commands. In most cases,
				// it's fine to set it in the ctx multiple times (one for each
				// name), however, the Slice-flags are not fine.
				// The slice-flags accumulate, so if we set it once as
				// "foo" and once as alias "F", then both will be present in the slice.
				if _, isAlias := aliases[name]; isAlias {
					continue
				}
				// If it is a string-slice, we need to set it as
				// "alfa, beta, gamma" instead of "[alfa beta gamma]", in order
				// for the backing StringSlice to parse it properly.
				if result := parent.StringSlice(name); len(result) > 0 {
					ctx.Set(name, strings.Join(result, ","))
				} else {
					ctx.Set(name, parent.String(name))
				}
				break
			}
		}
	}
}

func NewNodeConfig(ctx *cli.Context) *nodecfg.Config {
	nodeConfig := nodecfg.DefaultConfig
	// see simiar changes in `cmd/geth/config.go#defaultNodeConfig`
	if commit := params.GitCommit; commit != "" {
		nodeConfig.Version = params.VersionWithCommit(commit)
	} else {
		nodeConfig.Version = params.Version
	}
	nodeConfig.IPCPath = "" // force-disable IPC endpoint
	nodeConfig.Name = "erigon"
	if ctx.IsSet(utils.DataDirFlag.Name) {
		nodeConfig.Dirs = datadir.New(ctx.String(utils.DataDirFlag.Name))
	}
	return &nodeConfig
}

func MakeConfigNodeDefault(ctx *cli.Context) *node.Node {
	return makeConfigNode(NewNodeConfig(ctx))
}

func makeConfigNode(config *nodecfg.Config) *node.Node {
	stack, err := node.New(config)
	if err != nil {
		utils.Fatalf("Failed to create Erigon node: %v", err)
	}

	return stack
}
