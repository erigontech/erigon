// Package cli contains framework for building a command-line based turbo-geth node.
package cli

import (
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/internal/flags"

	"github.com/urfave/cli"
)

// MakeApp creates a cli application (based on `github.com/urlfave/cli` package).
// The application exits when `action` returns.
// Parameters:
// * action: the main function for the application. receives `*cli.Context` with parsed command-line flags. Returns no error, if some error could not be recovered from write to the log or panic.
// * cliFlags: the list of flags `cli.Flag` that the app should set and parse. By default, use `DefaultFlags()`. If you want to specify your own flag, use `append(DefaultFlags(), myFlag)` for this parameter.
func MakeApp(action func(*cli.Context), cliFlags []cli.Flag) *cli.App {
	app := flags.NewApp("", "", "turbo-geth experimental cli")
	app.Action = action
	app.Flags = append(cliFlags, debug.Flags...) // debug flags are required
	app.Before = func(ctx *cli.Context) error {
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		return nil
	}
	return app
}
