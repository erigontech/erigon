package cli

import (
	"github.com/ledgerwatch/turbo-geth/console/prompt"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/internal/flags"

	"github.com/urfave/cli"
)

func MakeApp(action func(*cli.Context), cliFlags []cli.Flag) *cli.App {
	app := flags.NewApp("", "", "turbo-geth experimental cli")
	app.Action = action
	app.Flags = append(cliFlags, debug.Flags...) // debug flags are required
	app.Before = func(ctx *cli.Context) error {
		return debug.Setup(ctx)
	}
	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		prompt.Stdin.Close() // Resets terminal mode.
		return nil
	}
	return app
}
