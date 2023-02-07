package app

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/urfave/cli/v2"
)

var supportCommand = cli.Command{
	Action:    MigrateFlags(connectDiagnostics),
	Name:      "support",
	Usage:     "Connect Erigon instance to a diagnostics system for support",
	ArgsUsage: "--diagnostics.url <URL for the diagnostics system> --private.api.addr <Erigon host: Erigon port>",
	Flags: []cli.Flag{
		&utils.PriaveApiAddrFlag,
		&utils.DiagnosticsURLFlag,
	},
	Category: "SUPPORT COMMANDS",
	Description: `
The support command connects a running Erigon instance to a diagnostics system specified
by the URL.`,
}

func connectDiagnostics(ctx *cli.Context) error {
	fmt.Printf("Hello, world!\n")
	return nil
}
