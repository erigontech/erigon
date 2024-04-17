package app

import (
	"fmt"
	"io"
	"net/http"

	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

var (
	diagnosticsPrintDownloader = cli.StringFlag{
		Name:  "diagnostics.print.downloader",
		Usage: "Prints details about downloader status to console",
	}
)

var diagnosticsCommand = cli.Command{
	Action:    MigrateFlags(printDiagnostics),
	Name:      "diagnostics",
	Usage:     "Print diagnostics data to console",
	ArgsUsage: "--diagnostics.print.downloader",
	Before: func(cliCtx *cli.Context) error {
		_, _, err := debug.Setup(cliCtx, true /* rootLogger */)
		if err != nil {
			return err
		}
		return nil
	},
	Flags: append([]cli.Flag{
		&diagnosticsPrintDownloader,
	}, debug.Flags...),

	Description: `The diagnostics command prints diagnostics data to console.`,
}

func printDiagnostics(cliCtx *cli.Context) error {
	return PrintDiagnostics(cliCtx, log.Root())
}

func PrintDiagnostics(cliCtx *cli.Context, logger log.Logger) error {
	logger.Info("[!!!!Starting diagnostics]")
	MakeHttpGetCall()
	return nil
}

func MakeHttpGetCall() {
	// Make a GET request

	resp, err := http.Get("http://localhost:6060/debug/snapshot-sync")
	if err != nil {
		fmt.Println("Error: ", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error: ", err)
	}

	var data diagnostics.SyncStatistics
	marshalErr := json.Unmarshal(body, &data)
	if marshalErr == nil {
		fmt.Println("current stage:", data.SyncStages.CurrentStage)
	}

}
