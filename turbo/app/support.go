package app

import (
	"io"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

var supportCommand = cli.Command{
	Action:    MigrateFlags(connectDiagnostics),
	Name:      "support",
	Usage:     "Connect Erigon instance to a diagnostics system for support",
	ArgsUsage: "--diagnostics.url <URL for the diagnostics system> --metrics.url <http://erigon_host:metrics_port>",
	Flags: []cli.Flag{
		&utils.MetricsURLsFlag,
		&utils.DiagnosticsURLFlag,
	},
	Category: "SUPPORT COMMANDS",
	Description: `
The support command connects a running Erigon instances to a diagnostics system specified
by the URL.`,
}

func connectDiagnostics(ctx *cli.Context) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	interrupt := false
	pollInterval := 500 * time.Millisecond
	pollEvery := time.NewTicker(pollInterval)
	defer pollEvery.Stop()
	client := &http.Client{}
	defer client.CloseIdleConnections()
	diagnosticsUrl := ctx.String(utils.DiagnosticsURLFlag.Name)
	giveRequestsUrl := path.Join(diagnosticsUrl, "giveRequests")
	for !interrupt {
		select {
		case interrupt = <-interruptCh:
			log.Info("interrupted, please wait for cleanup")
		case <-pollEvery.C:
			pollResponse, err := client.Get(giveRequestsUrl)
			if err != nil {
				log.Error("Polling problem", "err", err)
			} else if pollResponse.StatusCode != http.StatusOK {
				log.Error("Polling problem", "status", pollResponse.Status)
			} else {
				buf := make([]byte, pollResponse.ContentLength)
				if _, err = io.ReadFull(pollResponse.Body, buf); err != nil {
					log.Error("Reading polling response", "err", err)
				}
				log.Info("Polling response", "resp", string(buf))
			}
			pollResponse.Body.Close()
		}
	}
	return nil
}
