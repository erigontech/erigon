package diagnostics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	diaglib "github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type DiagnosticClient struct {
	ctx        *cli.Context
	metricsMux *http.ServeMux
	node       *node.ErigonNode

	snapshotDownload diaglib.DownloadStatistics
}

func NewDiagnosticClient(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) *DiagnosticClient {
	return &DiagnosticClient{ctx: ctx, metricsMux: metricsMux, node: node}
}

func (d *DiagnosticClient) Setup() {
	d.snapshotDownloader()
}

func (d *DiagnosticClient) snapshotDownloader() {
	go func() {
		//TODO: remove delay and implement some retry logic to wait for register provider on snashotsync.go
		time.Sleep(100 * time.Second)
		ctx, ch, _ /*cancel*/ := diaglib.Context[diaglib.DownloadStatistics](context.Background(), 1)

		err := diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.DownloadStatistics{}), log.Root())
		if err != nil {
			fmt.Println("Error starting providers:", err)
		} else {

			for info := range ch {
				d.snapshotDownload = info
			}
		}
	}()
}

func (d *DiagnosticClient) SnapshotDownload() diaglib.DownloadStatistics {
	return d.snapshotDownload
}
