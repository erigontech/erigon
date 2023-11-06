package diagnostics

import (
	"context"
	"net/http"

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
	d.runSnapshotListener()
}

func (d *DiagnosticClient) runSnapshotListener() {
	go func() {
		ctx, ch, _ /*cancel*/ := diaglib.Context[diaglib.DownloadStatistics](context.Background(), 1)

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.DownloadStatistics{}), log.Root())
		for info := range ch {
			d.snapshotDownload = info
			if info.DownloadFinished {
				return
			}
		}
	}()
}

func (d *DiagnosticClient) SnapshotDownload() diaglib.DownloadStatistics {
	return d.snapshotDownload
}
