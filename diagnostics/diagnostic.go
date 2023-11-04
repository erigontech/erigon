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
		retry(100, 1*time.Second, func() error {
			err := d.runSnapshotListener()
			if err != nil {
				fmt.Println("Error running snapshot listener:", err)
			}
			return err
		})
	}()
}

func (d *DiagnosticClient) runSnapshotListener() error {

	ctx, ch, _ /*cancel*/ := diaglib.Context[diaglib.DownloadStatistics](context.Background(), 1)

	err := diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.DownloadStatistics{}), log.Root())
	if err != nil {
		fmt.Println("Error starting providers:", err)
		return err
	} else {

		for info := range ch {
			d.snapshotDownload = info
		}

		return nil
	}
}

func retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; i < attempts; i++ {
		if i > 0 {
			fmt.Println("retrying after error:", err)
			time.Sleep(sleep)
		}
		err = f()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

func (d *DiagnosticClient) SnapshotDownload() diaglib.DownloadStatistics {
	return d.snapshotDownload
}
