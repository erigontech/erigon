package diagnostics

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	diaglib "github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon/turbo/node"
	"github.com/ledgerwatch/log/v3"
	"github.com/urfave/cli/v2"
)

type DiagnosticClient struct {
	ctx        *cli.Context
	metricsMux *http.ServeMux
	node       *node.ErigonNode

	snapshotDownload map[string]diaglib.DownloadStatistics
	fileDownload     map[string]diaglib.TorrentFile
}

func NewDiagnosticClient(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) *DiagnosticClient {
	return &DiagnosticClient{ctx: ctx, metricsMux: metricsMux, node: node, snapshotDownload: map[string]diaglib.DownloadStatistics{}, fileDownload: map[string]diaglib.TorrentFile{}}
}

func (d *DiagnosticClient) Setup() {
	d.runSnapshotListener()
	d.runTorrentListener()
}

func (d *DiagnosticClient) runSnapshotListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.DownloadStatistics](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.DownloadStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.snapshotDownload[info.StagePrefix] = info
				if info.DownloadFinished {
					return
				}
			}
		}

	}()
}

func (d *DiagnosticClient) SnapshotDownload() map[string]diaglib.DownloadStatistics {
	return d.snapshotDownload
}

func (d *DiagnosticClient) runTorrentListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.TorrentFile](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.TorrentFile{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				fmt.Println("info.Name", info.Name)
				fmt.Println("info.peers", info.PeersCount, "speed", info.PeersRate)
				fmt.Println("info.seeds", info.SeedsCount, "speed", info.SeedsRate)
				d.fileDownload[info.Name] = info
			}
		}
	}()
}

func (d *DiagnosticClient) TorrentFileDownload() map[string]diaglib.TorrentFile {
	return d.fileDownload
}
