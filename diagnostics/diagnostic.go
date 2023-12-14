package diagnostics

import (
	"context"
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

	snapshotDownload diaglib.SnapshotDownloadStatistics
}

func NewDiagnosticClient(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) *DiagnosticClient {
	return &DiagnosticClient{ctx: ctx, metricsMux: metricsMux, node: node, snapshotDownload: diaglib.SnapshotDownloadStatistics{}}
}

func (d *DiagnosticClient) Setup() {
	d.runSnapshotListener()
	d.runTorrentListener()
}

func (d *DiagnosticClient) runSnapshotListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.SnapshotDownloadStatistics](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.SnapshotDownloadStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.snapshotDownload.Downloaded = info.Downloaded
				d.snapshotDownload.Total = info.Total
				d.snapshotDownload.TotalTime = info.TotalTime
				d.snapshotDownload.DownloadRate = info.DownloadRate
				d.snapshotDownload.UploadRate = info.UploadRate
				d.snapshotDownload.Peers = info.Peers
				d.snapshotDownload.Files = info.Files
				d.snapshotDownload.Connections = info.Connections
				d.snapshotDownload.Alloc = info.Alloc
				d.snapshotDownload.Sys = info.Sys
				d.snapshotDownload.DownloadFinished = info.DownloadFinished
				d.snapshotDownload.TorrentMetadataReady = info.TorrentMetadataReady

				if info.DownloadFinished {
					return
				}
			}
		}

	}()
}

func (d *DiagnosticClient) SnapshotDownload() diaglib.SnapshotDownloadStatistics {
	return d.snapshotDownload
}

func (d *DiagnosticClient) runTorrentListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.SegmentDownloadStatistics](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.SegmentDownloadStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				if d.snapshotDownload.Segments == nil {
					d.snapshotDownload.Segments = map[string]diaglib.SegmentDownloadStatistics{}
				}

				d.snapshotDownload.Segments[info.Name] = info
			}
		}
	}()
}
