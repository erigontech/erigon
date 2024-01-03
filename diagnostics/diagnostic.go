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
	d.runSegmentDownloadingListener()
	d.runSegmentIndexingListener()
	d.runSegmentIndexingFinishedListener()
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
				d.snapshotDownload.LogPrefix = info.LogPrefix

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

func (d *DiagnosticClient) runSegmentDownloadingListener() {
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
				if d.snapshotDownload.SegmentsDownloading == nil {
					d.snapshotDownload.SegmentsDownloading = map[string]diaglib.SegmentDownloadStatistics{}
				}

				d.snapshotDownload.SegmentsDownloading[info.Name] = info
			}
		}
	}()
}

func (d *DiagnosticClient) runSegmentIndexingListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.SnapshotIndexingStatistics](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.SnapshotIndexingStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.addOrUpdateSegmentIndexingState(info)
			}
		}
	}()
}

func (d *DiagnosticClient) runSegmentIndexingFinishedListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.SnapshotSegmentIndexingFinishedUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.SnapshotSegmentIndexingFinishedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				found := false
				for i := range d.snapshotDownload.SegmentIndexing.Segments {
					if d.snapshotDownload.SegmentIndexing.Segments[i].SegmentName == info.SegmentName {
						found = true
						d.snapshotDownload.SegmentIndexing.Segments[i].Percent = 100
					}
				}

				if !found {
					d.snapshotDownload.SegmentIndexing.Segments = append(d.snapshotDownload.SegmentIndexing.Segments, diaglib.SnapshotSegmentIndexingStatistics{
						SegmentName: info.SegmentName,
						Percent:     100,
						Alloc:       0,
						Sys:         0,
					})
				}
			}
		}
	}()
}

func (d *DiagnosticClient) addOrUpdateSegmentIndexingState(upd diaglib.SnapshotIndexingStatistics) {
	if d.snapshotDownload.SegmentIndexing.Segments == nil {
		d.snapshotDownload.SegmentIndexing.Segments = []diaglib.SnapshotSegmentIndexingStatistics{}
	}

	for i := range upd.Segments {
		found := false
		for j := range d.snapshotDownload.SegmentIndexing.Segments {
			if d.snapshotDownload.SegmentIndexing.Segments[j].SegmentName == upd.Segments[i].SegmentName {
				d.snapshotDownload.SegmentIndexing.Segments[j].Percent = upd.Segments[i].Percent
				d.snapshotDownload.SegmentIndexing.Segments[j].Alloc = upd.Segments[i].Alloc
				d.snapshotDownload.SegmentIndexing.Segments[j].Sys = upd.Segments[i].Sys
				found = true
				break
			}
		}

		if !found {
			d.snapshotDownload.SegmentIndexing.Segments = append(d.snapshotDownload.SegmentIndexing.Segments, upd.Segments[i])
		}
	}

	d.snapshotDownload.SegmentIndexing.TimeElapsed = upd.TimeElapsed
}
