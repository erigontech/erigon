package diagnostics

import (
	"context"
	"net/http"
	"sync"

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

	syncStats        diaglib.SyncStatistics
	snapshotFileList diaglib.SnapshoFilesList
	mu               sync.Mutex
}

func NewDiagnosticClient(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) *DiagnosticClient {
	return &DiagnosticClient{ctx: ctx, metricsMux: metricsMux, node: node, syncStats: diaglib.SyncStatistics{}}
}

func (d *DiagnosticClient) Setup() {
	d.runSnapshotListener()
	d.runSegmentDownloadingListener()
	d.runSegmentIndexingListener()
	d.runSegmentIndexingFinishedListener()
	d.runCurrentSyncStageListener()
	d.runSyncStagesListListener()
	d.runBlockExecutionListener()
	d.runSnapshotFilesListListener()

	//d.logDiagMsgs()
}

/*func (d *DiagnosticClient) logDiagMsgs() {
	ticker := time.NewTicker(20 * time.Second)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				d.logStr()
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}
func (d *DiagnosticClient) logStr() {
	d.mu.Lock()
	defer d.mu.Unlock()
	log.Info("SyncStatistics", "stats", interfaceToJSONString(d.syncStats))
}

func interfaceToJSONString(i interface{}) string {
	b, err := json.Marshal(i)
	if err != nil {
		return ""
	}
	return string(b)
}*/

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
				d.mu.Lock()
				d.syncStats.SnapshotDownload.Downloaded = info.Downloaded
				d.syncStats.SnapshotDownload.Total = info.Total
				d.syncStats.SnapshotDownload.TotalTime = info.TotalTime
				d.syncStats.SnapshotDownload.DownloadRate = info.DownloadRate
				d.syncStats.SnapshotDownload.UploadRate = info.UploadRate
				d.syncStats.SnapshotDownload.Peers = info.Peers
				d.syncStats.SnapshotDownload.Files = info.Files
				d.syncStats.SnapshotDownload.Connections = info.Connections
				d.syncStats.SnapshotDownload.Alloc = info.Alloc
				d.syncStats.SnapshotDownload.Sys = info.Sys
				d.syncStats.SnapshotDownload.DownloadFinished = info.DownloadFinished
				d.syncStats.SnapshotDownload.TorrentMetadataReady = info.TorrentMetadataReady
				d.mu.Unlock()

				if info.DownloadFinished {
					return
				}
			}
		}

	}()
}

func (d *DiagnosticClient) SyncStatistics() diaglib.SyncStatistics {
	return d.syncStats
}

func (d *DiagnosticClient) SnapshotFilesList() diaglib.SnapshoFilesList {
	return d.snapshotFileList
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
				d.mu.Lock()
				if d.syncStats.SnapshotDownload.SegmentsDownloading == nil {
					d.syncStats.SnapshotDownload.SegmentsDownloading = map[string]diaglib.SegmentDownloadStatistics{}
				}

				d.syncStats.SnapshotDownload.SegmentsDownloading[info.Name] = info
				d.mu.Unlock()
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
				d.mu.Lock()
				found := false
				for i := range d.syncStats.SnapshotIndexing.Segments {
					if d.syncStats.SnapshotIndexing.Segments[i].SegmentName == info.SegmentName {
						found = true
						d.syncStats.SnapshotIndexing.Segments[i].Percent = 100
					}
				}

				if !found {
					d.syncStats.SnapshotIndexing.Segments = append(d.syncStats.SnapshotIndexing.Segments, diaglib.SnapshotSegmentIndexingStatistics{
						SegmentName: info.SegmentName,
						Percent:     100,
						Alloc:       0,
						Sys:         0,
					})
				}
				d.mu.Unlock()
			}
		}
	}()
}

func (d *DiagnosticClient) addOrUpdateSegmentIndexingState(upd diaglib.SnapshotIndexingStatistics) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.syncStats.SnapshotIndexing.Segments == nil {
		d.syncStats.SnapshotIndexing.Segments = []diaglib.SnapshotSegmentIndexingStatistics{}
	}

	for i := range upd.Segments {
		found := false
		for j := range d.syncStats.SnapshotIndexing.Segments {
			if d.syncStats.SnapshotIndexing.Segments[j].SegmentName == upd.Segments[i].SegmentName {
				d.syncStats.SnapshotIndexing.Segments[j].Percent = upd.Segments[i].Percent
				d.syncStats.SnapshotIndexing.Segments[j].Alloc = upd.Segments[i].Alloc
				d.syncStats.SnapshotIndexing.Segments[j].Sys = upd.Segments[i].Sys
				found = true
				break
			}
		}

		if !found {
			d.syncStats.SnapshotIndexing.Segments = append(d.syncStats.SnapshotIndexing.Segments, upd.Segments[i])
		}
	}

	d.syncStats.SnapshotIndexing.TimeElapsed = upd.TimeElapsed
}

func (d *DiagnosticClient) runSyncStagesListListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.SyncStagesList](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.SyncStagesList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.syncStats.SyncStages.StagesList = info.Stages
				d.mu.Unlock()
				return
			}
		}
	}()
}

func (d *DiagnosticClient) runCurrentSyncStageListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.CurrentSyncStage](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.CurrentSyncStage{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.syncStats.SyncStages.CurrentStage = info.Stage
				if int(d.syncStats.SyncStages.CurrentStage) >= len(d.syncStats.SyncStages.StagesList) {
					return
				}
				d.mu.Unlock()
			}
		}
	}()
}

func (d *DiagnosticClient) runBlockExecutionListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.BlockExecutionStatistics](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.BlockExecutionStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.syncStats.BlockExecution = info
				d.mu.Unlock()

				if int(d.syncStats.SyncStages.CurrentStage) >= len(d.syncStats.SyncStages.StagesList) {
					return
				}
			}
		}
	}()
}

func (d *DiagnosticClient) runSnapshotFilesListListener() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.SnapshoFilesList](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.SnapshoFilesList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				d.mu.Lock()
				d.snapshotFileList = info
				d.mu.Unlock()

				if len(info.Files) > 0 {
					return
				}
			}
		}
	}()
}
