package diagnostics

import (
	"context"
	"net/http"
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
	diaglib "github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/diskutils"
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
	hardwareInfo     diaglib.HardwareInfo
	peersSyncMap     sync.Map
}

func NewDiagnosticClient(ctx *cli.Context, metricsMux *http.ServeMux, node *node.ErigonNode) *DiagnosticClient {
	return &DiagnosticClient{ctx: ctx, metricsMux: metricsMux, node: node, syncStats: diaglib.SyncStatistics{}, hardwareInfo: diaglib.HardwareInfo{}, snapshotFileList: diaglib.SnapshoFilesList{}}
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
	d.getSysInfo()
	d.runCollectPeersStatistics()

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

func (d *DiagnosticClient) findNodeDisk() string {
	dirPath := d.node.Backend().DataDir()
	mountPoint := diskutils.MountPointForDirPath(dirPath)

	return mountPoint
}

func (d *DiagnosticClient) getSysInfo() {
	nodeDisk := d.findNodeDisk()

	ramInfo := diaglib.GetRAMInfo()
	diskInfo := diaglib.GetDiskInfo(nodeDisk)
	cpuInfo := diaglib.GetCPUInfo()

	d.mu.Lock()
	d.hardwareInfo = diaglib.HardwareInfo{
		RAM:  ramInfo,
		Disk: diskInfo,
		CPU:  cpuInfo,
	}
	d.mu.Unlock()
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

func (d *DiagnosticClient) HardwareInfo() diaglib.HardwareInfo {
	return d.hardwareInfo
}

func (d *DiagnosticClient) Peers() map[string]*diaglib.PeerStatistics {
	stats := make(map[string]*diaglib.PeerStatistics)

	d.peersSyncMap.Range(func(key, value interface{}) bool {

		if loadedKey, ok := key.(string); ok {
			if loadedValue, ok := value.(diaglib.PeerStatistics); ok {
				stats[loadedKey] = &loadedValue
			} else {
				log.Debug("Failed to cast value to PeerStatistics struct", value)
			}
		} else {
			log.Debug("Failed to cast key to string", key)
		}

		return true
	})

	d.PeerDataResetStatistics()

	return stats
}

func (d *DiagnosticClient) PeerDataResetStatistics() {
	d.peersSyncMap.Range(func(key, value interface{}) bool {
		if stats, ok := value.(diaglib.PeerStatistics); ok {
			stats.BytesIn = 0
			stats.BytesOut = 0
			stats.CapBytesIn = make(map[string]uint64)
			stats.CapBytesOut = make(map[string]uint64)
			stats.TypeBytesIn = make(map[string]uint64)
			stats.TypeBytesOut = make(map[string]uint64)

			d.peersSyncMap.Store(key, stats)
		} else {
			log.Debug("Failed to cast value to PeerStatistics struct", value)
		}

		return true
	})
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

func (d *DiagnosticClient) runCollectPeersStatistics() {
	go func() {
		ctx, ch, cancel := diaglib.Context[diaglib.PeerStatisticMsgUpdate](context.Background(), 1)
		defer cancel()

		rootCtx, _ := common.RootContext()

		diaglib.StartProviders(ctx, diaglib.TypeOf(diaglib.PeerStatisticMsgUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				cancel()
				return
			case info := <-ch:
				if value, ok := d.peersSyncMap.Load(info.PeerID); ok {
					if stats, ok := value.(diaglib.PeerStatistics); ok {
						if info.Inbound {
							stats.BytesIn += uint64(info.Bytes)
							stats.CapBytesIn[info.MsgCap] += uint64(info.Bytes)
							stats.TypeBytesIn[info.MsgType] += uint64(info.Bytes)
						} else {
							stats.BytesOut += uint64(info.Bytes)
							stats.CapBytesOut[info.MsgCap] += uint64(info.Bytes)
							stats.TypeBytesOut[info.MsgType] += uint64(info.Bytes)
						}

						d.peersSyncMap.Store(info.PeerID, stats)
					} else {
						log.Debug("Failed to cast value to PeerStatistics struct", value)
					}
				} else {
					d.peersSyncMap.Store(info.PeerID, diaglib.PeerStatistics{
						PeerType:     info.PeerType,
						CapBytesIn:   make(map[string]uint64),
						CapBytesOut:  make(map[string]uint64),
						TypeBytesIn:  make(map[string]uint64),
						TypeBytesOut: make(map[string]uint64),
					})
				}
			}
		}
	}()
}
