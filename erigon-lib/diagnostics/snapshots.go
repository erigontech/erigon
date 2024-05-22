package diagnostics

import (
	"context"

	"github.com/ledgerwatch/log/v3"
)

func (d *DiagnosticClient) setupSnapshotDiagnostics(rootCtx context.Context) {
	d.runSnapshotListener(rootCtx)
	d.runSegmentDownloadingListener(rootCtx)
	d.runSegmentIndexingListener(rootCtx)
	d.runSegmentIndexingFinishedListener(rootCtx)
	d.runSnapshotFilesListListener(rootCtx)
	d.runFileDownloadedListener(rootCtx)
}

func (d *DiagnosticClient) runSnapshotListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SnapshotDownloadStatistics](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SnapshotDownloadStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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

func (d *DiagnosticClient) runSegmentDownloadingListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SegmentDownloadStatistics](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SegmentDownloadStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.mu.Lock()
				if d.syncStats.SnapshotDownload.SegmentsDownloading == nil {
					d.syncStats.SnapshotDownload.SegmentsDownloading = map[string]SegmentDownloadStatistics{}
				}

				if val, ok := d.syncStats.SnapshotDownload.SegmentsDownloading[info.Name]; ok {
					val.TotalBytes = info.TotalBytes
					val.DownloadedBytes = info.DownloadedBytes
					val.Webseeds = info.Webseeds
					val.Peers = info.Peers

					d.syncStats.SnapshotDownload.SegmentsDownloading[info.Name] = val
				} else {
					d.syncStats.SnapshotDownload.SegmentsDownloading[info.Name] = info
				}

				d.mu.Unlock()
			}
		}
	}()
}

func (d *DiagnosticClient) runSegmentIndexingListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SnapshotIndexingStatistics](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SnapshotIndexingStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.addOrUpdateSegmentIndexingState(info)
			}
		}
	}()
}

func (d *DiagnosticClient) runSegmentIndexingFinishedListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SnapshotSegmentIndexingFinishedUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SnapshotSegmentIndexingFinishedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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
					d.syncStats.SnapshotIndexing.Segments = append(d.syncStats.SnapshotIndexing.Segments, SnapshotSegmentIndexingStatistics{
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

func (d *DiagnosticClient) addOrUpdateSegmentIndexingState(upd SnapshotIndexingStatistics) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.syncStats.SnapshotIndexing.Segments == nil {
		d.syncStats.SnapshotIndexing.Segments = []SnapshotSegmentIndexingStatistics{}
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

func (d *DiagnosticClient) runSnapshotFilesListListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SnapshoFilesList](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SnapshoFilesList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
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

func (d *DiagnosticClient) runFileDownloadedListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[FileDownloadedStatisticsUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(FileDownloadedStatisticsUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.mu.Lock()

				if d.syncStats.SnapshotDownload.SegmentsDownloading == nil {
					d.syncStats.SnapshotDownload.SegmentsDownloading = map[string]SegmentDownloadStatistics{}
				}

				if val, ok := d.syncStats.SnapshotDownload.SegmentsDownloading[info.FileName]; ok {
					val.DownloadedStats = FileDownloadedStatistics{
						TimeTook:    info.TimeTook,
						AverageRate: info.AverageRate,
					}

					d.syncStats.SnapshotDownload.SegmentsDownloading[info.FileName] = val
				} else {
					d.syncStats.SnapshotDownload.SegmentsDownloading[info.FileName] = SegmentDownloadStatistics{
						Name:            info.FileName,
						TotalBytes:      0,
						DownloadedBytes: 0,
						Webseeds:        nil,
						Peers:           nil,
						DownloadedStats: FileDownloadedStatistics{
							TimeTook:    info.TimeTook,
							AverageRate: info.AverageRate,
						},
					}
				}

				d.mu.Unlock()
			}
		}
	}()
}

func (d *DiagnosticClient) UpdateFileDownloadedStatistics(downloadedInfo *FileDownloadedStatisticsUpdate, downloadingInfo *SegmentDownloadStatistics) {
	if d.syncStats.SnapshotDownload.SegmentsDownloading == nil {
		d.syncStats.SnapshotDownload.SegmentsDownloading = map[string]SegmentDownloadStatistics{}
	}

	if downloadedInfo != nil {
		dwStats := FileDownloadedStatistics{
			TimeTook:    downloadedInfo.TimeTook,
			AverageRate: downloadedInfo.AverageRate,
		}
		if val, ok := d.syncStats.SnapshotDownload.SegmentsDownloading[downloadedInfo.FileName]; ok {
			val.DownloadedStats = dwStats

			d.syncStats.SnapshotDownload.SegmentsDownloading[downloadedInfo.FileName] = val
		} else {
			d.syncStats.SnapshotDownload.SegmentsDownloading[downloadedInfo.FileName] = SegmentDownloadStatistics{
				Name:            downloadedInfo.FileName,
				TotalBytes:      0,
				DownloadedBytes: 0,
				Webseeds:        make([]SegmentPeer, 0),
				Peers:           make([]SegmentPeer, 0),
				DownloadedStats: dwStats,
			}
		}
	} else {
		if val, ok := d.syncStats.SnapshotDownload.SegmentsDownloading[downloadingInfo.Name]; ok {
			val.TotalBytes = downloadingInfo.TotalBytes
			val.DownloadedBytes = downloadingInfo.DownloadedBytes
			val.Webseeds = downloadingInfo.Webseeds
			val.Peers = downloadingInfo.Peers

			d.syncStats.SnapshotDownload.SegmentsDownloading[downloadingInfo.Name] = val
		} else {
			d.syncStats.SnapshotDownload.SegmentsDownloading[downloadingInfo.Name] = *downloadingInfo
		}
	}
}

func (d *DiagnosticClient) SyncStatistics() SyncStatistics {
	return d.syncStats
}

func (d *DiagnosticClient) SnapshotFilesList() SnapshoFilesList {
	return d.snapshotFileList
}
