// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package diaglib

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"
)

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
				d.SetSnapshotDownloadInfo(info)
				d.UpdateSnapshotStageStats(CalculateSyncStageStats(info), "Downloading snapshots")

				if info.DownloadFinished {
					d.SaveData()
					return
				}
			}
		}
	}()
}

func (d *DiagnosticClient) SetSnapshotDownloadInfo(info SnapshotDownloadStatistics) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.setSnapshotDownloadInfo(info)
}

func (d *DiagnosticClient) setSnapshotDownloadInfo(info SnapshotDownloadStatistics) {
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
				d.SetDownloadSegments(info)
			}
		}
	}()
}

func (d *DiagnosticClient) SetDownloadSegments(info SegmentDownloadStatistics) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.setDownloadSegments(info)
}

func (d *DiagnosticClient) setDownloadSegments(info SegmentDownloadStatistics) {
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
				d.SetSnapshotFilesList(info)

				if len(info.Files) > 0 {
					return
				}
			}
		}
	}()
}

func (d *DiagnosticClient) SetSnapshotFilesList(info SnapshoFilesList) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.setSnapshotFilesList(info)
}

func (d *DiagnosticClient) setSnapshotFilesList(info SnapshoFilesList) {
	d.snapshotFileList = info
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
				d.UpdateFileDownloadedStatistics(&info, nil)
			}
		}
	}()
}

func (d *DiagnosticClient) UpdateFileDownloadedStatistics(downloadedInfo *FileDownloadedStatisticsUpdate, downloadingInfo *SegmentDownloadStatistics) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.updateFileDownloadedStatistics(downloadedInfo, downloadingInfo)
}

func (d *DiagnosticClient) updateFileDownloadedStatistics(downloadedInfo *FileDownloadedStatisticsUpdate, downloadingInfo *SegmentDownloadStatistics) {
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

func (d *DiagnosticClient) UpdateSnapshotStageStats(stats SyncStageStats, subStageInfo string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.updateSnapshotStageStats(stats, subStageInfo)
}

// TODO: Rethink functionality of stages/substages
func (d *DiagnosticClient) updateSnapshotStageStats(stats SyncStageStats, subStageInfo string) {
	idxs := d.getCurrentSyncIdxs()
	if idxs.Stage == -1 || idxs.SubStage == -1 {
		//log.Debug("[Diagnostics] Can't find running stage or substage while updating Snapshots stage stats.", "stages:", d.syncStages, "stats:", stats, "subStageInfo:", subStageInfo)
		return
	}

	d.syncStages[idxs.Stage].SubStages[idxs.SubStage].Stats = stats
}
