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

package diagnostics

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
)

type SnapshotsStats struct {
	TotalBytes          int64
	DownloadedBytes     int64
	DownloadRate        float64
	AverageDownloadRate float64
	UploadedBytes       int64
	UploadRate          float64
	AverageUploadRate   float64
	TotalSeenPeers      []TorrentPeerStats
	Peers               []int
	Torrents            []TorrentStats
}

// TODO: check wether I need uint64 or int or something else
type TorrentStats struct {
	TorrentName             string
	TorrentHash             string
	TotalBytes              int64
	DownloadedBytes         int64
	DownloadRate            int64
	UploadedBytes           int64
	UploadRate              float64
	TotalSeenPeers          []int
	Peers                   []int
	TotalPieces             int
	PiecesCompleted         int
	PiecesPartialyCompleted int
	StartTime               time.Time
}

type TorrentStatsUpdate struct {
	TorrentName             string
	DownloadedBytes         int64
	DownloadRate            int64
	UploadedBytes           int64
	Peers                   [][20]byte
	PiecesCompleted         int
	PiecesPartialyCompleted int
}

type TorrentPeerStats struct {
	PeerId       [20]byte
	RemoteAddr   string
	PiecesCount  uint64
	DownloadRate float64
}

func (d *DiagnosticClient) runTorrentDownloaderListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[TorrentStats](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(TorrentStats{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.AddTorrentStats(info)
			}
		}
	}()
}

func (d *DiagnosticClient) AddTorrentStats(stats TorrentStats) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.addTorrentStats(stats)
}

func (d *DiagnosticClient) addTorrentStats(stats TorrentStats) {
	d.snapshotsStats.Torrents = append(d.snapshotsStats.Torrents, stats)

	d.snapshotsStats.TotalBytes += stats.TotalBytes
}

func (d *DiagnosticClient) runTorrentUpdateListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[TorrentStatsUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(TorrentStatsUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.UpdateTorrentStats(info)
			}
		}
	}()
}

func (d *DiagnosticClient) UpdateTorrentStats(stats TorrentStatsUpdate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.updateTorrentStats(stats)
}

func (d *DiagnosticClient) updateTorrentStats(stats TorrentStatsUpdate) {
	for idx, torrent := range d.snapshotsStats.Torrents {
		if torrent.TorrentName == stats.TorrentName {
			peersIdxs := make([]int, 0)

			for idx, peer := range d.snapshotsStats.TotalSeenPeers {
				for _, peerId := range stats.Peers {
					if peerId == peer.PeerId {
						peersIdxs = append(peersIdxs, idx)
					}
				}
			}

			d.snapshotsStats.DownloadedBytes += stats.DownloadedBytes

			d.snapshotsStats.Torrents[idx].DownloadRate = stats.DownloadRate
			d.snapshotsStats.Torrents[idx].DownloadedBytes = stats.DownloadedBytes
			d.snapshotsStats.Torrents[idx].UploadedBytes = stats.UploadedBytes
			d.snapshotsStats.Torrents[idx].Peers = peersIdxs
			d.snapshotsStats.Torrents[idx].TotalSeenPeers = mergePeerIndexes(d.snapshotsStats.Torrents[idx].TotalSeenPeers, peersIdxs)
			d.snapshotsStats.Torrents[idx].PiecesCompleted = stats.PiecesCompleted
			d.snapshotsStats.Torrents[idx].PiecesPartialyCompleted = stats.PiecesPartialyCompleted
		}
	}
}

func mergePeerIndexes(arr1, arr2 []int) []int {
	uniqueMap := make(map[int]bool)
	var mergedArr []int

	for _, v := range arr1 {
		if !uniqueMap[v] {
			mergedArr = append(mergedArr, v)
			uniqueMap[v] = true
		}
	}

	for _, v := range arr2 {
		if !uniqueMap[v] {
			mergedArr = append(mergedArr, v)
			uniqueMap[v] = true
		}
	}

	return mergedArr
}

func (d *DiagnosticClient) runTorrentPeersListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[TorrentPeerStats](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(TorrentPeerStats{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.UpdateTorrentPeerStats(info)
			}
		}
	}()
}

func (d *DiagnosticClient) UpdateTorrentPeerStats(stats TorrentPeerStats) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.updateTorrentPeerStats(stats)
}

func (d *DiagnosticClient) updateTorrentPeerStats(stats TorrentPeerStats) {
	for idx, peer := range d.snapshotsStats.TotalSeenPeers {
		if peer.PeerId == stats.PeerId {
			d.snapshotsStats.TotalSeenPeers[idx] = stats
			return
		}
	}

	d.snapshotsStats.TotalSeenPeers = append(d.snapshotsStats.TotalSeenPeers, stats)
}

func (d *DiagnosticClient) TorrentStatsJson(w io.Writer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := json.NewEncoder(w).Encode(d.snapshotsStats); err != nil {
		log.Debug("[diagnostics] d.snapshotsStats", "err", err)
	}
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
		val.PiecesCount = info.PiecesCount
		val.PiecesCompleted = info.PiecesCompleted

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

func (d *DiagnosticClient) updateSnapshotStageStats(stats SyncStageStats, subStageInfo string) {
	idxs := d.getCurrentSyncIdxs()
	if idxs.Stage == -1 || idxs.SubStage == -1 {
		log.Debug("[Diagnostics] Can't find running stage or substage while updating Snapshots stage stats.", "stages:", d.syncStages, "stats:", stats, "subStageInfo:", subStageInfo)
		return
	}

	d.syncStages[idxs.Stage].SubStages[idxs.SubStage].Stats = stats
}
