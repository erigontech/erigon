/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package diagnostics

type PeerStatisticsGetter interface {
	GetPeersStatistics() map[string]*PeerStatistics
}

type PeerStatistics struct {
	BytesIn      uint64
	BytesOut     uint64
	CapBytesIn   map[string]uint64
	CapBytesOut  map[string]uint64
	TypeBytesIn  map[string]uint64
	TypeBytesOut map[string]uint64
}

type SyncStatistics struct {
	SyncStages       SyncStages                 `json:"syncStages"`
	SnapshotDownload SnapshotDownloadStatistics `json:"snapshotDownload"`
	SnapshotIndexing SnapshotIndexingStatistics `json:"snapshotIndexing"`
}

type SnapshotDownloadStatistics struct {
	Downloaded           uint64                               `json:"downloaded"`
	Total                uint64                               `json:"total"`
	TotalTime            float64                              `json:"totalTime"`
	DownloadRate         uint64                               `json:"downloadRate"`
	UploadRate           uint64                               `json:"uploadRate"`
	Peers                int32                                `json:"peers"`
	Files                int32                                `json:"files"`
	Connections          uint64                               `json:"connections"`
	Alloc                uint64                               `json:"alloc"`
	Sys                  uint64                               `json:"sys"`
	DownloadFinished     bool                                 `json:"downloadFinished"`
	SegmentsDownloading  map[string]SegmentDownloadStatistics `json:"segmentsDownloading"`
	TorrentMetadataReady int32                                `json:"torrentMetadataReady"`
}

type SegmentDownloadStatistics struct {
	Name            string `json:"name"`
	TotalBytes      uint64 `json:"totalBytes"`
	DownloadedBytes uint64 `json:"downloadedBytes"`
	WebseedsCount   int    `json:"webseedsCount"`
	PeersCount      int    `json:"peersCount"`
	WebseedsRate    uint64 `json:"webseedsRate"`
	PeersRate       uint64 `json:"peersRate"`
}

type SnapshotIndexingStatistics struct {
	Segments    []SnapshotSegmentIndexingStatistics `json:"segments"`
	TimeElapsed float64                             `json:"timeElapsed"`
}

type SnapshotSegmentIndexingStatistics struct {
	SegmentName string `json:"segmentName"`
	Percent     int    `json:"percent"`
	Alloc       uint64 `json:"alloc"`
	Sys         uint64 `json:"sys"`
}

type SnapshotSegmentIndexingFinishedUpdate struct {
	SegmentName string `json:"segmentName"`
}

type SyncStagesList struct {
	Stages []string `json:"stages"`
}

type CurrentSyncStage struct {
	Stage uint `json:"stage"`
}

type SyncStages struct {
	StagesList   []string `json:"stagesList"`
	CurrentStage uint     `json:"currentStage"`
}

func (ti SnapshotDownloadStatistics) Type() Type {
	return TypeOf(ti)
}

func (ti SegmentDownloadStatistics) Type() Type {
	return TypeOf(ti)
}

func (ti SnapshotIndexingStatistics) Type() Type {
	return TypeOf(ti)
}

func (ti SnapshotSegmentIndexingFinishedUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti SyncStagesList) Type() Type {
	return TypeOf(ti)
}

func (ti CurrentSyncStage) Type() Type {
	return TypeOf(ti)
}
