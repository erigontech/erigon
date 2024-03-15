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
	PeerType     string
	BytesIn      uint64
	BytesOut     uint64
	CapBytesIn   map[string]uint64
	CapBytesOut  map[string]uint64
	TypeBytesIn  map[string]uint64
	TypeBytesOut map[string]uint64
}

type PeerDataUpdate struct {
	PeerID string
	ENR    string
	Enode  string
	ID     string
	Name   string
	Type   string
	Caps   []string
}

type PeerStatisticMsgUpdate struct {
	PeerType string
	PeerID   string
	Inbound  bool
	MsgType  string
	MsgCap   string
	Bytes    int
}

type SyncStatistics struct {
	SyncStages       SyncStages                 `json:"syncStages"`
	SnapshotDownload SnapshotDownloadStatistics `json:"snapshotDownload"`
	SnapshotIndexing SnapshotIndexingStatistics `json:"snapshotIndexing"`
	BlockExecution   BlockExecutionStatistics   `json:"blockExecution"`
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
	Name            string        `json:"name"`
	TotalBytes      uint64        `json:"totalBytes"`
	DownloadedBytes uint64        `json:"downloadedBytes"`
	Webseeds        []SegmentPeer `json:"webseeds"`
	Peers           []SegmentPeer `json:"peers"`
}

type SegmentPeer struct {
	Url          string `json:"url"`
	DownloadRate uint64 `json:"downloadRate"`
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

type BlockExecutionStatistics struct {
	From        uint64  `json:"from"`
	To          uint64  `json:"to"`
	BlockNumber uint64  `json:"blockNumber"`
	BlkPerSec   float64 `json:"blkPerSec"`
	TxPerSec    float64 `json:"txPerSec"`
	MgasPerSec  float64 `json:"mgasPerSec"`
	GasState    float64 `json:"gasState"`
	Batch       uint64  `json:"batch"`
	Alloc       uint64  `json:"alloc"`
	Sys         uint64  `json:"sys"`
	TimeElapsed float64 `json:"timeElapsed"`
}

type SnapshoFilesList struct {
	Files []string `json:"files"`
}

type HardwareInfo struct {
	Disk DiskInfo `json:"disk"`
	RAM  RAMInfo  `json:"ram"`
	CPU  CPUInfo  `json:"cpu"`
}

type RAMInfo struct {
	Total uint64 `json:"total"`
	Free  uint64 `json:"free"`
}

type DiskInfo struct {
	FsType string `json:"fsType"`
	Total  uint64 `json:"total"`
	Free   uint64 `json:"free"`
}

type CPUInfo struct {
	Cores     int     `json:"cores"`
	ModelName string  `json:"modelName"`
	Mhz       float64 `json:"mhz"`
}

type BlockHeadersUpdate struct {
	CurrentBlockNumber  uint64  `json:"blockNumber"`
	PreviousBlockNumber uint64  `json:"previousBlockNumber"`
	Speed               float64 `json:"speed"`
	Alloc               uint64  `json:"alloc"`
	Sys                 uint64  `json:"sys"`
	InvalidHeaders      int     `json:"invalidHeaders"`
	RejectedBadHeaders  int     `json:"rejectedBadHeaders"`
}

func (ti BlockHeadersUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti SnapshoFilesList) Type() Type {
	return TypeOf(ti)
}

func (ti BlockExecutionStatistics) Type() Type {
	return TypeOf(ti)
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

func (ti PeerStatisticMsgUpdate) Type() Type {
	return TypeOf(ti)
}
