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

import (
	"time"
)

type SyncStageType string

const (
	Snapshots           SyncStageType = "Snapshots"
	BlockHashes         SyncStageType = "BlockHashes"
	Senders             SyncStageType = "Senders"
	Execution           SyncStageType = "Execution"
	HashState           SyncStageType = "HashState"
	IntermediateHashes  SyncStageType = "IntermediateHashes"
	CallTraces          SyncStageType = "CallTraces"
	AccountHistoryIndex SyncStageType = "AccountHistoryIndex"
	StorageHistoryIndex SyncStageType = "StorageHistoryIndex"
	LogIndex            SyncStageType = "LogIndex"
	TxLookup            SyncStageType = "TxLookup"
	Finish              SyncStageType = "Finish"
)

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
	SnapshotDownload SnapshotDownloadStatistics `json:"snapshotDownload"`
	SnapshotIndexing SnapshotIndexingStatistics `json:"snapshotIndexing"`
	BlockExecution   BlockExecutionStatistics   `json:"blockExecution"`
	SyncFinished     bool                       `json:"syncFinished"`
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
	Name            string                   `json:"name"`
	TotalBytes      uint64                   `json:"totalBytes"`
	DownloadedBytes uint64                   `json:"downloadedBytes"`
	Webseeds        []SegmentPeer            `json:"webseeds"`
	Peers           []SegmentPeer            `json:"peers"`
	DownloadedStats FileDownloadedStatistics `json:"downloadedStats"`
}

type FileDownloadedStatistics struct {
	TimeTook    float64 `json:"timeTook"`
	AverageRate uint64  `json:"averageRate"`
}

type FileDownloadedStatisticsUpdate struct {
	FileName    string  `json:"fileName"`
	TimeTook    float64 `json:"timeTook"`
	AverageRate uint64  `json:"averageRate"`
}

type SegmentPeer struct {
	Url          string `json:"url"`
	DownloadRate uint64 `json:"downloadRate"`
}

type SnapshotIndexingStatistics struct {
	Segments         []SnapshotSegmentIndexingStatistics `json:"segments"`
	TimeElapsed      float64                             `json:"timeElapsed"`
	IndexingFinished bool                                `json:"indexingFinished"`
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

type HeadersWaitingUpdate struct {
	From uint64 `json:"from"`
}

type HeaderCanonicalMarkerUpdate struct {
	AncestorHeight uint64 `json:"ancestorHeight"`
	AncestorHash   string `json:"ancestorHash"`
}
type HeadersProcessedUpdate struct {
	Highest   uint64  `json:"highest"`
	Age       int     `json:"age"`
	Headers   uint64  `json:"headers"`
	In        float64 `json:"in"`
	BlkPerSec uint64  `json:"blkPerSec"`
}

type Headers struct {
	WaitingForHeaders uint64                      `json:"waitingForHeaders"`
	WriteHeaders      BlockHeadersUpdate          `json:"writeHeaders"`
	CanonicalMarker   HeaderCanonicalMarkerUpdate `json:"canonicalMarker"`
	Processed         HeadersProcessedUpdate      `json:"processed"`
}
type BodiesInfo struct {
	BlockDownload BodiesDownloadBlockUpdate `json:"blockDownload"`
	BlockWrite    BodiesWriteBlockUpdate    `json:"blockWrite"`
	Processed     BodiesProcessedUpdate     `json:"processed"`
	Processing    BodiesProcessingUpdate    `json:"processing"`
}

type BodiesDownloadBlockUpdate struct {
	BlockNumber    uint64 `json:"blockNumber"`
	DeliveryPerSec uint64 `json:"deliveryPerSec"`
	WastedPerSec   uint64 `json:"wastedPerSec"`
	Remaining      uint64 `json:"remaining"`
	Delivered      uint64 `json:"delivered"`
	BlockPerSec    uint64 `json:"blockPerSec"`
	Cache          uint64 `json:"cache"`
	Alloc          uint64 `json:"alloc"`
	Sys            uint64 `json:"sys"`
}

type BodiesWriteBlockUpdate struct {
	BlockNumber uint64 `json:"blockNumber"`
	Remaining   uint64 `json:"remaining"`
	Alloc       uint64 `json:"alloc"`
	Sys         uint64 `json:"sys"`
}

type BodiesProcessedUpdate struct {
	HighestBlock uint64  `json:"highestBlock"`
	Blocks       uint64  `json:"blocks"`
	TimeElapsed  float64 `json:"timeElapsed"`
	BlkPerSec    float64 `json:"blkPerSec"`
}

type BodiesProcessingUpdate struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

type ResourcesUsage struct {
	MemoryUsage []MemoryStats `json:"memoryUsage"`
}

type MemoryStats struct {
	Alloc       uint64 `json:"alloc"`
	Sys         uint64 `json:"sys"`
	OtherFields []interface{}
	Timestamp   time.Time             `json:"timestamp"`
	StageIndex  CurrentSyncStagesIdxs `json:"stageIndex"`
}

type NetworkSpeedTestResult struct {
	Latency       time.Duration `json:"latency"`
	DownloadSpeed float64       `json:"downloadSpeed"`
	UploadSpeed   float64       `json:"uploadSpeed"`
	PacketLoss    float64       `json:"packetLoss"`
}

func (ti FileDownloadedStatisticsUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti MemoryStats) Type() Type {
	return TypeOf(ti)
}

func (ti BodiesProcessingUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti BodiesProcessedUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti BodiesWriteBlockUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti BodiesDownloadBlockUpdate) Type() Type {
	return TypeOf(ti)
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

func (ti PeerStatisticMsgUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti HeadersWaitingUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti HeaderCanonicalMarkerUpdate) Type() Type {
	return TypeOf(ti)
}

func (ti HeadersProcessedUpdate) Type() Type {
	return TypeOf(ti)
}
