// Copyright 2021 The Erigon Authors
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
	"maps"
	"time"
)

type PeerStatistics struct {
	PeerName     string
	PeerType     string
	BytesIn      uint64
	BytesOut     uint64
	CapBytesIn   map[string]uint64
	CapBytesOut  map[string]uint64
	TypeBytesIn  map[string]uint64
	TypeBytesOut map[string]uint64
}

func (p PeerStatistics) Clone() PeerStatistics {
	p1 := p
	p1.CapBytesIn = maps.Clone(p.CapBytesIn)
	p1.CapBytesOut = maps.Clone(p.CapBytesOut)
	p1.TypeBytesIn = maps.Clone(p.TypeBytesIn)
	p1.TypeBytesOut = maps.Clone(p.TypeBytesOut)
	return p1
}

func (p PeerStatistics) Equal(p2 PeerStatistics) bool {
	return p.PeerType == p2.PeerType &&
		p.BytesIn == p2.BytesIn &&
		p.BytesOut == p2.BytesOut &&
		maps.Equal(p.CapBytesIn, p2.CapBytesIn) &&
		maps.Equal(p.CapBytesOut, p2.CapBytesOut) &&
		maps.Equal(p.TypeBytesIn, p2.TypeBytesIn) &&
		maps.Equal(p.TypeBytesOut, p2.TypeBytesOut) &&
		p.PeerName == p2.PeerName
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
	PeerName string
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
	SnapshotFillDB   SnapshotFillDBStatistics   `json:"snapshotFillDB"`
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
	Url          string   `json:"url"`
	DownloadRate uint64   `json:"downloadRate"`
	UploadRate   uint64   `json:"uploadRate"`
	PiecesCount  uint64   `json:"piecesCount"`
	RemoteAddr   string   `json:"remoteAddr"`
	PeerId       [20]byte `json:"peerId"`
	TorrentName  string   `json:"torrentName"`
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

type SnapshotFillDBStatistics struct {
	Stages []SnapshotFillDBStage `json:"stages"`
}

type SnapshotFillDBStage struct {
	StageName string `json:"stageName"`
	Current   uint64 `json:"current"`
	Total     uint64 `json:"total"`
}

type SnapshotFillDBStageUpdate struct {
	Stage       SnapshotFillDBStage `json:"stage"`
	TimeElapsed float64             `json:"timeElapsed"`
}

type SnapshoFilesList struct {
	Files []string `json:"files"`
}

type HardwareInfo struct {
	Disk DiskInfo  `json:"disk"`
	RAM  RAMInfo   `json:"ram"`
	CPU  []CPUInfo `json:"cpu"`
}

type RAMInfo struct {
	Total       uint64  `json:"total"`
	Available   uint64  `json:"available"`
	Used        uint64  `json:"used"`
	UsedPercent float64 `json:"usedPercent"`
}

type DiskInfo struct {
	FsType     string `json:"fsType"`
	Total      uint64 `json:"total"`
	Free       uint64 `json:"free"`
	MountPoint string `json:"mountPoint"`
	Device     string `json:"device"`
	Details    string `json:"details"`
}

type CPUInfo struct {
	CPU        int32    `json:"cpu"`
	VendorID   string   `json:"vendorId"`
	Family     string   `json:"family"`
	Model      string   `json:"model"`
	Stepping   int32    `json:"stepping"`
	PhysicalID string   `json:"physicalId"`
	CoreID     string   `json:"coreId"`
	Cores      int32    `json:"cores"`
	ModelName  string   `json:"modelName"`
	Mhz        float64  `json:"mhz"`
	CacheSize  int32    `json:"cacheSize"`
	Flags      []string `json:"flags"`
	Microcode  string   `json:"microcode"`
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

func (ti SnapshotFillDBStageUpdate) Type() Type {
	return TypeOf(ti)
}
