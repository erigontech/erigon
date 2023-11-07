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

type DownloadStatistics struct {
	Downloaded       uint64  `json:"downloaded"`
	Total            uint64  `json:"total"`
	TotalTime        float64 `json:"totalTime"`
	DownloadRate     uint64  `json:"downloadRate"`
	UploadRate       uint64  `json:"uploadRate"`
	Peers            int32   `json:"peers"`
	Files            int32   `json:"files"`
	Connections      uint64  `json:"connections"`
	Alloc            uint64  `json:"alloc"`
	Sys              uint64  `json:"sys"`
	DownloadFinished bool    `json:"downloadFinished"`
	StagePrefix      string  `json:"stagePrefix"`
}

func (ti DownloadStatistics) Type() Type {
	return TypeOf(ti)
}
