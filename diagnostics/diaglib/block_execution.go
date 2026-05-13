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
	"sync"
)

type BlockEexcStatsData struct {
	data BlockExecutionStatistics
	mu   sync.Mutex
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
