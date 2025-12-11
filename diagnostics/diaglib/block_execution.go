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
	"encoding/json"
	"io"
	"sync"

	"github.com/erigontech/erigon-lib/log/v3"
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

func (b *BlockEexcStatsData) SetData(d BlockExecutionStatistics) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = d
}

func (b *BlockEexcStatsData) Data() (d BlockExecutionStatistics) {
	b.mu.Lock()
	d = b.data
	b.mu.Unlock()
	return
}

func (d *DiagnosticClient) setupBlockExecutionDiagnostics(rootCtx context.Context) {
	d.runBlockExecutionListener(rootCtx)
}

func (d *DiagnosticClient) runBlockExecutionListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[BlockExecutionStatistics](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(BlockExecutionStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.BlockExecution.SetData(info)
				if d.syncStats.SyncFinished {
					return
				}
			}
		}
	}()
}

func (d *DiagnosticClient) BlockExecutionInfoJson(w io.Writer) {
	if err := json.NewEncoder(w).Encode(d.BlockExecution.Data()); err != nil {
		log.Debug("[diagnostics] BlockExecutionInfoJson", "err", err)
	}
}
