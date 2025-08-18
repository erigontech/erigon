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

	"github.com/erigontech/erigon-lib/log/v3"
)

func (d *DiagnosticClient) setupResourcesUsageDiagnostics(rootCtx context.Context) {
	d.runMemoryStatsListener(rootCtx)
}

func (d *DiagnosticClient) ResourcesUsageJson(w io.Writer) {
	d.resourcesUsageMutex.Lock()
	defer d.resourcesUsageMutex.Unlock()

	returnObj := d.resourcesUsage
	d.resourcesUsage = ResourcesUsage{}

	if err := json.NewEncoder(w).Encode(returnObj); err != nil {
		log.Debug("[diagnostics] ResourcesUsageJson", "err", err)
	}
}

func (d *DiagnosticClient) runMemoryStatsListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[MemoryStats](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(MemoryStats{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.resourcesUsageMutex.Lock()
				info.StageIndex = d.GetCurrentSyncIdxs()
				d.resourcesUsage.MemoryUsage = append(d.resourcesUsage.MemoryUsage, info)
				d.resourcesUsageMutex.Unlock()
			}
		}
	}()
}
