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
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"
)

func (d *DiagnosticClient) runSegmentIndexingListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SnapshotIndexingStatistics](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SnapshotIndexingStatistics{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.AddOrUpdateSegmentIndexingState(info)
				indexingFinished := d.UpdateIndexingStatus()
				if indexingFinished {
					d.SaveData()
					return
				}
			}
		}
	}()
}

func (d *DiagnosticClient) AddOrUpdateSegmentIndexingState(upd SnapshotIndexingStatistics) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.addOrUpdateSegmentIndexingState(upd)
}

func (d *DiagnosticClient) addOrUpdateSegmentIndexingState(upd SnapshotIndexingStatistics) {
	if d.syncStats.SnapshotIndexing.Segments == nil {
		d.syncStats.SnapshotIndexing.Segments = []SnapshotSegmentIndexingStatistics{}
	}

	for i := range upd.Segments {
		found := false
		for j := range d.syncStats.SnapshotIndexing.Segments {
			if d.syncStats.SnapshotIndexing.Segments[j].SegmentName == upd.Segments[i].SegmentName {
				d.syncStats.SnapshotIndexing.Segments[j].Percent = upd.Segments[i].Percent
				d.syncStats.SnapshotIndexing.Segments[j].Alloc = upd.Segments[i].Alloc
				d.syncStats.SnapshotIndexing.Segments[j].Sys = upd.Segments[i].Sys
				found = true
				break
			}
		}

		if !found {
			d.syncStats.SnapshotIndexing.Segments = append(d.syncStats.SnapshotIndexing.Segments, upd.Segments[i])
		}
	}

	// If elapsed time is equal to minus one it menas that indexing took less than main loop update and we should not update it
	if upd.TimeElapsed != -1 {
		d.syncStats.SnapshotIndexing.TimeElapsed = upd.TimeElapsed
	}
}

func (d *DiagnosticClient) UpdateIndexingStatus() (indexingFinished bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.updateIndexingStatus()
}

func (d *DiagnosticClient) updateIndexingStatus() (indexingFinished bool) {
	segments := d.syncStats.SnapshotIndexing.Segments
	if len(segments) == 0 {
		// If there are no segments, update progress as 0% and return false.
		d.updateSnapshotStageStats(SyncStageStats{
			TimeElapsed: SecondsToHHMMString(uint64(d.syncStats.SnapshotIndexing.TimeElapsed)),
			TimeLeft:    "unknown",
			Progress:    "0%",
		}, "Indexing snapshots")
		return false
	}

	totalProgressPercent := 0
	for _, seg := range segments {
		totalProgressPercent += seg.Percent
	}

	totalProgress := totalProgressPercent / len(segments)

	d.updateSnapshotStageStats(SyncStageStats{
		TimeElapsed: SecondsToHHMMString(uint64(d.syncStats.SnapshotIndexing.TimeElapsed)),
		TimeLeft:    "unknown",
		Progress:    fmt.Sprintf("%d%%", totalProgress),
	}, "Indexing snapshots")

	if totalProgress >= 100 {
		d.syncStats.SnapshotIndexing.IndexingFinished = true
	}

	return d.syncStats.SnapshotIndexing.IndexingFinished
}
