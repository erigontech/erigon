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

	d.syncStats.SnapshotIndexing.TimeElapsed = upd.TimeElapsed
}

func (d *DiagnosticClient) runSegmentIndexingFinishedListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SnapshotSegmentIndexingFinishedUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SnapshotSegmentIndexingFinishedUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.SegmentIndexed(&info)
				d.UpdateIndexingStatus()
			}
		}
	}()
}

func (d *DiagnosticClient) SegmentIndexed(info *SnapshotSegmentIndexingFinishedUpdate) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.segmentIndexed(info)
}

func (d *DiagnosticClient) segmentIndexed(info *SnapshotSegmentIndexingFinishedUpdate) {
	found := false
	for i := range d.syncStats.SnapshotIndexing.Segments {
		if d.syncStats.SnapshotIndexing.Segments[i].SegmentName == info.SegmentName {
			found = true
			d.syncStats.SnapshotIndexing.Segments[i].Percent = 100
		}
	}

	if !found {
		d.syncStats.SnapshotIndexing.Segments = append(d.syncStats.SnapshotIndexing.Segments, SnapshotSegmentIndexingStatistics{
			SegmentName: info.SegmentName,
			Percent:     100,
			Alloc:       0,
			Sys:         0,
		})
	}
}

func (d *DiagnosticClient) UpdateIndexingStatus() (indexingFinished bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.updateIndexingStatus()
}

func (d *DiagnosticClient) updateIndexingStatus() (indexingFinished bool) {
	totalProgressPercent := 0
	for _, seg := range d.syncStats.SnapshotIndexing.Segments {
		totalProgressPercent += seg.Percent
	}

	totalProgress := totalProgressPercent / len(d.syncStats.SnapshotIndexing.Segments)

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
