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
	"fmt"
	"io"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

var (
	SnapshotDownloadStatisticsKey = []byte("diagSnapshotDownloadStatistics")
	SnapshotIndexingStatisticsKey = []byte("diagSnapshotIndexingStatistics")
	SnapshotFillDBStatisticsKey   = []byte("diagSnapshotFillDBStatistics")
)

func (d *DiagnosticClient) setupSnapshotDiagnostics(rootCtx context.Context) {
	d.runSnapshotListener(rootCtx)
	d.runSegmentDownloadingListener(rootCtx)
	d.runSnapshotFilesListListener(rootCtx)
	d.runSegmentIndexingListener(rootCtx)
	d.runFileDownloadedListener(rootCtx)
	d.runFillDBListener(rootCtx)
}

func (d *DiagnosticClient) runFillDBListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SnapshotFillDBStageUpdate](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SnapshotFillDBStageUpdate{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.SetFillDBInfo(info.Stage)

				totalTimeString := time.Duration(info.TimeElapsed) * time.Second

				d.UpdateSnapshotStageStats(SyncStageStats{
					TimeElapsed: totalTimeString.String(),
					TimeLeft:    "unknown",
					Progress:    fmt.Sprintf("%d%%", (info.Stage.Current*100)/info.Stage.Total),
				}, "Fill DB from snapshots")
				d.SaveSnapshotStageStatsToDB()
			}
		}
	}()
}

func (d *DiagnosticClient) SetFillDBInfo(info SnapshotFillDBStage) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.setFillDBInfo(info)
}

func (d *DiagnosticClient) setFillDBInfo(info SnapshotFillDBStage) {
	if d.syncStats.SnapshotFillDB.Stages == nil {
		d.syncStats.SnapshotFillDB.Stages = []SnapshotFillDBStage{info}
	} else {

		for idx, stg := range d.syncStats.SnapshotFillDB.Stages {
			if stg.StageName == info.StageName {
				d.syncStats.SnapshotFillDB.Stages[idx] = info
				break
			}
		}
	}
}

func (d *DiagnosticClient) SaveSnapshotStageStatsToDB() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.saveSnapshotStageStatsToDB()
}

func (d *DiagnosticClient) saveSnapshotStageStatsToDB() {
	err := d.db.Update(d.ctx, func(tx kv.RwTx) error {
		err := SnapshotFillDBUpdater(d.syncStats.SnapshotFillDB)(tx)
		if err != nil {
			return err
		}

		err = StagesListUpdater(d.syncStages)(tx)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Debug("[Diagnostics] Failed to update snapshot download info", "err", err)
	}
}

// Deprecated - it's not thread-safe and used only in tests. Need introduce another method or add special methods for Tests.
func (d *DiagnosticClient) SyncStatistics() SyncStatistics {
	var newStats SyncStatistics
	statsBytes, err := json.Marshal(d.syncStats)
	if err != nil {
		return SyncStatistics{}
	}
	err = json.Unmarshal(statsBytes, &newStats)
	if err != nil {
		return SyncStatistics{}
	}
	return newStats
}

func (d *DiagnosticClient) SyncStatsJson(w io.Writer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := json.NewEncoder(w).Encode(d.syncStats); err != nil {
		log.Debug("[diagnostics] SyncStatsJson", "err", err)
	}
}

func (d *DiagnosticClient) SnapshotFilesListJson(w io.Writer) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := json.NewEncoder(w).Encode(d.snapshotFileList); err != nil {
		log.Debug("[diagnostics] SnapshotFilesListJson", "err", err)
	}
}

func SnapshotDownloadInfoFromTx(tx kv.Tx) ([]byte, error) {
	bytes, err := ReadDataFromTable(tx, kv.DiagSyncStages, SnapshotDownloadStatisticsKey)
	if err != nil {
		return nil, err
	}

	return common.CopyBytes(bytes), nil
}

func SnapshotIndexingInfoFromTx(tx kv.Tx) ([]byte, error) {
	bytes, err := ReadDataFromTable(tx, kv.DiagSyncStages, SnapshotIndexingStatisticsKey)
	if err != nil {
		return nil, err
	}

	return common.CopyBytes(bytes), nil
}

func SnapshotFillDBInfoFromTx(tx kv.Tx) ([]byte, error) {
	bytes, err := ReadDataFromTable(tx, kv.DiagSyncStages, SnapshotFillDBStatisticsKey)
	if err != nil {
		return nil, err
	}

	return common.CopyBytes(bytes), nil
}

func SnapshotDownloadUpdater(info SnapshotDownloadStatistics) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSyncStages, SnapshotDownloadStatisticsKey, info)
}

func SnapshotIndexingUpdater(info SnapshotIndexingStatistics) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSyncStages, SnapshotIndexingStatisticsKey, info)
}

func SnapshotFillDBUpdater(info SnapshotFillDBStatistics) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSyncStages, SnapshotFillDBStatisticsKey, info)
}
