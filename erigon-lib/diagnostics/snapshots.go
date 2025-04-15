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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
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

	/*go func() {
		for {
			time.Sleep(1 * time.Second)
			d.SaveSnaphotsSyncToJsonFile()
		}
	}()*/
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

// //
func (d *DiagnosticClient) SaveSnaphotsSyncToJsonFile() {
	d.mu.Lock()
	defer d.mu.Unlock()

	var data []byte

	// Check if file exists
	if _, err := os.Stat("/app/sync_messages.json"); err == nil {
		// File exists, read existing content
		existingData, err := os.ReadFile("/app/sync_messages.json")
		if err != nil {
			log.Error("[Diagnostics] Error reading existing sync messages file", "err", err)
			return
		}

		// Parse existing messages
		var messages []SyncStatistics
		if err := json.Unmarshal(existingData, &messages); err != nil {
			log.Error("[Diagnostics] Error unmarshalling existing sync messages", "err", err)
			return
		}

		// Append new stats
		messages = append(messages, d.syncStats)

		// Marshal and write back
		data, err = json.Marshal(messages)
		if err != nil {
			log.Error("[Diagnostics] Error marshalling updated sync messages", "err", err)
			return
		}
	} else {
		// File doesn't exist, create new array with single message
		messages := []SyncStatistics{d.syncStats}
		data, err = json.Marshal(messages)
		if err != nil {
			log.Error("[Diagnostics] Error marshalling new sync messages", "err", err)
			return
		}
	}

	// Write to file
	if err := os.WriteFile("/app/sync_messages.json", data, 0644); err != nil {
		log.Error("[Diagnostics] Error writing sync messages to file", "err", err)
		return
	}
	log.Info("[Diagnostics] Sync messages saved to sync_messages.json")
}
