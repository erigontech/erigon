package diagnostics

import (
	"context"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

var (
	StagesListKey   = []byte("diagStagesList")
	CurrentStageKey = []byte("diagCurrentStage")
)

func (d *DiagnosticClient) setupStagesDiagnostics(rootCtx context.Context) {
	d.runCurrentSyncStageListener(rootCtx)
	d.runSyncStagesListListener(rootCtx)
}

func (d *DiagnosticClient) runSyncStagesListListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SyncStagesList](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SyncStagesList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				if err := d.db.Update(d.ctx, StagesListUpdater(info.Stages)); err != nil {
					log.Error("[Diagnostics] Failed to update stages list", "err", err)
				}

				d.mu.Lock()
				d.syncStats.SyncStages.StagesList = info.Stages
				d.mu.Unlock()
				return
			}
		}
	}()
}

func (d *DiagnosticClient) runCurrentSyncStageListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[CurrentSyncStage](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(CurrentSyncStage{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				if err := d.db.Update(d.ctx, CurrentStageUpdater(info.Stage)); err != nil {
					log.Error("[Diagnostics] Failed to update current stage", "err", err)
				}

				d.mu.Lock()
				d.syncStats.SyncStages.CurrentStage = info.Stage
				if int(d.syncStats.SyncStages.CurrentStage) >= len(d.syncStats.SyncStages.StagesList) {
					return
				}
				d.mu.Unlock()
			}
		}
	}()
}

func ReadSyncStagesInfo(db kv.RoDB) (info SyncStages) {
	stageesList := ReadStagesList(db)
	currentStage := ReadCurrentStage(db)

	return SyncStages{
		StagesList:   stageesList,
		CurrentStage: currentStage,
	}
}

func ReadStagesList(db kv.RoDB) []string {
	data := ReadDataFromTable(db, kv.DiagSyncStages, StagesListKey)
	var info []string
	err := json.Unmarshal(data, &info)

	if len(data) == 0 {
		return []string{}
	}

	if err != nil {
		log.Error("[Diagnostics] Failed to read stages list", "err", err)
		return []string{}
	} else {
		return info
	}
}

func ReadCurrentStage(db kv.RoDB) uint {
	data := ReadDataFromTable(db, kv.DiagSyncStages, CurrentStageKey)
	var info uint
	err := json.Unmarshal(data, &info)

	if len(data) == 0 {
		return 0
	}

	if err != nil {
		log.Error("[Diagnostics] Failed to read current stage", "err", err)
		return 0
	} else {
		return info
	}
}

func StagesListUpdater(info []string) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSyncStages, StagesListKey, info)
}

func CurrentStageUpdater(info uint) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSyncStages, CurrentStageKey, info)
}
