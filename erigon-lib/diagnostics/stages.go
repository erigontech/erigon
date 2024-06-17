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

type CurrentSyncStagesIdxs struct {
	Stage    int `json:"currentStage"`
	SubStage int `json:"currentSubStage"`
}

type SyncStage struct {
	ID        string         `json:"id"`
	State     StageState     `json:"state"`
	SubStages []SyncSubStage `json:"subStages"`
	Stats     SyncStageStats `json:"stats"`
}

type SyncSubStage struct {
	ID    string         `json:"id"`
	State StageState     `json:"state"`
	Stats SyncStageStats `json:"stats"`
}

type SyncStageStats struct {
	TimeElapsed string `json:"timeElapsed"`
	TimeLeft    string `json:"timeLeft"`
	Progress    string `json:"progress"`
}

type UpdateSyncSubStageList struct {
	List []UpdateSyncSubStage
}

type UpdateSyncSubStage struct {
	StageId  string
	SubStage SyncSubStage
}

func (ti UpdateSyncSubStageList) Type() Type {
	return TypeOf(ti)
}

type SyncStageList struct {
	StagesList []SyncStage `json:"stages"`
}

func (ti SyncStageList) Type() Type {
	return TypeOf(ti)
}

type StageState int

const (
	Queued StageState = iota
	Running
	Completed
)

func (s StageState) String() string {
	return [...]string{"Queued", "Running", "Completed"}[s]
}

type CurrentSyncStage struct {
	Stage string `json:"stage"`
}

func (ti CurrentSyncStage) Type() Type {
	return TypeOf(ti)
}

type CurrentSyncSubStage struct {
	SubStage string `json:"subStage"`
}

func (ti CurrentSyncSubStage) Type() Type {
	return TypeOf(ti)
}

func (d *DiagnosticClient) setupStagesDiagnostics(rootCtx context.Context) {
	d.runSyncStagesListListener(rootCtx)
	d.runCurrentSyncStageListener(rootCtx)
	d.runCurrentSyncSubStageListener(rootCtx)
	d.runSubStageListener(rootCtx)
}

func (d *DiagnosticClient) runSyncStagesListListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SyncStageList](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SyncStageList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.mu.Lock()
				d.SetStagesList(info.StagesList)
				d.mu.Unlock()

				d.saveSyncStagesToDB()
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
				d.mu.Lock()
				d.SetCurrentSyncStage(info)
				d.mu.Unlock()

				d.saveSyncStagesToDB()
			}
		}
	}()
}

func (d *DiagnosticClient) runCurrentSyncSubStageListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[CurrentSyncSubStage](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(CurrentSyncSubStage{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.mu.Lock()
				d.SetCurrentSyncSubStage(info)
				d.mu.Unlock()

				d.saveSyncStagesToDB()
			}
		}
	}()
}

func (d *DiagnosticClient) runSubStageListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[UpdateSyncSubStageList](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(UpdateSyncSubStageList{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				d.mu.Lock()
				d.AddOrUpdateSugStages(info)
				d.mu.Unlock()

				d.saveSyncStagesToDB()
			}
		}
	}()
}

func (d *DiagnosticClient) saveSyncStagesToDB() {
	if err := d.db.Update(d.ctx, StagesListUpdater(d.syncStages)); err != nil {
		log.Error("[Diagnostics] Failed to update stages list", "err", err)
	}
}

func (d *DiagnosticClient) getCurrentSyncIdxs() CurrentSyncStagesIdxs {
	currentIdxs := CurrentSyncStagesIdxs{
		Stage:    -1,
		SubStage: -1,
	}

	for sIdx, stage := range d.syncStages {
		if stage.State == Running {
			currentIdxs.Stage = sIdx

			for subIdx, subStage := range stage.SubStages {
				if subStage.State == Running {
					currentIdxs.SubStage = subIdx
				}
			}
			break
		}
	}

	return currentIdxs
}

func (d *DiagnosticClient) SetStagesList(stages []SyncStage) {
	d.syncStages = stages
}

func (d *DiagnosticClient) SetCurrentSyncStage(css CurrentSyncStage) {
	isSet := false
	for idx, stage := range d.syncStages {
		if !isSet {
			if stage.ID == css.Stage {
				d.syncStages[idx].State = Running
				isSet = true
			} else {
				d.syncStages[idx].State = Completed
				for subIdx := range d.syncStages[idx].SubStages {
					d.syncStages[idx].SubStages[subIdx].State = Completed
				}
			}
		} else {
			d.syncStages[idx].State = Queued
		}
	}
}

func (d *DiagnosticClient) SetCurrentSyncSubStage(css CurrentSyncSubStage) {
	for idx, stage := range d.syncStages {
		if stage.State == Running {
			isSet := false
			for subIdx, subStage := range stage.SubStages {
				if !isSet {
					if subStage.ID == css.SubStage {
						d.syncStages[idx].SubStages[subIdx].State = Running
						isSet = true
					} else {
						d.syncStages[idx].SubStages[subIdx].State = Completed
					}
				} else {
					d.syncStages[idx].SubStages[subIdx].State = Queued
				}
			}

			break
		}
	}
}

func (d *DiagnosticClient) AddOrUpdateSugStages(subStages UpdateSyncSubStageList) {
	for _, subStage := range subStages.List {
		d.AddOrUpdateSugStage(subStage)
	}
}

func (d *DiagnosticClient) AddOrUpdateSugStage(subStage UpdateSyncSubStage) {
	for idx, stage := range d.syncStages {
		if stage.ID == subStage.StageId {
			subStageExist := false
			for subIdx, sub := range stage.SubStages {
				if sub.ID == subStage.SubStage.ID {
					subStageExist = true
					d.syncStages[idx].SubStages[subIdx] = subStage.SubStage
					break
				}
			}

			if !subStageExist {
				d.syncStages[idx].SubStages = append(d.syncStages[idx].SubStages, subStage.SubStage)
			}

			break
		}
	}
}

func ReadSyncStages(db kv.RoDB) []SyncStage {
	data := ReadDataFromTable(db, kv.DiagSyncStages, StagesListKey)

	if len(data) == 0 {
		return []SyncStage{}
	}

	var info []SyncStage
	err := json.Unmarshal(data, &info)

	if err != nil {
		log.Error("[Diagnostics] Failed to read stages list", "err", err)
		return []SyncStage{}
	} else {
		return info
	}
}

func StagesListUpdater(info []SyncStage) func(tx kv.RwTx) error {
	return PutDataToTable(kv.DiagSyncStages, StagesListKey, info)
}

func (d *DiagnosticClient) GetSyncStages() []SyncStage {
	return d.syncStages
}
