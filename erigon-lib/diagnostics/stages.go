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
	ID        string         `json:"stage"`
	State     StageState     `json:"state"`
	SubStages []SyncSubStage `json:"subStages"`
}

type SyncSubStage struct {
	ID    string     `json:"subStage"`
	State StageState `json:"state"`
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
	if err := d.db.Update(d.ctx, StagesListUpdater(d.syncStats.SyncStages)); err != nil {
		log.Error("[Diagnostics] Failed to update stages list", "err", err)
	}
}

func (d *DiagnosticClient) getCurrentSyncIdxs() CurrentSyncStagesIdxs {
	currentIdxs := CurrentSyncStagesIdxs{
		Stage:    -1,
		SubStage: -1,
	}

	for sIdx, stage := range d.syncStats.SyncStages {
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
	d.syncStats.SyncStages = stages
}

func (d *DiagnosticClient) AddOrUpdateStagesList(stages []SyncStage, stage SyncStage) []SyncStage {
	resultArr := stages
	stageExists := false

	for _, st := range resultArr {
		if st.ID == stage.ID {
			st.State = stage.State
			st.SubStages = stage.SubStages
			stageExists = true
			break
		}
	}

	if !stageExists {
		resultArr = append(resultArr, stage)
	}

	return resultArr
}

func (d *DiagnosticClient) SetCurrentSyncStage(css CurrentSyncStage) {
	isSet := false
	for idx, stage := range d.syncStats.SyncStages {
		if !isSet {
			if stage.ID == css.Stage {
				d.syncStats.SyncStages[idx].State = Running
				isSet = true
			} else {
				d.syncStats.SyncStages[idx].State = Completed
				for subIdx := range d.syncStats.SyncStages[idx].SubStages {
					d.syncStats.SyncStages[idx].SubStages[subIdx].State = Completed
				}
			}
		} else {
			d.syncStats.SyncStages[idx].State = Queued
		}
	}
}

func (d *DiagnosticClient) SetCurrentSyncSubStage(css CurrentSyncSubStage) {
	for idx, stage := range d.syncStats.SyncStages {
		if stage.State == Running {
			isSet := false
			for subIdx, subStage := range stage.SubStages {
				if !isSet {
					if subStage.ID == css.SubStage {
						d.syncStats.SyncStages[idx].SubStages[subIdx].State = Running
						isSet = true
					} else {
						d.syncStats.SyncStages[idx].SubStages[subIdx].State = Completed
					}
				} else {
					d.syncStats.SyncStages[idx].SubStages[subIdx].State = Queued
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
	for idx, stage := range d.syncStats.SyncStages {
		if stage.ID == subStage.StageId {
			subStageExist := false
			for subIdx, sub := range stage.SubStages {
				if sub.ID == subStage.SubStage.ID {
					subStageExist = true
					d.syncStats.SyncStages[idx].SubStages[subIdx] = subStage.SubStage
					break
				}
			}

			if !subStageExist {
				d.syncStats.SyncStages[idx].SubStages = append(d.syncStats.SyncStages[idx].SubStages, subStage.SubStage)
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
