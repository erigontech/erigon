package diagnostics

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
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

func (ti SyncStage) Type() Type {
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
	Stage stages.SyncStage `json:"currentStage"`
}

func (ti CurrentSyncStage) Type() Type {
	return TypeOf(ti)
}

func (d *DiagnosticClient) setupStagesDiagnostics(rootCtx context.Context) {
	d.runSyncStagesListListener(rootCtx)
	d.runSyncStageListListener(rootCtx)
	d.runCurrentSyncStageListener(rootCtx)
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
				if err := d.db.Update(d.ctx, StagesListUpdater(info.StagesList)); err != nil {
					log.Error("[Diagnostics] Failed to update stages list", "err", err)
				}

				d.mu.Lock()
				d.syncStats.SyncStages = info.StagesList
				d.mu.Unlock()
			}
		}
	}()
}

func (d *DiagnosticClient) runSyncStageListListener(rootCtx context.Context) {
	go func() {
		ctx, ch, closeChannel := Context[SyncStage](rootCtx, 1)
		defer closeChannel()

		StartProviders(ctx, TypeOf(SyncStage{}), log.Root())
		for {
			select {
			case <-rootCtx.Done():
				return
			case info := <-ch:
				stages := addOrUpdateStagesList(d.syncStats.SyncStages, info)

				if err := d.db.Update(d.ctx, StagesListUpdater(stages)); err != nil {
					log.Error("[Diagnostics] Failed to update stages list", "err", err)
				}

				fmt.Println("Stages", stages)
				d.mu.Lock()
				d.syncStats.SyncStages = stages
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
				stages := addOrUpdateCurrentStage(d.syncStats.SyncStages, info)

				if err := d.db.Update(d.ctx, StagesListUpdater(stages)); err != nil {
					log.Error("[Diagnostics] Failed to update stages list", "err", err)
				}

				d.mu.Lock()
				d.syncStats.SyncStages = stages
				d.mu.Unlock()
			}
		}
	}()
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

func addOrUpdateStagesList(stages []SyncStage, stage SyncStage) []SyncStage {
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

func addOrUpdateCurrentStage(stages []SyncStage, css CurrentSyncStage) []SyncStage {
	resultArr := stages

	for idx, stage := range stages {
		if stage.ID == string(css.Stage) {
			stage.State = Running
			if idx > 0 {
				stages[idx-1].State = Completed
			}
		}
	}

	return resultArr
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
