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
	"fmt"
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

type SetSyncSubStageList struct {
	Stage string
	List  []SyncSubStage
}

func (ti SetSyncSubStageList) Type() Type {
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

func (d *DiagnosticClient) GetCurrentSyncIdxs() CurrentSyncStagesIdxs {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.getCurrentSyncIdxs()
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
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.syncStages) != len(stages) {
		d.syncStages = stages
	}
}

func (d *DiagnosticClient) SetSubStagesList(stageId string, subStages []SyncSubStage) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for idx, stage := range d.syncStages {
		if stage.ID == stageId {
			if len(d.syncStages[idx].SubStages) != len(subStages) {
				d.syncStages[idx].SubStages = subStages
				break
			}
		}
	}
}

func (d *DiagnosticClient) SetCurrentSyncStage(css CurrentSyncStage) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	stageState, err := d.GetStageState(css.Stage)
	if err != nil {
		return err
	}

	if stageState == Completed {
		return nil
	}

	isSet := false
	for idx, stage := range d.syncStages {
		if !isSet {
			if stage.ID == css.Stage {
				d.syncStages[idx].State = Running
				isSet = true
			} else {
				d.setStagesState(idx, Completed)
			}
		} else {
			d.setStagesState(idx, Queued)
		}
	}

	return nil
}

func (d *DiagnosticClient) setStagesState(stadeIdx int, state StageState) {
	d.syncStages[stadeIdx].State = state
	d.setSubStagesState(stadeIdx, state)
}

func (d *DiagnosticClient) setSubStagesState(stadeIdx int, state StageState) {
	for subIdx := range d.syncStages[stadeIdx].SubStages {
		d.syncStages[stadeIdx].SubStages[subIdx].State = state
	}
}

func (d *DiagnosticClient) SetCurrentSyncSubStage(css CurrentSyncSubStage) {
	d.mu.Lock()
	defer d.mu.Unlock()

	for idx, stage := range d.syncStages {
		if stage.State == Running {
			for subIdx, subStage := range stage.SubStages {
				if subStage.ID == css.SubStage {
					if d.syncStages[idx].SubStages[subIdx].State == Completed {
						return
					}

					if subIdx > 0 {
						d.syncStages[idx].SubStages[subIdx-1].State = Completed
					}

					d.syncStages[idx].SubStages[subIdx].State = Running
				}
			}

			break
		}
	}
}

// Deprecated - used only in tests. Non-thread-safe.
func (d *DiagnosticClient) GetStageState(stageId string) (StageState, error) {
	return d.getStageState(stageId)
}

func (d *DiagnosticClient) getStageState(stageId string) (StageState, error) {
	for _, stage := range d.syncStages {
		if stage.ID == stageId {
			return stage.State, nil
		}
	}

	stagesIdsList := make([]string, 0, len(d.syncStages))
	for _, stage := range d.syncStages {
		stagesIdsList = append(stagesIdsList, stage.ID)
	}

	return 0, fmt.Errorf("stage %s not found in stages list %s", stageId, stagesIdsList)
}

// Deprecated - not thread-safe method. Used only in tests. Need introduce more thread-safe method or something special for tests.
func (d *DiagnosticClient) GetSyncStages() []SyncStage {
	return d.syncStages
}
