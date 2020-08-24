package stagedsync

import (
	"fmt"
	"runtime"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
)

type State struct {
	unwindStack  *PersistentUnwindStack
	stages       []*Stage
	unwindOrder  []*Stage
	currentStage uint

	beforeStageRun    map[stages.SyncStage]func() error
	onBeforeUnwind    func(stages.SyncStage) error
	beforeStageUnwind map[stages.SyncStage]func() error
}

func (s *State) Len() int {
	return len(s.stages)
}

func (s *State) NextStage() {
	if s == nil {
		return
	}
	s.currentStage++
}

func (s *State) GetLocalHeight(db ethdb.Getter) (uint64, error) {
	state, err := s.StageState(stages.Headers, db)
	return state.BlockNumber, err
}

func (s *State) UnwindTo(blockNumber uint64, db ethdb.Database) error {
	log.Info("UnwindTo", "block", blockNumber)
	for _, stage := range s.unwindOrder {
		fmt.Printf("Adding to unwind stack: %d, %s\n", stage.ID, stages.DBKeys[stage.ID])
		if stage.Disabled {
			continue
		}
		if err := s.unwindStack.Add(UnwindState{stage.ID, blockNumber, nil}, db); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) IsDone() bool {
	return s.currentStage >= uint(len(s.stages)) && s.unwindStack.Empty()
}

func (s *State) CurrentStage() (uint, *Stage) {
	return s.currentStage, s.stages[s.currentStage]
}

func (s *State) SetCurrentStage(id stages.SyncStage) error {
	for i, stage := range s.stages {
		if stage.ID == id {
			s.currentStage = uint(i)
			return nil
		}
	}
	return fmt.Errorf("stage not found with id: %v", id)
}

func (s *State) StageByID(id stages.SyncStage) (*Stage, error) {
	for _, stage := range s.stages {
		if stage.ID == id {
			return stage, nil
		}
	}
	return nil, fmt.Errorf("stage not found with id: %v", id)
}

func NewState(stagesList []*Stage) *State {
	return &State{
		stages:            stagesList,
		currentStage:      0,
		unwindStack:       NewPersistentUnwindStack(),
		beforeStageRun:    make(map[stages.SyncStage]func() error),
		beforeStageUnwind: make(map[stages.SyncStage]func() error),
	}
}

func (s *State) LoadUnwindInfo(db ethdb.Getter) error {
	for _, stage := range s.unwindOrder {
		if err := s.unwindStack.AddFromDB(db, stage.ID); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) StageState(stage stages.SyncStage, db ethdb.Getter) (*StageState, error) {
	blockNum, stageData, err := stages.GetStageProgress(db, stage)
	if err != nil {
		return nil, err
	}
	return &StageState{s, stage, blockNum, stageData}, nil
}

func (s *State) Run(db ethdb.GetterPutter, tx ethdb.GetterPutter) error {
	for !s.IsDone() {
		if !s.unwindStack.Empty() {
			for unwind := s.unwindStack.Pop(); unwind != nil; unwind = s.unwindStack.Pop() {
				if err := s.onBeforeUnwind(unwind.Stage); err != nil {
					return err
				}
				if hook, ok := s.beforeStageUnwind[unwind.Stage]; ok {
					if err := hook(); err != nil {
						return err
					}
				}
				if err := s.UnwindStage(unwind, db, tx); err != nil {
					return err
				}
			}
			if err := s.SetCurrentStage(0); err != nil {
				return err
			}
		}

		index, stage := s.CurrentStage()

		if hook, ok := s.beforeStageRun[stage.ID]; ok {
			if err := hook(); err != nil {
				return err
			}
		}

		if stage.Disabled {
			message := fmt.Sprintf(
				"Sync stage %d/%d. %v disabled. %s",
				index+1,
				s.Len(),
				stage.Description,
				stage.DisabledDescription,
			)

			log.Info(message)

			s.NextStage()
			continue
		}

		if err := s.runStage(stage, db, tx); err != nil {
			return err
		}
	}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	log.Info("Memory", "alloc", common.StorageSize(m.Alloc), "sys", common.StorageSize(m.Sys))
	return nil
}

func (s *State) runStage(stage *Stage, db ethdb.Getter, tx ethdb.Getter) error {
	if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		db = tx
	}
	stageState, err := s.StageState(stage.ID, db)
	if err != nil {
		return err
	}
	index, stage := s.CurrentStage()

	message := fmt.Sprintf("Sync stage %d/%d. %v...", index+1, s.Len(), stage.Description)
	log.Info(message)

	err = stage.ExecFunc(stageState, s)
	if err != nil {
		return err
	}

	log.Info(fmt.Sprintf("%s DONE!", message))
	return nil
}

func (s *State) UnwindStage(unwind *UnwindState, db ethdb.GetterPutter, tx ethdb.GetterPutter) error {
	if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
		db = tx
	}
	log.Info("Unwinding...", "stage", string(stages.DBKeys[unwind.Stage]))
	stage, err := s.StageByID(unwind.Stage)
	if err != nil {
		return err
	}
	if stage.UnwindFunc == nil {
		return nil
	}

	stageState, err := s.StageState(unwind.Stage, db)
	if err != nil {
		return err
	}

	if stageState.BlockNumber <= unwind.UnwindPoint {
		if err = unwind.Skip(db); err != nil {
			return err
		}
		return nil
	}

	err = stage.UnwindFunc(unwind, stageState)
	if err != nil {
		return err
	}

	if hook, ok := s.beforeStageRun[stage.ID]; ok {
		if err := hook(); err != nil {
			return err
		}
	}
	log.Info("Unwinding... DONE!")
	return nil
}

func (s *State) DisableStages(ids ...stages.SyncStage) {
	for i := range s.stages {
		for _, id := range ids {
			if s.stages[i].ID != id {
				continue
			}
			s.stages[i].Disabled = true
		}
	}
}

func (s *State) MockExecFunc(id stages.SyncStage, f ExecFunc) {
	for i := range s.stages {
		if s.stages[i].ID == id {
			s.stages[i].ExecFunc = f
		}
	}
}

func (s *State) BeforeStageRun(id stages.SyncStage, f func() error) {
	s.beforeStageRun[id] = f
}

func (s *State) BeforeStageUnwind(id stages.SyncStage, f func() error) {
	s.beforeStageUnwind[id] = f
}

func (s *State) OnBeforeUnwind(f func(id stages.SyncStage) error) {
	s.onBeforeUnwind = f
}
