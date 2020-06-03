package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type State struct {
	stages       []*Stage
	currentStage uint
}

func (s *State) Len() int {
	return len(s.stages)
}

func (s *State) NextStage() {
	s.currentStage++
}

func (s *State) IsDone() bool {
	return s.currentStage >= uint(len(s.stages))
}

func (s *State) CurrentStage() (uint, *Stage) {
	return s.currentStage, s.stages[s.currentStage]
}

func NewState(stages []*Stage) *State {
	return &State{
		stages:       stages,
		currentStage: 0,
	}
}

func (s *State) StageState(stage stages.SyncStage, db ethdb.Getter) (*StageState, error) {
	blockNum, err := stages.GetStageProgress(db, stage)
	if err != nil {
		return nil, err
	}
	return &StageState{s, stage, blockNum}, nil
}

type StageState struct {
	state       *State
	Stage       stages.SyncStage
	BlockNumber uint64
}

func (s *StageState) Update(db ethdb.Putter, newBlockNum uint64) error {
	return stages.SaveStageProgress(db, s.Stage, newBlockNum)
}

func (s *StageState) Done() {
	if s.state != nil {
		s.state.NextStage()
	}
}

func (s *StageState) ExecutionAt(db ethdb.Getter) (uint64, error) {
	return stages.GetStageProgress(db, stages.Execution)
}

func (s *StageState) DoneAndUpdate(db ethdb.Putter, newBlockNum uint64) error {
	err := stages.SaveStageProgress(db, s.Stage, newBlockNum)
	if s.state != nil {
		s.state.NextStage()
	}
	return err
}
