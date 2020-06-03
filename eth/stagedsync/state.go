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

func (s *State) CurrentStage() *Stage {
	return s.stages[s.currentStage]
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

func (s *StageState) Done(db ethdb.Putter, newBlockNum uint64) error {
	err := stages.SaveStageProgress(db, s.Stage, newBlockNum)
	s.state.NextStage()
	return err
}
