package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type ExecFunc func(*StageState, Unwinder) error
type UnwindFunc func(*UnwindState, *StageState) error

type Stage struct {
	ID                  stages.SyncStage
	Description         string
	ExecFunc            ExecFunc
	Disabled            bool
	DisabledDescription string
	UnwindFunc          UnwindFunc
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
	s.state.NextStage()
}

func (s *StageState) ExecutionAt(db ethdb.Getter) (uint64, error) {
	return stages.GetStageProgress(db, stages.Execution)
}

func (s *StageState) DoneAndUpdate(db ethdb.Putter, newBlockNum uint64) error {
	err := stages.SaveStageProgress(db, s.Stage, newBlockNum)
	s.state.NextStage()
	return err
}
