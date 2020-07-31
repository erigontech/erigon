package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type ExecFunc func(*StageState, Unwinder) error
type UnwindFunc func(*UnwindState, *StageState) error

type Stage struct {
	ID                  stages.SyncStage
	Disabled            bool
	Description         string
	ExecFunc            ExecFunc
	DisabledDescription string
	UnwindFunc          UnwindFunc
}

type StageState struct {
	state       *State
	Stage       stages.SyncStage
	BlockNumber uint64
	StageData   []byte
}

func (s *StageState) Update(db ethdb.Putter, newBlockNum uint64) error {
	return stages.SaveStageProgress(db, s.Stage, newBlockNum, nil)
}

func (s *StageState) UpdateWithStageData(db ethdb.Putter, newBlockNum uint64, stageData []byte) error {
	return stages.SaveStageProgress(db, s.Stage, newBlockNum, stageData)
}

func (s *StageState) Done() {
	s.state.NextStage()
}

func (s *StageState) ExecutionAt(db ethdb.Getter) (uint64, error) {
	execution, _, err := stages.GetStageProgress(db, stages.Execution)
	return execution, err
}

func (s *StageState) DoneAndUpdate(db ethdb.Putter, newBlockNum uint64) error {
	err := stages.SaveStageProgress(db, s.Stage, newBlockNum, nil)
	s.state.NextStage()
	return err
}
