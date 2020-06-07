package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type Unwinder interface {
	UnwindTo(uint64, ethdb.Database) error
}

type UnwindState struct {
	Stage       stages.SyncStage
	UnwindPoint uint64
}

func (u *UnwindState) Done(db ethdb.Putter) error {
	err := stages.SaveStageProgress(db, u.Stage, u.UnwindPoint)
	if err != nil {
		return err
	}
	return stages.SaveStageUnwind(db, u.Stage, 0)
}

func (u *UnwindState) Skip(db ethdb.Putter) error {
	return stages.SaveStageUnwind(db, u.Stage, 0)
}

type PersistentUnwindStack struct {
	unwindStack []UnwindState
}

func NewPersistentUnwindStack() *PersistentUnwindStack {
	return &PersistentUnwindStack{make([]UnwindState, 0)}
}

func (s *PersistentUnwindStack) LoadFromDb(db ethdb.Getter, stageID stages.SyncStage) error {
	unwindPoint, err := stages.GetStageUnwind(db, stageID)
	if err != nil {
		return err
	}
	if unwindPoint > 0 {
		u := UnwindState{stageID, unwindPoint}
		s.unwindStack = append(s.unwindStack, u)
	}
	return nil
}

func (s *PersistentUnwindStack) Empty() bool {
	return len(s.unwindStack) == 0
}

func (s *PersistentUnwindStack) Add(u UnwindState, db ethdb.GetterPutter) error {
	currentPoint, err := stages.GetStageUnwind(db, u.Stage)
	if err != nil {
		return err
	}
	if currentPoint > 0 && u.UnwindPoint >= currentPoint {
		return nil
	}
	s.unwindStack = append(s.unwindStack, u)
	return stages.SaveStageUnwind(db, u.Stage, u.UnwindPoint)
}

func (s *PersistentUnwindStack) Pop() *UnwindState {
	if len(s.unwindStack) == 0 {
		return nil
	}
	unwind := s.unwindStack[len(s.unwindStack)-1]
	s.unwindStack = s.unwindStack[:len(s.unwindStack)-1]
	return &unwind
}
