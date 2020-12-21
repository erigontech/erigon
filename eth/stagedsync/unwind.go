package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// Unwinder allows the stage to cause an unwind.
type Unwinder interface {
	// UnwindTo begins staged sync unwind to the specified block.
	UnwindTo(uint64, ethdb.Database) error
}

// UnwindState contains the information about unwind.
type UnwindState struct {
	// Stage is the ID of the stage
	Stage stages.SyncStage
	// UnwindPoint is the block to unwind to.
	UnwindPoint uint64
}

// Done() updates the DB state of the stage.
func (u *UnwindState) Done(db ethdb.Putter) error {
	err := stages.SaveStageProgress(db, u.Stage, u.UnwindPoint)
	if err != nil {
		return err
	}
	return stages.SaveStageUnwind(db, u.Stage, 0)
}

// Skip() ignores the unwind
func (u *UnwindState) Skip(db ethdb.Putter) error {
	return stages.SaveStageUnwind(db, u.Stage, 0)
}

type PersistentUnwindStack struct {
	unwindStack []UnwindState
}

func NewPersistentUnwindStack() *PersistentUnwindStack {
	return &PersistentUnwindStack{make([]UnwindState, 0)}
}

func (s *PersistentUnwindStack) AddFromDB(db ethdb.Getter, stageID stages.SyncStage) error {
	u, err := s.LoadFromDB(db, stageID)
	if err != nil {
		return err
	}
	if u == nil {
		return nil
	}

	s.unwindStack = append(s.unwindStack, *u)
	return nil
}

func (s *PersistentUnwindStack) LoadFromDB(db ethdb.Getter, stageID stages.SyncStage) (*UnwindState, error) {
	unwindPoint, err := stages.GetStageUnwind(db, stageID)
	if err != nil {
		return nil, err
	}
	if unwindPoint > 0 {
		return &UnwindState{stageID, unwindPoint}, nil
	}
	return nil, nil
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
