package stagedsync

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
)

// Unwinder allows the stage to cause an unwind.
type Unwinder interface {
	// UnwindTo begins staged sync unwind to the specified block.
	UnwindTo(unwindPoint uint64, tx ethdb.RwTx, badBlock common.Hash) error
}

// UnwindState contains the information about unwind.
type UnwindState struct {
	ID stages.SyncStage
	// UnwindPoint is the block to unwind to.
	UnwindPoint uint64
	// If unwind is caused by a bad block, this hash is not empty
	BadBlock common.Hash
	state    *State
}

func (u *UnwindState) LogPrefix() string { return u.state.LogPrefix() }

// Done updates the DB state of the stage.
func (u *UnwindState) Done(db ethdb.Putter) error {
	err := stages.SaveStageProgress(db, u.ID, u.UnwindPoint)
	if err != nil {
		return err
	}
	return stages.SaveStageUnwind(db, u.ID, 0)
}

// Skip ignores the unwind
func (u *UnwindState) Skip(db ethdb.Putter) error {
	return stages.SaveStageUnwind(db, u.ID, 0)
}

type PersistentUnwindStack struct {
	unwindStack []UnwindState
	state       *State
}

func NewPersistentUnwindStack(state *State) *PersistentUnwindStack {
	return &PersistentUnwindStack{make([]UnwindState, 0), state}
}

func (s *PersistentUnwindStack) AddFromDB(db ethdb.KVGetter, stageID stages.SyncStage) error {
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

func (s *PersistentUnwindStack) LoadFromDB(db ethdb.KVGetter, stageID stages.SyncStage) (*UnwindState, error) {
	unwindPoint, err := stages.GetStageUnwind(db, stageID)
	if err != nil {
		return nil, err
	}
	if unwindPoint > 0 {
		return &UnwindState{stageID, unwindPoint, common.Hash{}, s.state}, nil
	}
	return nil, nil
}

func (s *PersistentUnwindStack) Empty() bool {
	return len(s.unwindStack) == 0
}

func (s *PersistentUnwindStack) Add(u UnwindState, tx ethdb.RwTx) error {
	currentPoint, err := stages.GetStageUnwind(tx, u.ID)
	if err != nil {
		return err
	}
	if currentPoint > 0 && u.UnwindPoint >= currentPoint {
		return nil
	}
	s.unwindStack = append(s.unwindStack, u)
	return stages.SaveStageUnwind(tx, u.ID, u.UnwindPoint)
}

func (s *PersistentUnwindStack) Pop() *UnwindState {
	if len(s.unwindStack) == 0 {
		return nil
	}
	unwind := s.unwindStack[len(s.unwindStack)-1]
	s.unwindStack = s.unwindStack[:len(s.unwindStack)-1]
	return &unwind
}

// PruneState contains the information about unwind.
type PruneState struct {
	ID         stages.SyncStage
	PrunePoint uint64 // PrunePoint is the block to prune to.
	state      *State
}

func (u *PruneState) LogPrefix() string { return u.state.LogPrefix() }

// Done updates the DB state of the stage.
func (u *PruneState) Done(db ethdb.Putter) error {
	return stages.SaveStagePrune(db, u.ID, 0)
}

// Skip ignores the prune
func (u *PruneState) Skip(db ethdb.Putter) error {
	return stages.SaveStagePrune(db, u.ID, 0)
}
