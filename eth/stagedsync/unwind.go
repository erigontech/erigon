package stagedsync

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
)

// Unwinder allows the stage to cause an unwind.
type Unwinder interface {
	// UnwindTo begins staged sync unwind to the specified block.
	UnwindTo(unwindPoint uint64, badBlock common.Hash)
}

// UnwindState contains the information about unwind.
type UnwindState struct {
	ID stages.SyncStage
	// UnwindPoint is the block to unwind to.
	UnwindPoint        uint64
	CurrentBlockNumber uint64
	// If unwind is caused by a bad block, this hash is not empty
	BadBlock common.Hash
	state    *State
}

func (u *UnwindState) LogPrefix() string { return u.state.LogPrefix() }

// Done updates the DB state of the stage.
func (u *UnwindState) Done(db ethdb.Putter) error {
	return stages.SaveStageProgress(db, u.ID, u.UnwindPoint)
}

// Skip ignores the unwind
func (u *UnwindState) Skip() {}

type PruneState struct {
	ID         stages.SyncStage
	PrunePoint uint64 // PrunePoint is the block to prune to.
	state      *State
}

func (u *PruneState) LogPrefix() string { return u.state.LogPrefix() }
