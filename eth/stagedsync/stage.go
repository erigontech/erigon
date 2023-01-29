package stagedsync

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

// ExecFunc is the execution function for the stage to move forward.
// * state - is the current state of the stage and contains stage data.
// * unwinder - if the stage needs to cause unwinding, `unwinder` methods can be used.
type ExecFunc func(firstCycle bool, badBlockUnwind bool, s *StageState, unwinder Unwinder, tx kv.RwTx, quiet bool) error

// UnwindFunc is the unwinding logic of the stage.
// * unwindState - contains information about the unwind itself.
// * stageState - represents the state of this stage at the beginning of unwind.
type UnwindFunc func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error

// PruneFunc is the execution function for the stage to prune old data.
// * state - is the current state of the stage and contains stage data.
type PruneFunc func(firstCycle bool, p *PruneState, tx kv.RwTx) error

// Stage is a single sync stage in staged sync.
type Stage struct {
	// Description is a string that is shown in the logs.
	Description string
	// DisabledDescription shows in the log with a message if the stage is disabled. Here, you can show which command line flags should be provided to enable the page.
	DisabledDescription string
	// Forward is called when the stage is executed. The main logic of the stage should be here. Should always end with `s.Done()` to allow going to the next stage. MUST NOT be nil!
	Forward ExecFunc
	// Unwind is called when the stage should be unwound. The unwind logic should be there. MUST NOT be nil!
	Unwind UnwindFunc
	Prune  PruneFunc
	// ID of the sync stage. Should not be empty and should be unique. It is recommended to prefix it with reverse domain to avoid clashes (`com.example.my-stage`).
	ID stages.SyncStage
	// Disabled defines if the stage is disabled. It sets up when the stage is build by its `StageBuilder`.
	Disabled bool
}

// StageState is the state of the stage.
type StageState struct {
	state       *Sync
	ID          stages.SyncStage
	BlockNumber uint64 // BlockNumber is the current block number of the stage at the beginning of the state execution.
}

func (s *StageState) LogPrefix() string { return s.state.LogPrefix() }

// Update updates the stage state (current block number) in the database. Can be called multiple times during stage execution.
func (s *StageState) Update(db kv.Putter, newBlockNum uint64) error {
	if m, ok := syncMetrics[s.ID]; ok {
		m.Set(newBlockNum)
	}
	return stages.SaveStageProgress(db, s.ID, newBlockNum)
}
func (s *StageState) UpdatePrune(db kv.Putter, blockNum uint64) error {
	return stages.SaveStagePruneProgress(db, s.ID, blockNum)
}

// ExecutionAt gets the current state of the "Execution" stage, which block is currently executed.
func (s *StageState) ExecutionAt(db kv.Getter) (uint64, error) {
	execution, err := stages.GetStageProgress(db, stages.Execution)
	return execution, err
}

// Unwinder allows the stage to cause an unwind.
type Unwinder interface {
	// UnwindTo begins staged sync unwind to the specified block.
	UnwindTo(unwindPoint uint64, badBlock libcommon.Hash)
}

// UnwindState contains the information about unwind.
type UnwindState struct {
	ID stages.SyncStage
	// UnwindPoint is the block to unwind to.
	UnwindPoint        uint64
	CurrentBlockNumber uint64
	// If unwind is caused by a bad block, this hash is not empty
	BadBlock libcommon.Hash
	state    *Sync
}

func (u *UnwindState) LogPrefix() string { return u.state.LogPrefix() }

// Done updates the DB state of the stage.
func (u *UnwindState) Done(db kv.Putter) error {
	return stages.SaveStageProgress(db, u.ID, u.UnwindPoint)
}

type PruneState struct {
	ID              stages.SyncStage
	ForwardProgress uint64 // progress of stage forward move
	PruneProgress   uint64 // progress of stage prune move. after sync cycle it become equal to ForwardProgress by Done() method
	state           *Sync
}

func (s *PruneState) LogPrefix() string { return s.state.LogPrefix() + " Prune" }
func (s *PruneState) Done(db kv.Putter) error {
	return stages.SaveStagePruneProgress(db, s.ID, s.ForwardProgress)
}
func (s *PruneState) DoneAt(db kv.Putter, blockNum uint64) error {
	return stages.SaveStagePruneProgress(db, s.ID, blockNum)
}
