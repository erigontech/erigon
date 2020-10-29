package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// ExecFunc is the execution function for the stage to move forward.
// * state - is the current state of the stage and contains stage data.
// * unwinder - if the stage needs to cause unwinding, `unwinder` methods can be used.
type ExecFunc func(state *StageState, unwinder Unwinder) error

// UnwindFunc is the unwinding logic of the stage.
// * unwindState - contains information about the unwind itself.
// * stageState - represents the state of this stage at the beginning of unwind.
type UnwindFunc func(unwindState *UnwindState, state *StageState) error

// Stage is a single sync stage in staged sync.
type Stage struct {
	// ID of the sync stage. Should not be empty and should be unique. It is recommended to prefix it with reverse domain to avoid clashes (`com.example.my-stage`).
	ID stages.SyncStage
	// Description is a string that is shown in the logs.
	Description string
	// Disabled defines if the stage is disabled. It sets up when the stage is build by its `StageBuilder`.
	Disabled bool
	// DisabledDescription shows in the log with a message if the stage is disabled. Here, you can show which command line flags should be provided to enable the page.
	DisabledDescription string
	// ExecFunc is called when the stage is executed. The main logic of the stage should be here. Should always end with `s.Done()` to allow going to the next stage. MUST NOT be nil!
	ExecFunc ExecFunc
	// UnwindFunc is called when the stage should be unwound. The unwind logic should be there. MUST NOT be nil!
	UnwindFunc UnwindFunc
}

// StageState is the state of the stage.
type StageState struct {
	state *State
	// Stage is the ID of this stage.
	Stage stages.SyncStage
	// BlockNumber is the current block number of the stage at the beginning of the state execution.
	BlockNumber uint64
	// StageData (optional) is the additional data for the stage execution at the beginning.
	StageData []byte
}

// Update updates the stage state (current block number) in the database. Can be called multiple times during stage execution.
func (s *StageState) Update(db ethdb.Putter, newBlockNum uint64) error {
	return stages.SaveStageProgress(db, s.Stage, newBlockNum, nil)
}

// UpdateWithStageData updates both the current block number for that stage, as well as some additional information as array of bytes: stageData.
func (s *StageState) UpdateWithStageData(db ethdb.Putter, newBlockNum uint64, stageData []byte) error {
	return stages.SaveStageProgress(db, s.Stage, newBlockNum, stageData)
}

// Done makes sure that the stage execution is complete and proceeds to the next state.
// If Done() is not called and the stage `ExecFunc` exits, then the same stage will be called again.
// This side effect is useful for something like block body download.
func (s *StageState) Done() {
	s.state.NextStage()
}

// ExecutionAt gets the current state of the "Execution" stage, which block is currently executed.
func (s *StageState) ExecutionAt(db ethdb.Getter) (uint64, error) {
	execution, _, err := stages.GetStageProgress(db, stages.Execution)
	return execution, err
}

// DoneAndUpdate a convenience method combining both `Done()` and `Update()` calls together.
func (s *StageState) DoneAndUpdate(db ethdb.Putter, newBlockNum uint64) error {
	err := stages.SaveStageProgress(db, s.Stage, newBlockNum, nil)
	s.state.NextStage()
	return err
}
