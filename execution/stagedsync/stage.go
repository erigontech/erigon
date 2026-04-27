// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// ExecFunc is the execution function for the stage to move forward.
// * state - is the current state of the stage and contains stage data.
// * unwinder - if the stage needs to cause unwinding, `unwinder` methods can be used.
type ExecFunc func(badBlockUnwind bool, s *StageState, unwinder Unwinder, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, logger log.Logger) error

// UnwindFunc is the unwinding logic of the stage.
// * unwindState - contains information about the unwind itself.
// * stageState - represents the state of this stage at the beginning of unwind.
type UnwindFunc func(u *UnwindState, s *StageState, doms *execctx.SharedDomains, rwTx kv.TemporalRwTx, logger log.Logger) error

// Stage is a single sync stage in staged sync. As of Stage D the staged-sync
// pipeline only handles forward and unwind; prune work has moved to the
// storage component's bg loop (see Provider.RegisterPruneJob).
type Stage struct {
	// Description is a string that is shown in the logs.
	Description string
	// DisabledDescription shows in the log with a message if the stage is disabled. Here, you can show which command line flags should be provided to enable the page.
	DisabledDescription string
	// Forward is called when the stage is executed. The main logic of the stage should be here. Should always end with `s.Done()` to allow going to the next stage. MUST NOT be nil!
	Forward ExecFunc
	// Unwind is called when the stage should be unwound. The unwind logic should be there. MUST NOT be nil!
	Unwind UnwindFunc
	// ID of the sync stage. Should not be empty and should be unique. It is recommended to prefix it with reverse domain to avoid clashes (`com.example.my-stage`).
	ID stages.SyncStage
	// Disabled defines if the stage is disabled. It sets up when the stage is build by its `StageBuilder`.
	Disabled bool
}

type CurrentSyncCycleInfo struct {
	IsInitialCycle bool // means: not-on-chain-tip. can be several sync cycle in this mode.
	IsFirstCycle   bool // means: first cycle
}

// StageState is the state of the stage.
type StageState struct {
	State       *Sync
	ID          stages.SyncStage
	BlockNumber uint64 // BlockNumber is the current block number of the stage at the beginning of the state execution.

	CurrentSyncCycle CurrentSyncCycleInfo
}

func (s *StageState) LogPrefix() string {
	if s == nil {
		return ""
	}
	return s.State.LogPrefix()
}

func (s *StageState) SyncMode() stages.Mode {
	if s == nil {
		return stages.ModeUnknown
	}
	return s.State.mode
}

// Update updates the stage state (current block number) in the database. Can be called multiple times during stage execution.
func (s *StageState) Update(db kv.Putter, newBlockNum uint64) error {
	if err := stages.SaveStageProgress(db, s.ID, newBlockNum); err != nil {
		return err
	}
	s.BlockNumber = newBlockNum
	return nil
}

func (s *StageState) UpdatePrune(db kv.Putter, blockNum uint64) error {
	return stages.SaveStagePruneProgress(db, s.ID, blockNum)
}

// ExecutionAt gets the current state of the "Execution" stage, which block is currently executed.
func (s *StageState) ExecutionAt(db kv.Getter) (uint64, error) {
	execution, err := stages.GetStageProgress(db, stages.Execution)
	return execution, err
}

type UnwindReason struct {
	// If we're unwinding due to a fork - we want to unlink blocks but not mark
	// them as bad - as they may get replayed then deselected
	Block *common.Hash
	// If unwind is caused by a bad block, this error is not empty
	ErrBadBlock error
	// If unwind is caused by some operational error, this error is not empty
	ErrOperational error
}

func (u UnwindReason) IsBadBlock() bool {
	return u.ErrBadBlock != nil
}

func (u UnwindReason) Err() error {
	if u.ErrBadBlock != nil {
		return fmt.Errorf("bad block err: %w", u.ErrBadBlock)
	}
	if u.ErrOperational != nil {
		return fmt.Errorf("operational err: %w", u.ErrOperational)
	}
	return nil
}

var StagedUnwind = UnwindReason{}
var ExecUnwind = UnwindReason{}
var ForkChoice = UnwindReason{}

func BadBlock(badBlock common.Hash, err error) UnwindReason {
	if !errors.Is(err, rules.ErrInvalidBlock) {
		// make sure to always have ErrInvalidBlock in the error chain for bad block unwinding
		err = fmt.Errorf("%w: %w", rules.ErrInvalidBlock, err)
	}
	return UnwindReason{Block: &badBlock, ErrBadBlock: err}
}

func OperationalErr(err error) UnwindReason {
	return UnwindReason{ErrOperational: err}
}

// Unwinder allows the stage to cause an unwind.
type Unwinder interface {
	// UnwindTo begins staged sync unwind to the specified block.
	UnwindTo(unwindPoint uint64, reason UnwindReason, tx kv.Tx) error
	HasUnwindPoint() bool
	LogPrefix() string
}

// UnwindState contains the information about unwind.
type UnwindState struct {
	ID stages.SyncStage
	// UnwindPoint is the block to unwind to.
	UnwindPoint        uint64
	CurrentBlockNumber uint64
	Reason             UnwindReason
	state              *Sync

	CurrentSyncCycle CurrentSyncCycleInfo
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
	// state is the back-reference to the Sync that produced this PruneState
	// (used by LogPrefix). Stage D extracted prune from the Sync pipeline,
	// so prune jobs registered with the storage Provider construct
	// PruneState directly via NewStandalonePruneState — those have state==nil
	// and LogPrefix falls back to the stage ID.
	state *Sync

	CurrentSyncCycle CurrentSyncCycleInfo
}

// NewStandalonePruneState builds a PruneState that doesn't carry a Sync
// back-reference. Used by prune jobs registered with the storage component's
// bg loop (Provider.RegisterPruneJob) — they don't have a Sync handle but
// still need to call existing PruneXxx funcs that take *PruneState.
func NewStandalonePruneState(id stages.SyncStage, fwdProgress, pruneProgress uint64, isInitialCycle bool) *PruneState {
	return &PruneState{
		ID:               id,
		ForwardProgress:  fwdProgress,
		PruneProgress:    pruneProgress,
		CurrentSyncCycle: CurrentSyncCycleInfo{IsInitialCycle: isInitialCycle},
	}
}

func (s *PruneState) LogPrefix() string {
	if s.state == nil {
		return string(s.ID) + " Prune"
	}
	return s.state.LogPrefix() + " Prune"
}
func (s *PruneState) Done(db kv.Putter) error {
	return stages.SaveStagePruneProgress(db, s.ID, s.ForwardProgress)
}
func (s *PruneState) DoneAt(db kv.Putter, blockNum uint64) error {
	return stages.SaveStagePruneProgress(db, s.ID, blockNum)
}
