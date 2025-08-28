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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

// ExecFunc is the execution function for the stage to move forward.
// * state - is the current state of the stage and contains stage data.
// * unwinder - if the stage needs to cause unwinding, `unwinder` methods can be used.
type ExecFunc func(badBlockUnwind bool, s *StageState, unwinder Unwinder, txc wrap.TxContainer, logger log.Logger) error

// UnwindFunc is the unwinding logic of the stage.
// * unwindState - contains information about the unwind itself.
// * stageState - represents the state of this stage at the beginning of unwind.
type UnwindFunc func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error

// PruneFunc is the execution function for the stage to prune old data.
// * state - is the current state of the stage and contains stage data.
type PruneFunc func(p *PruneState, tx kv.RwTx, logger log.Logger) error

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

type CurrentSyncCycleInfo struct {
	IsInitialCycle bool // means: not-on-chain-tip. can be several sync cycle in this mode.
	IsFirstCycle   bool // means: first cycle
}

// StageState is the state of the stage.
type StageState struct {
	state       *Sync
	ID          stages.SyncStage
	BlockNumber uint64 // BlockNumber is the current block number of the stage at the beginning of the state execution.

	CurrentSyncCycle CurrentSyncCycleInfo
}

func (s *StageState) LogPrefix() string {
	if s == nil {
		return ""
	}
	return s.state.LogPrefix()
}

func (s *StageState) SyncMode() stages.Mode {
	if s == nil {
		return stages.ModeUnknown
	}
	return s.state.mode
}

// Update updates the stage state (current block number) in the database. Can be called multiple times during stage execution.
func (s *StageState) Update(db kv.Putter, newBlockNum uint64) error {
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

type UnwindReason struct {
	// If we're unwinding due to a fork - we want to unlink blocks but not mark
	// them as bad - as they may get replayed then deselected
	Block *common.Hash
	// If unwind is caused by a bad block, this error is not empty
	Err error
}

func (u UnwindReason) IsBadBlock() bool {
	return u.Err != nil
}

var StagedUnwind = UnwindReason{nil, nil}
var ExecUnwind = UnwindReason{nil, nil}
var ForkChoice = UnwindReason{nil, nil}

func BadBlock(badBlock common.Hash, err error) UnwindReason {
	return UnwindReason{&badBlock, err}
}

func ForkReset(badBlock common.Hash) UnwindReason {
	return UnwindReason{&badBlock, nil}
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
	state           *Sync

	CurrentSyncCycle CurrentSyncCycleInfo
}

func (s *PruneState) LogPrefix() string { return s.state.LogPrefix() + " Prune" }
func (s *PruneState) Done(db kv.Putter) error {
	return stages.SaveStagePruneProgress(db, s.ID, s.ForwardProgress)
}
func (s *PruneState) DoneAt(db kv.Putter, blockNum uint64) error {
	return stages.SaveStagePruneProgress(db, s.ID, blockNum)
}
