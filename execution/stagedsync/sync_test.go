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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func TestStagesSuccess(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestDisabledStages(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
			Disabled: true,
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestErroredStage(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	expectedErr := errors.New("test error")
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestUnwindSomeStagesBehindUnwindPoint(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 1000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if s.BlockNumber == 0 {
					if err := s.Update(txc.Tx, 1700); err != nil {
						return err
					}
				}
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(1500, UnwindReason{}, txc.Tx)
					return nil
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(txc.Tx)
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		// stages.Bodies is skipped because it is behind
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 1000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))
}

func TestUnwind(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(500, UnwindReason{}, txc.Tx)
					return s.Update(txc.Tx, 3000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(txc.Tx)
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	//check that at unwind disabled stage not appear
	flow = flow[:0]
	state.unwindOrder = []*Stage{s[2], s[1], s[0]}
	_ = state.UnwindTo(100, UnwindReason{}, tx)
	_, err = state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow = []stages.SyncStage{
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

}

func TestUnwindEmptyUnwinder(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(500, UnwindReason{}, txc.Tx)
					return s.Update(txc.Tx, 3000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(txc.Tx)
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 2000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func TestSyncDoTwice(t *testing.T) {
	flow := make([]stages.SyncStage, 0)

	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return s.Update(txc.Tx, s.BlockNumber+100)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return s.Update(txc.Tx, s.BlockNumber+200)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return s.Update(txc.Tx, s.BlockNumber+300)
			},
		},
	}

	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	state = New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, err = state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 200, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 400, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 600, int(stageState.BlockNumber))
}

func TestStateSyncInterruptRestart(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	expectedErr := errors.New("interrupt")
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}

	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedErr = nil

	state = New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, err = state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestSyncInterruptLongUnwind(t *testing.T) {
	// interrupt a stage that is too big to fit in one batch,
	// so the db is in inconsitent state, so we have to restart with that
	flow := make([]stages.SyncStage, 0)
	unwound := false
	interrupted := false
	errInterrupted := errors.New("interrupted")

	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(500, UnwindReason{}, txc.Tx)
					return s.Update(txc.Tx, 3000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				if !interrupted {
					interrupted = true
					return errInterrupted
				}
				assert.Equal(t, 500, int(u.UnwindPoint))
				return u.Done(txc.Tx)
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	assert.Error(t, errInterrupted, err)

	//state = NewState(s)
	//state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	//err = state.LoadUnwindInfo(tx)
	//require.NoError(t, err)
	//state.UnwindTo(500, common.Hash{})
	_, err = state.Run(db, wrap.NewTxContainer(tx, nil), true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), // interrupt here
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func unwindOf(s stages.SyncStage) stages.SyncStage {
	return stages.SyncStage(append([]byte(s), 0xF0))
}
