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
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

type (
	filesAheadTx   struct{ kv.TemporalTx }
	filesAheadView struct{ kv.FreezeInfo }
)

func (tx filesAheadTx) FreezeInfo() kv.FreezeInfo { return filesAheadView{tx.TemporalTx.FreezeInfo()} }
func (filesAheadView) ValueFilesEndTxNum() uint64 { return 100 }

func TestUnwindToReportsUnknownSnapshotBoundary(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	require.NoError(t, changeset.WriteDiffSet(tx, 1, common.Hash{1}, &changeset.StateChangeSet{}))
	sync := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, nil, nil, nil, log.New(), stages.ModeApplyingBlocks)
	err := sync.UnwindTo(1, ForkChoice, filesAheadTx{tx})
	require.ErrorIs(t, err, commitmentdb.ErrBehindCommitment)
	require.False(t, sync.HasUnwindPoint())
}

func TestNewKeepsMissingUnwindStagesAsNil(t *testing.T) {
	headerStage := &Stage{ID: stages.Headers}
	state := New(rawdbv3.TxNums,
		ethconfig.Defaults.Sync,
		[]*Stage{headerStage},
		UnwindOrder{stages.Bodies, stages.Headers},
		nil,
		log.New(),
		stages.ModeApplyingBlocks,
	)

	require.Equal(t, []*Stage{nil, headerStage}, state.unwindOrder)
}

func TestNewKeepsMissingPruneStagesAsNil(t *testing.T) {
	headerStage := &Stage{ID: stages.Headers}
	state := New(rawdbv3.TxNums,
		ethconfig.Defaults.Sync,
		[]*Stage{headerStage},
		nil,
		PruneOrder{stages.Bodies, stages.Headers},
		log.New(),
		stages.ModeApplyingBlocks,
	)

	require.Equal(t, []*Stage{nil, headerStage}, state.pruningOrder)
}

func TestStagesSuccess(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
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
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
			Disabled: true,
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
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
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestRunStartsAtFirstStageAfterPruneError(t *testing.T) {
	pruneErr := errors.New("prune failed")
	var flow []stages.SyncStage
	forward := func(_ bool, s *StageState, _ Unwinder, _ *execctx.SharedDomains, _ kv.TemporalRwTx, _ log.Logger) error {
		flow = append(flow, s.ID)
		return nil
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, []*Stage{
		{ID: stages.Headers, Forward: forward},
		{
			ID:      stages.Bodies,
			Forward: forward,
			Prune: func(context.Context, *PruneState, kv.RwTx, time.Duration, log.Logger) error {
				return pruneErr
			},
		},
	}, nil, PruneOrder{stages.Bodies}, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	require.ErrorIs(t, state.RunPrune(t.Context(), tx, false, nil, 0), pruneErr)

	_, err := state.Run(nil, tx, false, false)
	require.NoError(t, err)
	require.Equal(t, []stages.SyncStage{stages.Headers, stages.Bodies}, flow)
}

// Rolling back the transaction restores stage progress in the database. The caller
// reuses the same Sync, even after recovering a stage panic, so the next run must
// start at the first forward stage without replaying the failed unwind.
func TestFailedRunUnwindDoesNotLeakIntoNextRun(t *testing.T) {
	run := func(s *Sync, tx kv.TemporalRwTx) error {
		_, err := s.Run(nil, tx, false, false)
		return err
	}
	runNoInterrupt := func(s *Sync, tx kv.TemporalRwTx) error {
		_, err := s.RunNoInterrupt(nil, tx)
		return err
	}
	runUnwind := func(s *Sync, tx kv.TemporalRwTx) error { return s.RunUnwind(nil, tx) }
	for _, tc := range []struct {
		name        string
		mode        stages.Mode
		run, resume func(*Sync, kv.TemporalRwTx) error
		panics      bool
	}{
		{"RunUnwind/error", stages.ModeForkValidation, runUnwind, runNoInterrupt, false},
		{"RunUnwind/panic", stages.ModeForkValidation, runUnwind, runNoInterrupt, true},
		{"Run/error", stages.ModeApplyingBlocks, run, run, false},
		{"Run/panic", stages.ModeApplyingBlocks, run, run, true},
		{"RunNoInterrupt/error", stages.ModeApplyingBlocks, runNoInterrupt, run, false},
		{"RunNoInterrupt/panic", stages.ModeApplyingBlocks, runNoInterrupt, run, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			unwindErr := errors.New("unwind failed")
			var flow []stages.SyncStage
			forward := func(_ bool, s *StageState, _ Unwinder, _ *execctx.SharedDomains, _ kv.TemporalRwTx, _ log.Logger) error {
				flow = append(flow, s.ID)
				return nil
			}
			fail := func(*UnwindState, *StageState, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				if tc.panics {
					panic(unwindErr)
				}
				return unwindErr
			}
			state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, []*Stage{
				{ID: stages.Headers, Forward: forward},
				{ID: stages.Bodies, Forward: forward, Unwind: fail},
			}, UnwindOrder{stages.Bodies}, nil, log.New(), tc.mode)
			db, setup := temporaltest.NewTestTx(t)
			require.NoError(t, stages.SaveStageProgress(setup, stages.Bodies, 2))
			require.NoError(t, setup.Commit())

			require.NoError(t, state.UnwindTo(1, BadBlock(common.Hash{1}, errors.New("invalid block")), nil))
			attempt := func(tx kv.TemporalRwTx) error { return tc.run(state, tx) }
			if tc.panics {
				require.PanicsWithValue(t, unwindErr, func() { _ = db.UpdateTemporal(t.Context(), attempt) })
			} else {
				require.ErrorIs(t, db.UpdateTemporal(t.Context(), attempt), unwindErr)
			}
			require.False(t, state.HasUnwindPoint())
			require.Empty(t, state.UnwindReason())
			require.Nil(t, state.PrevUnwindPoint(), "a failed unwind must not be reported as completed")
			require.Zero(t, state.currentStage)

			require.NoError(t, db.UpdateTemporal(t.Context(), func(tx kv.TemporalRwTx) error {
				return tc.resume(state, tx)
			}))
			require.Equal(t, []stages.SyncStage{unwindOf(stages.Bodies), stages.Headers, stages.Bodies}, flow)
		})
	}
}

func TestUnwindSomeStagesBehindUnwindPoint(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 1000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				if s.BlockNumber == 0 {
					if err := s.Update(tx, 1700); err != nil {
						return err
					}
				}
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(1500, UnwindReason{}, tx)
					return nil
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		// stages.Bodies is skipped because it is behind
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 1000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, false, false)
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
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(500, UnwindReason{}, tx)
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	// check that at unwind disabled stage not appear
	flow = flow[:0]
	state.unwindOrder = []*Stage{s[2], s[1], s[0]}
	unwindPoint := uint64(100)
	require.NoError(t, state.UnwindTo(unwindPoint, UnwindReason{}, tx))
	_, err = state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)
	require.Equal(t, &unwindPoint, state.PrevUnwindPoint())

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
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(500, UnwindReason{}, tx)
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 2000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func TestSyncDoTwice(t *testing.T) {
	flow := make([]stages.SyncStage, 0)

	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return s.Update(tx, s.BlockNumber+100)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return s.Update(tx, s.BlockNumber+200)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return s.Update(tx, s.BlockNumber+300)
			},
		},
	}

	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)

	state = New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, err = state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 200, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 400, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, false, false)
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
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}

	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedErr = nil

	state = New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, err = state.Run(nil, tx, true /* initialCycle */, false)
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
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from txn signatures",
			Forward: func(badBlockUnwind bool, s *StageState, u Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					_ = u.UnwindTo(500, UnwindReason{}, tx)
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(u *UnwindState, s *StageState, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				if !interrupted {
					interrupted = true
					return errInterrupted
				}
				assert.Equal(t, 500, int(u.UnwindPoint))
				return u.Done(tx)
			},
		},
	}
	state := New(rawdbv3.TxNums, ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	assert.ErrorIs(t, err, errInterrupted)

	require.NoError(t, state.UnwindTo(500, StagedUnwind, nil))
	_, err = state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), // interrupt here
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, false, false)
	require.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func unwindOf(s stages.SyncStage) stages.SyncStage {
	return stages.SyncStage(append([]byte(s), 0xF0))
}
