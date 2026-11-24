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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestNewKeepsMissingUnwindStagesAsNil(t *testing.T) {
	headerStage := &Stage{ID: stages.Headers}
	state := New(
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
	state := New(
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
	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
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
	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
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
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
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
	state := New(ethconfig.Defaults.Sync, []*Stage{
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

// A reused Sync (fork validation) must not replay an unwind whose RunUnwind
// failed on an earlier, rolled-back transaction, whether the stage returned an
// error or panicked and StateStep recovered it.
func TestFailedRunUnwindDoesNotLeakIntoNextRun(t *testing.T) {
	unwindErr := errors.New("unwind failed")
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panics), func(t *testing.T) {
			var flow []stages.SyncStage
			stage := func(id stages.SyncStage, unwindFails bool) *Stage {
				return &Stage{
					ID: id,
					Forward: func(_ bool, s *StageState, _ Unwinder, _ *execctx.SharedDomains, _ kv.TemporalRwTx, _ log.Logger) error {
						flow = append(flow, s.ID)
						return nil
					},
					Unwind: func(*UnwindState, *StageState, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
						flow = append(flow, unwindOf(id))
						if unwindFails && panics {
							panic(unwindErr)
						}
						if unwindFails {
							return unwindErr
						}
						return nil
					},
				}
			}
			state := New(ethconfig.Defaults.Sync, []*Stage{stage(stages.Headers, false), stage(stages.Bodies, true)},
				UnwindOrder{stages.Bodies, stages.Headers}, nil, log.New(), stages.ModeForkValidation)

			_, tx := temporaltest.NewTestTx(t)
			for _, id := range []stages.SyncStage{stages.Headers, stages.Bodies} {
				require.NoError(t, stages.SaveStageProgress(tx, id, 2000))
			}
			require.NoError(t, state.UnwindTo(100, StagedUnwind, nil))
			runUnwind := func() (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						err = rec.(error)
					}
				}()
				return state.RunUnwind(nil, tx)
			}
			require.ErrorIs(t, runUnwind(), unwindErr)
			require.False(t, state.HasUnwindPoint())

			flow = nil
			_, tx2 := temporaltest.NewTestTx(t)
			for _, id := range []stages.SyncStage{stages.Headers, stages.Bodies} {
				require.NoError(t, stages.SaveStageProgress(tx2, id, 2000))
			}
			_, err := state.RunNoInterrupt(nil, tx2)
			require.NoError(t, err)
			require.Equal(t, []stages.SyncStage{stages.Headers, stages.Bodies}, flow)
		})
	}
}

// A reused fork-validation Sync must not replay an unwind that a forward stage requested
// inside RunNoInterrupt when a stage unwind then failed on the rolled-back tx.
func TestFailedNoInterruptUnwindDoesNotLeakIntoNextRun(t *testing.T) {
	unwindErr := errors.New("unwind failed")
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprintf("panic=%t", panics), func(t *testing.T) {
			var flow []stages.SyncStage
			requestUnwind := true
			stage := func(id stages.SyncStage, unwindFails bool) *Stage {
				return &Stage{
					ID: id,
					Forward: func(_ bool, s *StageState, u Unwinder, _ *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
						flow = append(flow, s.ID)
						if id == stages.Bodies && requestUnwind {
							requestUnwind = false
							return u.UnwindTo(100, StagedUnwind, tx)
						}
						return nil
					},
					Unwind: func(*UnwindState, *StageState, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
						flow = append(flow, unwindOf(id))
						if unwindFails && panics {
							panic(unwindErr)
						}
						if unwindFails {
							return unwindErr
						}
						return nil
					},
				}
			}
			state := New(ethconfig.Defaults.Sync, []*Stage{stage(stages.Headers, false), stage(stages.Bodies, true)},
				UnwindOrder{stages.Bodies, stages.Headers}, nil, log.New(), stages.ModeForkValidation)
			run := func(tx kv.TemporalRwTx) (err error) {
				defer func() {
					if rec := recover(); rec != nil {
						err = rec.(error)
					}
				}()
				_, err = state.RunNoInterrupt(nil, tx)
				return err
			}
			newTx := func() kv.TemporalRwTx {
				_, tx := temporaltest.NewTestTx(t)
				for _, id := range []stages.SyncStage{stages.Headers, stages.Bodies} {
					require.NoError(t, stages.SaveStageProgress(tx, id, 2000))
				}
				return tx
			}

			require.ErrorIs(t, run(newTx()), unwindErr)
			require.False(t, state.HasUnwindPoint())

			flow = nil
			require.NoError(t, run(newTx()))
			require.Equal(t, []stages.SyncStage{stages.Headers, stages.Bodies}, flow)
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
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
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
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
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
	_ = state.UnwindTo(100, UnwindReason{}, tx)
	_, err = state.Run(nil, tx, true /* initialCycle */, false)
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
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
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

	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	require.NoError(t, err)

	state = New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
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

	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedErr = nil

	state = New(ethconfig.Defaults.Sync, s, nil, nil, log.New(), stages.ModeApplyingBlocks)
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
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New(), stages.ModeApplyingBlocks)
	_, tx := temporaltest.NewTestTx(t)
	_, err := state.Run(nil, tx, true /* initialCycle */, false)
	assert.ErrorIs(t, err, errInterrupted)

	//state = NewState(s)
	//state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	//err = state.LoadUnwindInfo(tx)
	//require.NoError(t, err)
	//state.UnwindTo(500, common.Hash{})
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
