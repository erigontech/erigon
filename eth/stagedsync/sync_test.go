package stagedsync

import (
	"errors"
	"fmt"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func TestStagesSuccess(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
			Disabled: true,
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 1000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				if s.BlockNumber == 0 {
					if err := s.Update(tx, 1700); err != nil {
						return err
					}
				}
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(1500, libcommon.Hash{})
					return nil
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
		{
			ID:       stages.IntermediateHashes,
			Disabled: true,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(tx)
			},
		},
	}
	state := New(s, []stages.SyncStage{s[3].ID, s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		// stages.Bodies is skipped because it is behind
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))
}

func TestUnwind(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, libcommon.Hash{})
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
		{
			ID:       stages.IntermediateHashes,
			Disabled: true,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(tx)
			},
		},
	}
	state := New(s, []stages.SyncStage{s[3].ID, s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	//check that at unwind disabled stage not appear
	flow = flow[:0]
	state.unwindOrder = []*Stage{s[3], s[2], s[1], s[0]}
	state.UnwindTo(100, libcommon.Hash{})
	err = state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, libcommon.Hash{})
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
	}
	state := New(s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func TestSyncDoTwice(t *testing.T) {
	flow := make([]stages.SyncStage, 0)

	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return s.Update(tx, s.BlockNumber+100)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return s.Update(tx, s.BlockNumber+200)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return s.Update(tx, s.BlockNumber+300)
			},
		},
	}

	state := New(s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

	state = New(s, nil, nil, log.New())
	err = state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 200, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 400, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 600, int(stageState.BlockNumber))
}

func TestStateSyncInterruptRestart(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	expectedErr := errors.New("interrupt")
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}

	state := New(s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedErr = nil

	state = New(s, nil, nil, log.New())
	err = state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, libcommon.Hash{})
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx, logger log.Logger) error {
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
	state := New(s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */)
	assert.Error(t, errInterrupted, err)

	//state = NewState(s)
	//state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	//err = state.LoadUnwindInfo(tx)
	//assert.NoError(t, err)
	//state.UnwindTo(500, libcommon.Hash{})
	err = state.Run(db, tx, true /* initialCycle */)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), // interrupt here
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func unwindOf(s stages.SyncStage) stages.SyncStage {
	return stages.SyncStage(append([]byte(s), 0xF0))
}
