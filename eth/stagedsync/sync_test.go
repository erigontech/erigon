//go:build notzkevm
// +build notzkevm

package stagedsync

import (
	"errors"
	"fmt"
	"testing"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/stretchr/testify/assert"
)

func TestStagesSuccess(t *testing.T) {
	flow := make([]SyncStage, 0)
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				return nil
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodies",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				return nil
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				return nil
			},
		},
	}
	state := New(s, nil, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Bodies, Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestDisabledStages(t *testing.T) {
	flow := make([]SyncStage, 0)
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				return nil
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				return nil
			},
			Disabled: true,
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				return nil
			},
		},
	}
	state := New(s, nil, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestErroredStage(t *testing.T) {
	flow := make([]SyncStage, 0)
	expectedErr := errors.New("test error")
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				return nil
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				return expectedErr
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				return nil
			},
		},
	}
	state := New(s, []SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedFlow := []SyncStage{
		Headers, Bodies,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestUnwindSomeStagesBehindUnwindPoint(t *testing.T) {
	flow := make([]SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 1000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				if s.BlockNumber == 0 {
					if err := s.Update(tx, 1700); err != nil {
						return err
					}
				}
				flow = append(flow, Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(1500, libcommon.Hash{})
					return nil
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Senders))
				return u.Done(tx)
			},
		},
		{
			ID:       IntermediateHashes,
			Disabled: true,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(IntermediateHashes))
				return u.Done(tx)
			},
		},
	}
	state := New(s, []SyncStage{s[3].ID, s[2].ID, s[1].ID, s[0].ID}, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Bodies, Senders,
		// Bodies is skipped because it is behind
		unwindOf(Senders), unwindOf(Headers),
		Headers, Bodies, Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))

	stageState, err = state.StageState(Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1000, int(stageState.BlockNumber))

	stageState, err = state.StageState(Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))
}

func TestUnwind(t *testing.T) {
	flow := make([]SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, libcommon.Hash{})
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Senders))
				return u.Done(tx)
			},
		},
		{
			ID:       IntermediateHashes,
			Disabled: true,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(IntermediateHashes))
				return u.Done(tx)
			},
		},
	}
	state := New(s, []SyncStage{s[3].ID, s[2].ID, s[1].ID, s[0].ID}, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Bodies, Senders,
		unwindOf(Senders), unwindOf(Bodies), unwindOf(Headers),
		Headers, Bodies, Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	//check that at unwind disabled stage not appear
	flow = flow[:0]
	state.unwindOrder = []*Stage{s[3], s[2], s[1], s[0]}
	state.UnwindTo(100, libcommon.Hash{})
	err = state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow = []SyncStage{
		unwindOf(Senders), unwindOf(Bodies), unwindOf(Headers),
		Headers, Bodies, Senders,
	}

	assert.Equal(t, expectedFlow, flow)

}

func TestUnwindEmptyUnwinder(t *testing.T) {
	flow := make([]SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, libcommon.Hash{})
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Senders))
				return u.Done(tx)
			},
		},
	}
	state := New(s, []SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Bodies, Senders,
		unwindOf(Senders), unwindOf(Headers),
		Headers, Bodies, Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2000, int(stageState.BlockNumber))

	stageState, err = state.StageState(Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func TestSyncDoTwice(t *testing.T) {
	flow := make([]SyncStage, 0)

	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				return s.Update(tx, s.BlockNumber+100)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				return s.Update(tx, s.BlockNumber+200)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				return s.Update(tx, s.BlockNumber+300)
			},
		},
	}

	state := New(s, nil, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	state = New(s, nil, nil)
	err = state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Bodies, Senders,
		Headers, Bodies, Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 200, int(stageState.BlockNumber))

	stageState, err = state.StageState(Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 400, int(stageState.BlockNumber))

	stageState, err = state.StageState(Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 600, int(stageState.BlockNumber))
}

func TestStateSyncInterruptRestart(t *testing.T) {
	flow := make([]SyncStage, 0)
	expectedErr := errors.New("interrupt")
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				return nil
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				return expectedErr
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				return nil
			},
		},
	}

	state := New(s, nil, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedErr = nil

	state = New(s, nil, nil)
	err = state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Bodies, Headers, Bodies, Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestSyncInterruptLongUnwind(t *testing.T) {
	// interrupt a stage that is too big to fit in one batch,
	// so the db is in inconsitent state, so we have to restart with that
	flow := make([]SyncStage, 0)
	unwound := false
	interrupted := false
	errInterrupted := errors.New("interrupted")

	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Headers)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Bodies)
				if s.BlockNumber == 0 {
					return s.Update(tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, tx kv.RwTx, quiet bool) error {
				flow = append(flow, Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, libcommon.Hash{})
					return s.Update(tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, tx kv.RwTx) error {
				flow = append(flow, unwindOf(Senders))
				if !interrupted {
					interrupted = true
					return errInterrupted
				}
				assert.Equal(t, 500, int(u.UnwindPoint))
				return u.Done(tx)
			},
		},
	}
	state := New(s, []SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil)
	db, tx := memdb.NewTestTx(t)
	err := state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.Error(t, errInterrupted, err)

	//state = NewState(s)
	//state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	//err = state.LoadUnwindInfo(tx)
	//assert.NoError(t, err)
	//state.UnwindTo(500, libcommon.Hash{})
	err = state.Run(db, tx, true /* initialCycle */, false /* quiet */)
	assert.NoError(t, err)

	expectedFlow := []SyncStage{
		Headers, Bodies, Senders,
		unwindOf(Senders), // interrupt here
		unwindOf(Senders), unwindOf(Bodies), unwindOf(Headers),
		Headers, Bodies, Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(Headers, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(Bodies, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(Senders, tx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func unwindOf(s SyncStage) SyncStage {
	return SyncStage(append([]byte(s), 0xF0))
}
