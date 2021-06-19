package stagedsync

import (
	"errors"
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/stretchr/testify/assert"
)

func TestStateStagesSuccess(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestStateDisabledStages(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return nil
			},
			Disabled: true,
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestStateRepeatedStage(t *testing.T) {
	repeatStageTwo := 2
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				repeatStageTwo--
				if repeatStageTwo < 0 {
					s.Done()
				}
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Bodies, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestStateErroredStage(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	expectedErr := errors.New("test error")
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.Equal(t, expectedErr, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestStateUnwindSomeStagesBehindUnwindPoint(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 1000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				if s.BlockNumber == 0 {
					if err := s.Update(tx, 1700); err != nil {
						return err
					}
				}
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					return u.UnwindTo(1500, tx, common.Hash{})
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
		{
			ID:       stages.IntermediateHashes,
			Disabled: true,
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(tx)
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2], s[3]}
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		// stages.Bodies is skipped because it is behind
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx)
	assert.NoError(t, err)
	assert.Equal(t, 1000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))
}

func TestStateUnwind(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					err := u.UnwindTo(500, tx, common.Hash{})
					if err != nil {
						return err
					}
					return s.DoneAndUpdate(tx, 3000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
		{
			ID:       stages.IntermediateHashes,
			Disabled: true,
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(tx)
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2], s[3]}
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

}

func TestStateUnwindEmptyUnwinder(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					err := u.UnwindTo(500, tx, common.Hash{})
					if err != nil {
						return err
					}
					return s.DoneAndUpdate(tx, 3000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(tx)
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx)
	assert.NoError(t, err)
	assert.Equal(t, 2000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func TestStateSyncDoTwice(t *testing.T) {
	flow := make([]stages.SyncStage, 0)

	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				return s.DoneAndUpdate(tx, s.BlockNumber+100)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				return s.DoneAndUpdate(tx, s.BlockNumber+200)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				return s.DoneAndUpdate(tx, s.BlockNumber+300)
			},
		},
	}

	state := NewState(s)
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.NoError(t, err)

	state = NewState(s)
	err = state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx)
	assert.NoError(t, err)
	assert.Equal(t, 200, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx)
	assert.NoError(t, err)
	assert.Equal(t, 400, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx)
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
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}

	state := NewState(s)
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.Equal(t, expectedErr, err)

	expectedErr = nil

	state = NewState(s)
	err = state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestStateSyncInterruptLongUnwind(t *testing.T) {
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
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(tx, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(tx)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder, tx ethdb.RwTx) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					err := u.UnwindTo(500, tx, common.Hash{})
					if err != nil {
						return err
					}
					return s.DoneAndUpdate(tx, 3000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState, tx ethdb.RwTx) error {
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
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	db, tx := kv.NewTestTx(t)
	err := state.Run(db, tx)
	assert.Error(t, errInterrupted, err)

	state = NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	err = state.LoadUnwindInfo(tx)
	assert.NoError(t, err)
	err = state.Run(db, tx)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), // interrupt here
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, tx)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func unwindOf(s stages.SyncStage) stages.SyncStage {
	return stages.SyncStage(append([]byte(s), 0xF0))
}
