package stagedsync

import (
	"errors"
	"testing"

	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
)

func TestStateStagesSuccess(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	db := ethdb.NewMemDatabase()
	defer db.Close()
	err := state.Run(db, db)
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
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return nil
			},
			Disabled: true,
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	db := ethdb.NewMemDatabase()
	defer db.Close()
	err := state.Run(db, db)
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
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
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
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	db := ethdb.NewMemDatabase()
	defer db.Close()
	err := state.Run(db, db)
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
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	db := ethdb.NewMemDatabase()
	defer db.Close()
	err := state.Run(db, db)
	assert.Equal(t, expectedErr, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestStateUnwindSomeStagesBehindUnwindPoint(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(db)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 1000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(db)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				if s.BlockNumber == 0 {
					if err := s.Update(db, 1700); err != nil {
						return err
					}
				}
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					return u.UnwindTo(1500, db)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(db)
			},
		},
		{
			ID:       stages.IntermediateHashes,
			Disabled: true,
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(db)
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2], s[3]}
	err := state.Run(db, db)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		// stages.Bodies is skipped because it is behind
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, db)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, db)
	assert.NoError(t, err)
	assert.Equal(t, 1000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, db)
	assert.NoError(t, err)
	assert.Equal(t, 1500, int(stageState.BlockNumber))
}

func TestStateUnwind(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(db)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(db)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					err := u.UnwindTo(500, db)
					if err != nil {
						return err
					}
					return s.DoneAndUpdate(db, 3000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(db)
			},
		},
		{
			ID:       stages.IntermediateHashes,
			Disabled: true,
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(db)
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2], s[3]}
	err := state.Run(db, db)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

}

func TestStateUnwindEmptyUnwinder(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	flow := make([]stages.SyncStage, 0)
	unwound := false
	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(db)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					err := u.UnwindTo(500, db)
					if err != nil {
						return err
					}
					return s.DoneAndUpdate(db, 3000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(db)
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	err := state.Run(db, db)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, db)
	assert.NoError(t, err)
	assert.Equal(t, 2000, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func TestStateSyncDoTwice(t *testing.T) {
	flow := make([]stages.SyncStage, 0)
	db := ethdb.NewMemDatabase()
	defer db.Close()

	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				return s.DoneAndUpdate(db, s.BlockNumber+100)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				return s.DoneAndUpdate(db, s.BlockNumber+200)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				return s.DoneAndUpdate(db, s.BlockNumber+300)
			},
		},
	}

	state := NewState(s)
	err := state.Run(db, db)
	assert.NoError(t, err)

	state = NewState(s)
	err = state.Run(db, db)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, db)
	assert.NoError(t, err)
	assert.Equal(t, 200, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, db)
	assert.NoError(t, err)
	assert.Equal(t, 400, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, db)
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
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				s.Done()
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				s.Done()
				return expectedErr
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				s.Done()
				return nil
			},
		},
	}
	db := ethdb.NewMemDatabase()
	defer db.Close()

	state := NewState(s)
	err := state.Run(db, db)
	assert.Equal(t, expectedErr, err)

	expectedErr = nil

	state = NewState(s)
	err = state.Run(db, db)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Headers, stages.Bodies, stages.Senders,
	}
	assert.Equal(t, expectedFlow, flow)
}

func TestStateSyncInterruptLongUnwind(t *testing.T) {
	// interrupt a stage that is too big to fit in one batch,
	// so the db is in inconsitent state, so we have to restart with that
	db := ethdb.NewMemDatabase()
	defer db.Close()
	flow := make([]stages.SyncStage, 0)
	unwound := false
	interrupted := false
	errInterrupted := errors.New("interrupted")

	s := []*Stage{
		{
			ID:          stages.Headers,
			Description: "Downloading headers",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(db)
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.DoneAndUpdate(db, 2000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(db)
			},
		},
		{
			ID:          stages.Senders,
			Description: "Recovering senders from tx signatures",
			ExecFunc: func(s *StageState, u Unwinder) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					err := u.UnwindTo(500, db)
					if err != nil {
						return err
					}
					return s.DoneAndUpdate(db, 3000)
				}
				s.Done()
				return nil
			},
			UnwindFunc: func(u *UnwindState, s *StageState) error {
				flow = append(flow, unwindOf(stages.Senders))
				if !interrupted {
					interrupted = true
					return errInterrupted
				}
				assert.Equal(t, 500, int(u.UnwindPoint))
				return u.Done(db)
			},
		},
	}
	state := NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	err := state.Run(db, db)
	assert.Error(t, errInterrupted, err)

	state = NewState(s)
	state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	err = state.LoadUnwindInfo(db)
	assert.NoError(t, err)
	err = state.Run(db, db)
	assert.NoError(t, err)

	expectedFlow := []stages.SyncStage{
		stages.Headers, stages.Bodies, stages.Senders,
		unwindOf(stages.Senders), // interrupt here
		unwindOf(stages.Senders), unwindOf(stages.Bodies), unwindOf(stages.Headers),
		stages.Headers, stages.Bodies, stages.Senders,
	}

	assert.Equal(t, expectedFlow, flow)

	stageState, err := state.StageState(stages.Headers, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Bodies, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))

	stageState, err = state.StageState(stages.Senders, db)
	assert.NoError(t, err)
	assert.Equal(t, 500, int(stageState.BlockNumber))
}

func unwindOf(s stages.SyncStage) stages.SyncStage {
	return append(s, 0xF0)
}
