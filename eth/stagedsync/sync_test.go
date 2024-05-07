//go:build notzkevm
// +build notzkevm

package stagedsync

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
)

func TestStagesSuccess(t *testing.T) {
	flow := make([]SyncStage, 0)
	s := []*Stage{
		{
			ID:          Headers,
			Description: "Downloading headers",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          stages.Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return nil
			},
			Disabled: true,
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 1000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				if s.BlockNumber == 0 {
					if err := s.Update(txc.Tx, 1700); err != nil {
						return err
					}
				}
				flow = append(flow, Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(1500, UnwindReason{})
					return nil
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:       IntermediateHashes,
			Disabled: true,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(txc.Tx)
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[3].ID, s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, UnwindReason{})
					return s.Update(txc.Tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:       IntermediateHashes,
			Disabled: true,
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.IntermediateHashes)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.IntermediateHashes))
				return u.Done(txc.Tx)
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[3].ID, s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
	state.UnwindTo(100, UnwindReason{})
	_, err = state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, UnwindReason{})
					return s.Update(txc.Tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Senders))
				return u.Done(txc.Tx)
			},
		},
	}
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return s.Update(txc.Tx, s.BlockNumber+100)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return s.Update(txc.Tx, s.BlockNumber+200)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return s.Update(txc.Tx, s.BlockNumber+300)
			},
		},
	}

	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
	assert.NoError(t, err)

	state = New(ethconfig.Defaults.Sync, s, nil, nil, log.New())
	_, err = state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				return nil
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				return expectedErr
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				return nil
			},
		},
	}

	state := New(ethconfig.Defaults.Sync, s, nil, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
	assert.Equal(t, fmt.Errorf("[2/3 Bodies] %w", expectedErr), err)

	expectedErr = nil

	state = New(ethconfig.Defaults.Sync, s, nil, nil, log.New())
	_, err = state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Headers)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Headers))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          Bodies,
			Description: "Downloading block bodiess",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Bodies)
				if s.BlockNumber == 0 {
					return s.Update(txc.Tx, 2000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, unwindOf(stages.Bodies))
				return u.Done(txc.Tx)
			},
		},
		{
			ID:          Senders,
			Description: "Recovering senders from tx signatures",
			Forward: func(firstCycle bool, badBlockUnwind bool, s *StageState, u Unwinder, txc wrap.TxContainer, logger log.Logger) error {
				flow = append(flow, stages.Senders)
				if !unwound {
					unwound = true
					u.UnwindTo(500, UnwindReason{})
					return s.Update(txc.Tx, 3000)
				}
				return nil
			},
			Unwind: func(firstCycle bool, u *UnwindState, s *StageState, txc wrap.TxContainer, logger log.Logger) error {
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
	state := New(ethconfig.Defaults.Sync, s, []stages.SyncStage{s[2].ID, s[1].ID, s[0].ID}, nil, log.New())
	db, tx := memdb.NewTestTx(t)
	_, err := state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
	assert.Error(t, errInterrupted, err)

	//state = NewState(s)
	//state.unwindOrder = []*Stage{s[0], s[1], s[2]}
	//err = state.LoadUnwindInfo(tx)
	//assert.NoError(t, err)
	//state.UnwindTo(500, libcommon.Hash{})
	_, err = state.Run(db, wrap.TxContainer{Tx: tx}, true /* initialCycle */)
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
