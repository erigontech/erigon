package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon/ethdb"
)

type StagedSync struct {
	stages      []*Stage
	unwindOrder UnwindOrder
	Notifier    ChainEventNotifier
}

func New(stages []*Stage, unwindOrder UnwindOrder) *StagedSync {
	return &StagedSync{
		stages:      stages,
		unwindOrder: unwindOrder,
	}
}

func (stagedSync *StagedSync) Prepare(db ethdb.RwKV, tx ethdb.Tx) (*State, error) {
	state := NewState(stagedSync.stages)

	state.unwindOrder = make([]*Stage, len(stagedSync.unwindOrder))

	for i, stageIndex := range stagedSync.unwindOrder {
		for _, s := range stagedSync.stages {
			if s.ID == stageIndex {
				state.unwindOrder[i] = s
				break
			}
		}
	}

	if tx != nil {
		if err := state.LoadUnwindInfo(tx); err != nil {
			return nil, err
		}
	} else {
		if err := db.View(context.Background(), func(tx ethdb.Tx) error {
			return state.LoadUnwindInfo(tx)
		}); err != nil {
			return nil, err
		}
	}
	return state, nil
}
