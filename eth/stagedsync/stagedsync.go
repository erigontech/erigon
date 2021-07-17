package stagedsync

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

func (stagedSync *StagedSync) Prepare() (*State, error) {
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

	return state, nil
}
