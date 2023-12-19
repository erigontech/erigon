package blocksync

import (
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
)

func statePointListFromCheckpoints(checkpoints []*checkpoint.Checkpoint) statePointList {
	spl := make(statePointList, len(checkpoints))
	for i, cp := range checkpoints {
		spl[i] = statePointFromCheckpoint(cp)
	}

	return spl
}

func statePointListFromMilestones(milestones []*milestone.Milestone) statePointList {
	spl := make(statePointList, len(milestones))
	for i, ms := range milestones {
		spl[i] = statePointFromMilestone(ms)
	}

	return spl
}

type statePointList []statePoint

func (spl statePointList) extractBatch(batchSize int, reverse bool) (batch statePointList, remaining statePointList) {
	if reverse {
		idx := len(spl) - batchSize
		batch = spl[idx:]
		remaining = spl[:idx]
	} else {
		batch = spl[:batchSize]
		remaining = spl[batchSize:]
	}

	return
}
