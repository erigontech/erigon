package sync

import (
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
)

func statePointsFromCheckpoints(checkpoints []*checkpoint.Checkpoint) statePoints {
	statePoints := make(statePoints, len(checkpoints))
	for i, checkpoint := range checkpoints {
		statePoints[i] = statePointFromCheckpoint(checkpoint)
	}

	return statePoints
}

func statePointsFromMilestones(milestones []*milestone.Milestone) statePoints {
	statePoints := make(statePoints, len(milestones))
	for i, milestone := range milestones {
		statePoints[i] = statePointFromMilestone(milestone)
	}

	return statePoints
}

type statePoints []*statePoint
