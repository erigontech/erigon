package sync

type statePointKind string

const (
	checkpointKind = statePointKind("checkpoint")
	milestoneKind  = statePointKind("milestone")
)
