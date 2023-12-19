package blocksync

import (
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
)

type heimdallLayer interface {
	FetchCheckpoints(fromBlockNum uint64) ([]*checkpoint.Checkpoint, error)
	FetchMilestones(fromBlockNum uint64) ([]*milestone.Milestone, error)
}
