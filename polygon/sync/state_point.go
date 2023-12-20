package sync

import (
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
)

func statePointFromCheckpoint(cp *checkpoint.Checkpoint) statePoint {
	return statePoint{
		proposer:   cp.Proposer,
		startBlock: cp.StartBlock.Uint64(),
		endBlock:   cp.EndBlock.Uint64(),
		rootHash:   cp.RootHash,
		chainId:    cp.BorChainID,
		timestamp:  cp.Timestamp,
		kind:       checkpointKind,
	}
}

func statePointFromMilestone(ms *milestone.Milestone) statePoint {
	return statePoint{
		proposer:   ms.Proposer,
		startBlock: ms.StartBlock.Uint64(),
		endBlock:   ms.EndBlock.Uint64(),
		rootHash:   ms.Hash,
		chainId:    ms.BorChainID,
		timestamp:  ms.Timestamp,
		kind:       milestoneKind,
	}
}

type statePoint struct {
	proposer   common.Address
	startBlock uint64
	endBlock   uint64
	rootHash   common.Hash
	chainId    string
	timestamp  uint64
	kind       statePointKind
}

func (sp *statePoint) length() int {
	return int(sp.endBlock - sp.startBlock + 1)
}
