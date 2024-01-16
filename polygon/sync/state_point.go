package sync

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

func statePointFromCheckpoint(checkpoint *heimdall.Checkpoint) *statePoint {
	return &statePoint{
		proposer:   checkpoint.Proposer,
		startBlock: new(big.Int).Set(checkpoint.StartBlock),
		endBlock:   new(big.Int).Set(checkpoint.EndBlock),
		rootHash:   checkpoint.RootHash,
		chainId:    checkpoint.BorChainID,
		timestamp:  checkpoint.Timestamp,
		kind:       checkpointKind,
	}
}

func statePointFromMilestone(milestone *heimdall.Milestone) *statePoint {
	return &statePoint{
		proposer:   milestone.Proposer,
		startBlock: new(big.Int).Set(milestone.StartBlock),
		endBlock:   new(big.Int).Set(milestone.EndBlock),
		rootHash:   milestone.Hash,
		chainId:    milestone.BorChainID,
		timestamp:  milestone.Timestamp,
		kind:       milestoneKind,
	}
}

type statePoint struct {
	proposer   common.Address
	startBlock *big.Int
	endBlock   *big.Int
	rootHash   common.Hash
	chainId    string
	timestamp  uint64
	kind       statePointKind
}

func (sp *statePoint) length() int {
	return int(new(big.Int).Sub(sp.endBlock, sp.startBlock).Int64() + 1)
}
