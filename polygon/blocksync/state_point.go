package sync

import (
	"math/big"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
)

func statePointFromCheckpoint(cp *checkpoint.Checkpoint) statePoint {
	return statePoint{
		proposer:   cp.Proposer,
		startBlock: new(big.Int).Set(cp.StartBlock),
		endBlock:   new(big.Int).Set(cp.EndBlock),
		rootHash:   cp.RootHash,
		chainId:    cp.BorChainID,
		timestamp:  cp.Timestamp,
	}
}

func statePointFromMilestone(ms *milestone.Milestone) statePoint {
	return statePoint{
		proposer:   ms.Proposer,
		startBlock: new(big.Int).Set(ms.StartBlock),
		endBlock:   new(big.Int).Set(ms.EndBlock),
		rootHash:   ms.Hash,
		chainId:    ms.BorChainID,
		timestamp:  ms.Timestamp,
	}
}

type statePoint struct {
	proposer   libcommon.Address
	startBlock *big.Int
	endBlock   *big.Int
	rootHash   libcommon.Hash
	chainId    string
	timestamp  uint64
}

func (sp *statePoint) length() int {
	return int(sp.endBlock.Uint64() - sp.startBlock.Uint64() + 1)
}
