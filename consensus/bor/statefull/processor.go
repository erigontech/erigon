package statefull

import (
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
)

type ChainContext struct {
	Chain consensus.ChainHeaderReader
	Bor   consensus.Engine
}

func (c ChainContext) Engine() consensus.Engine {
	return c.Bor
}

func (c ChainContext) GetHeader(hash common.Hash, number uint64) *types.Header {
	return c.Chain.GetHeader(hash, number)
}
