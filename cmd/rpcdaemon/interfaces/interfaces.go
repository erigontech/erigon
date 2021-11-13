package interfaces

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
)

type BlockReader interface {
	WithSenders(tx kv.Tx, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error)
}
