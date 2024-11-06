package arbitrum

import (
	"context"

	"github.com/erigontech/erigon/arb/arbitrum_types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
)

type ArbInterface interface {
	PublishTransaction(ctx context.Context, tx *types.Transaction, options *arbitrum_types.ConditionalOptions) error
	BlockChain() *core.BlockChain
	ArbNode() interface{}
}
