package arbitrum

import (
	"context"

	"github.com/erigontech/erigon/arb/arbitrum_types"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/execution/eth1/eth1_chain_reader.go"
)

type ArbInterface interface {
	PublishTransaction(ctx context.Context, tx types.Transaction, options *arbitrum_types.ConditionalOptions) error
	BlockChain() eth1_chain_reader.ChainReaderWriterEth1
	ArbNode() interface{}
}
