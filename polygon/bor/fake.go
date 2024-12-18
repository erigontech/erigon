package bor

import (
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/ethash"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
)

type FakeBor struct {
	*ethash.FakeEthash
}

// NewFaker creates a bor consensus engine with a FakeEthash
func NewFaker() *FakeBor {
	return &FakeBor{
		FakeEthash: ethash.NewFaker(),
	}
}

func (f *FakeBor) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainReader, syscall consensus.SystemCall, logger log.Logger,
) (types.Transactions, types.Receipts, types.FlatRequests, error) {
	return f.FakeEthash.Finalize(config, header, state, txs, uncles, r, withdrawals, chain, syscall, logger)
}
