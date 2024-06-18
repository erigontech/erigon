package bor

import (
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
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
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal, requests types.Requests,
	chain consensus.ChainReader, syscall consensus.SystemCall, logger log.Logger,
) (types.Transactions, types.Receipts, types.Requests, error) {
	return f.FakeEthash.Finalize(config, header, state, txs, uncles, r, withdrawals, requests, chain, syscall, logger)
}
