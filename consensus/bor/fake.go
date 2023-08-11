package bor

import (
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

type FakeBor struct {
	*ethash.FakeEthash
}

// NewFaker creates a bor consensus engine with a fake FakeEthash +
// processing of fake bor system contracts
func NewFaker() *FakeBor {
	return &FakeBor{
		FakeEthash: ethash.NewFaker(),
	}
}

func (f *FakeBor) Finalize(config *chain.Config, header *types.Header, state *state.IntraBlockState,
	txs types.Transactions, uncles []*types.Header, r types.Receipts, withdrawals []*types.Withdrawal,
	chain consensus.ChainHeaderReader, syscall consensus.SystemCall, logger log.Logger,
) (types.Transactions, types.Receipts, error) {
	systemcontracts.UpgradeBuildInSystemContract(config, header.Number, state, logger)
	return f.FakeEthash.Finalize(config, header, state, txs, uncles, r, withdrawals, chain, syscall, logger)
}
