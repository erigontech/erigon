package misc

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
)

func CalcBaseFeeZk(config *chain.Config, parent *types.Header) *big.Int {
	if config.AllowFreeTransactions || parent.Number.Cmp(big.NewInt(0)) == 0 {
		// If the parent is the genesis block, the next block will include the initial batch transaction, which is a legacy transaction, so the basefee will be set to 0
		return big.NewInt(0)
	}
	if !config.IsLondon(parent.Number.Uint64() + 1) {
		return big.NewInt(0)
	}

	// If the parent block is injected block from L1 at block 1 (while block 0 is the genesis), it will have base fee of 0 so we will set the basefee of current block to the initial basefee
	if parent.Number.Cmp(big.NewInt(1)) == 0 {
		return new(big.Int).SetUint64(params.InitialBaseFee)
	}

	return CalcBaseFee(config, parent)
}
