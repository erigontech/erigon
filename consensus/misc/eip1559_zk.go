package misc

import (
	"math/big"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/types"
)

func CalcBaseFeeZk(config *chain.Config, parent *types.Header) *big.Int {
	if config.SupportGasless {
		return big.NewInt(0)
	}
	if !config.IsLondon(parent.Number.Uint64() + 1) {
		return big.NewInt(0)
	}

	return CalcBaseFee(config, parent)
}
