package misc

import (
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon/core/types"
)

func CalcBaseFeeZk(config *chain.Config, parent *types.Header) *big.Int {
	if config.SupportGasless {
		return big.NewInt(0)
	}

	return CalcBaseFee(config, parent)
}
