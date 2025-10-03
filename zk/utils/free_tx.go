package utils

import (
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon/core/types"
)

// IsTxFreeByZkEgps determines if a transaction should be considered free
// based on zkEVM per-type Effective Gas Price (EGP) flags configured on chain.Config.
// A tx is free if the corresponding per-type EGP percentage is 0.
// The per-type classification mirrors zk/sequencer/effective_gas.go.
func IsTxFreeByZkEgps(cfg *chain.Config, tx types.Transaction) bool {
	if cfg == nil {
		return false
	}
	if cfg.EffectiveGasPriceForEthTransfer > 0 &&
		cfg.EffectiveGasPriceForErc20Transfer > 0 &&
		cfg.EffectiveGasPriceForContractInvocation > 0 &&
		cfg.EffectiveGasPriceForContractDeployment > 0 {
		return false
	}
	// Contract deployment
	if tx.GetTo() == nil {
		return cfg.EffectiveGasPriceForContractDeployment == 0
	}

	data := tx.GetData()
	// ETH transfer (no calldata)
	if len(data) == 0 {
		return cfg.EffectiveGasPriceForEthTransfer == 0
	}

	// ERC20 transfer(s)
	if HasSelector(data, SelectorERC20Transfer) || HasSelector(data, SelectorERC20TransferFrom) {
		return cfg.EffectiveGasPriceForErc20Transfer == 0
	}

	// All other contract invocations
	return cfg.EffectiveGasPriceForContractInvocation == 0
}
