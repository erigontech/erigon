package sequencer

import (
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/zk/utils"
)

func DeriveEffectiveGasPrice(cfg ethconfig.Zk, tx types.Transaction) uint8 {
	if tx.GetTo() == nil {
		return cfg.EffectiveGasPriceForContractDeployment
	}

	data := tx.GetData()
	dataLen := len(data)
	if dataLen != 0 {
		if utils.HasSelector(data, utils.SelectorERC20Transfer) || utils.HasSelector(data, utils.SelectorERC20TransferFrom) {
			return cfg.EffectiveGasPriceForErc20Transfer
		}
		return cfg.EffectiveGasPriceForContractInvocation
	}

	return cfg.EffectiveGasPriceForEthTransfer
}
