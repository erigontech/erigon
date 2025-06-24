package sequencer

import (
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/core/types"
)

func DeriveEffectiveGasPrice(cfg ethconfig.Zk, tx types.Transaction) uint8 {
	if tx.GetTo() == nil {
		return cfg.EffectiveGasPriceForContractDeployment
	}

	data := tx.GetData()
	dataLen := len(data)
	if dataLen != 0 {
		if dataLen >= 8 {
			// transfer's method id 0xa9059cbb
			isTransfer := data[0] == 169 && data[1] == 5 && data[2] == 156 && data[3] == 187
			// transfer's method id 0x23b872dd
			isTransferFrom := data[0] == 35 && data[1] == 184 && data[2] == 114 && data[3] == 221
			if isTransfer || isTransferFrom {
				return cfg.EffectiveGasPriceForErc20Transfer
			}
		}

		return cfg.EffectiveGasPriceForContractInvocation
	}

	return cfg.EffectiveGasPriceForEthTransfer
}
