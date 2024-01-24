package backends

import zktypes "github.com/ledgerwatch/erigon/zk/types"

func (m callMsg) EffectiveGasPricePercentage() uint8 {
	return zktypes.EFFECTIVE_GAS_PRICE_PERCENTAGE_DISABLED
}
