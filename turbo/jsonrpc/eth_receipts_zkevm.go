package jsonrpc

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
)

func (api *APIImpl) addEffectiveGasPercentage(fields map[string]interface{}, tx kv.Tx, txn types.Transaction) (map[string]interface{}, error) {
	effectiveGasPricePercentage, err := api.getEffectiveGasPricePercentage(tx, txn.Hash())
	if err != nil {
		return nil, err
	}
	fields["effectiveGasPrice"] = core.CalculateEffectiveGas(txn.GetPrice(), effectiveGasPricePercentage)
	return fields, nil
}
