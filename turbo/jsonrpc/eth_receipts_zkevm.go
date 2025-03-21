package jsonrpc

import (
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/zk/hermez_db"
)

func (api *APIImpl) addEffectiveGasPercentage(fields map[string]interface{}, tx kv.Tx, txn types.Transaction) (map[string]interface{}, error) {
	hermezReader := hermez_db.NewHermezDbReader(tx)

	effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(txn.Hash())
	if err != nil {
		return nil, err
	}
	fields["effectiveGasPrice"] = core.CalculateEffectiveGas(txn.GetPrice().Clone(), effectiveGasPricePercentage)
	return fields, nil
}
