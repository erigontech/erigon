package commands

import (
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
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
