package health

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
)

func checkTxPool(ctx context.Context, txApi TxPoolAPI, ethApi EthAPI) error {
	if txApi == nil {
		return fmt.Errorf("no connection to the Erigon server or `tx_pool` namespace isn't enabled")
	}

	if ethApi == nil {
		return fmt.Errorf("no connection to the Erigon server or `eth` namespace isn't enabled")
	}

	var err error

	data, err := txApi.Content(ctx)
	if err != nil {
		return err
	}

	contentMap, ok := data.(map[string]map[string]map[string]*jsonrpc.RPCTransaction)
	if !ok {
		return fmt.Errorf("unexpected response type for tx_pool: %T", data)
	}

	pendingData, ok := contentMap["pending"]
	if !ok {
		return nil
	}

	if pendingData != nil && len(pendingData) > 0 {
		// pending pool has transactions lets check last block if it was 0 tx
		var latestBlockData map[string]interface{}
		fullTx := false
		latestBlockData, err = ethApi.GetBlockByNumber(ctx, -1, &fullTx)
		if err != nil {
			return err
		}

		var latestBlockNo *hexutil.Big
		latestBlockNo, ok = latestBlockData["number"].(*hexutil.Big)
		if !ok {
			return nil
		}

		// check 5 blocks back from latest to see if any of them has transactions
		var foundTransactions bool
		for i := 0; i < 5; i++ {
			blockToCheck := latestBlockNo.ToInt().Int64() - int64(i)
			var prevBlockData map[string]interface{}
			prevBlockData, err = ethApi.GetBlockByNumber(ctx, rpc.BlockNumber(blockToCheck), &fullTx)
			if err != nil {
				return err
			}
			var transactions []interface{}
			transactions, ok = prevBlockData["transactions"].([]interface{})
			if !ok {
				return nil
			}
			if len(transactions) != 0 {
				foundTransactions = true
				break
			}
		}
		if !foundTransactions {
			return fmt.Errorf("found transactions in pending pool but last 5 blocks have no transactions")
		}
	}

	return nil
}
