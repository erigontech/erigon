package requests

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/rpctest/rpctest"
)

func TxpoolContent(reqId int) (int, int, int, error) {
	var (
		b       rpctest.EthTxPool
		pending map[string]interface{}
		queued  map[string]interface{}
		baseFee map[string]interface{}
	)

	reqGen := initialiseRequestGenerator(reqId)

	if res := reqGen.Erigon("txpool_content", reqGen.TxpoolContent(), &b); res.Err != nil {
		return len(pending), len(queued), len(baseFee), fmt.Errorf("failed to fetch txpool content: %v", res.Err)
	}

	resp := b.Result.(map[string]interface{})

	pendingLen := 0
	queuedLen := 0
	baseFeeLen := 0

	if resp["pending"] != nil {
		pending = resp["pending"].(map[string]interface{})
		for _, txs := range pending { // iterate over senders
			pendingLen += len(txs.(map[string]interface{}))
		}
	}

	if resp["queue"] != nil {
		queued = resp["queue"].(map[string]interface{})
		for _, txs := range pending {
			queuedLen += len(txs.(map[string]interface{}))
		}
	}

	if resp["baseFee"] != nil {
		baseFee = resp["baseFee"].(map[string]interface{})
		for _, txs := range baseFee {
			baseFeeLen += len(txs.(map[string]interface{}))
		}
	}

	return pendingLen, queuedLen, baseFeeLen, nil
}
