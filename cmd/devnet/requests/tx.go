package requests

import (
	"fmt"
)

type EthTxPool struct {
	CommonResponse
	Result interface{} `json:"result"`
}

func (reqGen *requestGenerator) TxpoolContent() (int, int, int, error) {
	var (
		b       EthTxPool
		pending map[string]interface{}
		queued  map[string]interface{}
		baseFee map[string]interface{}
	)

	method, body := reqGen.txpoolContent()
	if res := reqGen.call(method, body, &b); res.Err != nil {
		return len(pending), len(queued), len(baseFee), fmt.Errorf("failed to fetch txpool content: %v", res.Err)
	}

	resp, ok := b.Result.(map[string]interface{})

	if !ok {
		return 0, 0, 0, fmt.Errorf("Unexpected result type: %T", b.Result)
	}

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
		for _, txs := range queued {
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

func (req *requestGenerator) txpoolContent() (RPCMethod, string) {
	const template = `{"jsonrpc":"2.0","method":%q,"params":[],"id":%d}`
	return Methods.TxpoolContent, fmt.Sprintf(template, Methods.TxpoolContent, req.reqID)
}
