// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

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
	if res := reqGen.rpcCallJSON(method, body, &b); res.Err != nil {
		return len(pending), len(queued), len(baseFee), fmt.Errorf("failed to fetch txpool content: %v", res.Err)
	}

	resp, ok := b.Result.(map[string]interface{})

	if !ok {
		return 0, 0, 0, fmt.Errorf("unexpected result type: %T", b.Result)
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
