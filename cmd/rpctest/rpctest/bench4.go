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

package rpctest

import (
	"fmt"

	"github.com/erigontech/erigon-lib/common"
)

func Bench4(erigon_url string) error {

	blockhash := common.HexToHash("0xdf15213766f00680c6a20ba76ba2cc9534435e19bc490039f3a7ef42095c8d13")
	req_id := 1
	template := `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",true],"id":%d}`
	var b EthBlockByNumber
	if err := post(client, erigon_url, fmt.Sprintf(template, 1720000, req_id), &b); err != nil {
		return fmt.Errorf("Could not retrieve block %d: %v\n", 1720000, err)
	}
	if b.Error != nil {
		fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
	}
	for txindex := 0; txindex < 6; txindex++ {
		txhash := b.Result.Transactions[txindex].Hash
		req_id++
		template = `{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["%s"],"id":%d}`
		var trace EthTxTrace
		if err := post(client, erigon_url, fmt.Sprintf(template, txhash, req_id), &trace); err != nil {
			print(client, erigon_url, fmt.Sprintf(template, txhash, req_id))
			return fmt.Errorf("Could not trace transaction %s: %v\n", txhash, err)
		}
		if trace.Error != nil {
			fmt.Printf("Error tracing transaction: %d %s\n", trace.Error.Code, trace.Error.Message)
		}
		print(client, erigon_url, fmt.Sprintf(template, txhash, req_id))
	}
	to := common.HexToAddress("0x8b3b3b624c3c0397d3da8fd861512393d51dcbac")
	sm := make(map[common.Hash]storageEntry)
	start := common.HexToHash("0xa283ff49a55f86420a4acd5835658d8f45180db430c7b0d7ae98da5c64f620dc")

	req_id++
	template = `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}`
	i := 6
	nextKey := &start
	for nextKey != nil {
		var sr DebugStorageRange
		if err := post(client, erigon_url, fmt.Sprintf(template, blockhash, i, to, *nextKey, 1024, req_id), &sr); err != nil {
			return fmt.Errorf("Could not get storageRange: %v\n", err)
		}
		if sr.Error != nil {
			fmt.Printf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
			break
		} else {
			nextKey = sr.Result.NextKey
			for k, v := range sr.Result.Storage {
				sm[k] = v
			}
		}
	}
	fmt.Printf("storageRange: %d\n", len(sm))
	return nil
}
