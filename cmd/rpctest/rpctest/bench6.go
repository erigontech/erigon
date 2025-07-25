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

func Bench6(erigon_url string) error {

	req_id := 0

	req_id++
	template := `
{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":%d}
`
	var blockNumber EthBlockNumber
	if err := post(client, erigon_url, fmt.Sprintf(template, req_id), &blockNumber); err != nil {
		return fmt.Errorf("Could not get block number: %v\n", err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	lastBlock := blockNumber.Number
	fmt.Printf("Last block: %d\n", lastBlock)
	accounts := make(map[common.Address]struct{})
	firstBn := 100000
	for bn := firstBn; bn <= int(lastBlock); bn++ {
		req_id++
		template := `
{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",true],"id":%d}
`
		var b EthBlockByNumber
		if err := post(client, erigon_url, fmt.Sprintf(template, bn, req_id), &b); err != nil {
			return fmt.Errorf("Could not retrieve block %d: %v\n", bn, err)
		}
		if b.Error != nil {
			fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
		}
		accounts[b.Result.Miner] = struct{}{}
		for _, txn := range b.Result.Transactions {
			accounts[txn.From] = struct{}{}
			if txn.To != nil {
				accounts[*txn.To] = struct{}{}
			}
			req_id++
			template = `
{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":%d}
`
			var receipt EthReceipt
			if err := post(client, erigon_url, fmt.Sprintf(template, txn.Hash, req_id), &receipt); err != nil {
				print(client, erigon_url, fmt.Sprintf(template, txn.Hash, req_id))
				return fmt.Errorf("Count not get receipt: %s: %v\n", txn.Hash, err)
			}
			if receipt.Error != nil {
				return fmt.Errorf("Error getting receipt: %d %s\n", receipt.Error.Code, receipt.Error.Message)
			}
		}
	}
	return nil
}
