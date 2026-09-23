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

	"github.com/erigontech/erigon/common"
)

func Bench6(erigon_url string) error {

	setRoutes(erigon_url, "")
	reqGen := &RequestGenerator{}

	lastBlock, err := reqGen.latestBlockNumber()
	if err != nil {
		return err
	}
	fmt.Printf("Last block: %d\n", lastBlock)
	accounts := make(map[common.Address]struct{})
	firstBn := uint64(100000)
	for bn := firstBn; bn <= lastBlock; bn++ {
		var b EthBlockByNumber
		if res := reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b); res.Err != nil {
			return fmt.Errorf("Could not retrieve block %d: %w\n", bn, res.Err)
		}
		if b.Error != nil {
			fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
		}
		accounts[b.Result.Miner] = struct{}{}
		for i := range b.Result.Transactions {
			txn := &b.Result.Transactions[i]
			accounts[txn.From] = struct{}{}
			if txn.To != nil {
				accounts[*txn.To] = struct{}{}
			}
			var receipt EthReceipt
			if res := reqGen.Erigon("eth_getTransactionReceipt", reqGen.getTransactionReceipt(txn.Hash), &receipt); res.Err != nil {
				printRPCRequest(client, erigon_url, res.RequestBody)
				return fmt.Errorf("Count not get receipt: %s: %w\n", txn.Hash, res.Err)
			}
			if receipt.Error != nil {
				return fmt.Errorf("Error getting receipt: %d %s\n", receipt.Error.Code, receipt.Error.Message)
			}
		}
	}
	return nil
}
