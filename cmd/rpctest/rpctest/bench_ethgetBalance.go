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
)

// BenchEthGetBalance compares response of Erigon with Geth
// but also can be used for comparing RPCDaemon with Geth
// parameters:
// needCompare - if false - doesn't call Erigon and doesn't compare responses
//
//	use false value - to generate vegeta files, it's faster but we can generate vegeta files for Geth and Erigon
func BenchEthGetBalance(erigonURL, gethURL string, needCompare bool, blockFrom uint64, blockTo uint64) error {
	setRoutes(erigonURL, gethURL)

	reqGen := &RequestGenerator{}

	var res CallResult
	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getBalance"}, resultsCh)
	}

	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	for bn := blockFrom; bn <= blockTo; bn++ {

		var b EthBlockByNumber
		res = reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b)
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}

		if b.Error != nil {
			return fmt.Errorf("Error retrieving block (Erigon): %d %s\n", b.Error.Code, b.Error.Message)
		}

		if needCompare {
			var bg EthBlockByNumber
			res = reqGen.Geth("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &bg)
			if res.Err != nil {
				return fmt.Errorf("Could not retrieve block (geth) %d: %v\n", bn, res.Err)
			}
			if bg.Error != nil {
				return fmt.Errorf("Error retrieving block (geth): %d %s\n", bg.Error.Code, bg.Error.Message)
			}
			if !compareBlocks(&b, &bg) {
				return fmt.Errorf("Block difference for %d\n", bn)
			}
		}

		for txn := range b.Result.Transactions {

			tx := b.Result.Transactions[txn]
			var balance EthBalance
			account := tx.From

			res = reqGen.Erigon("eth_getBalance", reqGen.getBalance(account, bn), &balance)
			if !needCompare {
				resultsCh <- res
			}
			if res.Err != nil {
				return fmt.Errorf("Could not get account balance (Erigon): %v\n", res.Err)
			}
			if balance.Error != nil {
				return fmt.Errorf("Error getting account balance (Erigon): %d %s", balance.Error.Code, balance.Error.Message)
			}
			if needCompare {
				var balanceg EthBalance
				res = reqGen.Geth("eth_getBalance", reqGen.getBalance(account, bn), &balanceg)
				if res.Err != nil {
					return fmt.Errorf("Could not get account balance (geth): %v\n", res.Err)
				}
				if balanceg.Error != nil {
					return fmt.Errorf("Error getting account balance (geth): %d %s\n", balanceg.Error.Code, balanceg.Error.Message)
				}
				if !compareBalances(&balance, &balanceg) {
					return fmt.Errorf("Account %x balance difference for block %d\n", account, bn)
				}
			}
		}
	}
	return nil
}
