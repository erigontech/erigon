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

// BenchEthGetBlockByNumber generates lots of requests for eth_getBlockByNumber to attempt to reproduce issue where empty results are being returned
func BenchEthGetBlockByNumber(erigonURL string) error {
	setRoutes(erigonURL, erigonURL)

	var res CallResult
	reqGen := &RequestGenerator{}

	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		return fmt.Errorf("Could not get block number: %v\n", res.Err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	fmt.Printf("Last block: %d\n", blockNumber.Number)
	for bn := uint64(0); bn <= uint64(blockNumber.Number)/2; bn++ {

		res = reqGen.Erigon2("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, false /* withTxs */))
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn, res.Err)
		}
		if errVal := res.Result.Get("error"); errVal != nil {
			return fmt.Errorf("error: %d %s", errVal.GetInt("code"), errVal.GetStringBytes("message"))
		}
		if res.Result.Get("result") == nil || res.Result.Get("result").Get("number") == nil {
			return fmt.Errorf("empty result: %s\n", res.Response)
		}

		bn1 := uint64(blockNumber.Number) - bn
		res = reqGen.Erigon2("eth_getBlockByNumber", reqGen.getBlockByNumber(bn1, false /* withTxs */))
		if res.Err != nil {
			return fmt.Errorf("Could not retrieve block (Erigon) %d: %v\n", bn1, res.Err)
		}
		if errVal := res.Result.Get("error"); errVal != nil {
			return fmt.Errorf("error: %d %s", errVal.GetInt("code"), errVal.GetStringBytes("message"))
		}
		if res.Result.Get("result") == nil || res.Result.Get("result").Get("number") == nil {
			return fmt.Errorf("empty result: %s\n", res.Response)
		}
	}
	return nil
}
