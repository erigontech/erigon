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
	"github.com/erigontech/erigon-lib/crypto"
)

func Bench2(erigon_url string) error {

	req_id := 0
	req_id++
	blockNumTemplate := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":%d}`
	var blockNumber EthBlockNumber
	if err := post(client, erigon_url, fmt.Sprintf(blockNumTemplate, req_id), &blockNumber); err != nil {
		return fmt.Errorf("Could not get block number: %v\n", err)
	}
	if blockNumber.Error != nil {
		return fmt.Errorf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
	}
	lastBlock := blockNumber.Number
	fmt.Printf("Last block: %d\n", lastBlock)
	firstBn := 1720000 - 2
	prevBn := firstBn
	for bn := firstBn; bn <= int(lastBlock); bn++ {
		req_id++
		blockByNumTemplate := `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",true],"id":%d}` //nolint
		var b EthBlockByNumber
		if err := post(client, erigon_url, fmt.Sprintf(blockByNumTemplate, bn, req_id), &b); err != nil {
			return fmt.Errorf("Could not retrieve block %d: %v\n", bn, err)
		}
		if b.Error != nil {
			fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
		}

		for i, txn := range b.Result.Transactions {
			if txn.To != nil && txn.Gas.ToInt().Uint64() > 21000 {
				// Request storage range
				// blockHash common.Hash, txIndex int, contractAddress common.Address, keyStart hexutil.Bytes, maxResult int
				req_id++
				storageRangeTemplate := `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}` //nolint
				sm := make(map[common.Hash]storageEntry)
				nextKey := &common.Hash{}
				for nextKey != nil {
					var sr DebugStorageRange
					if err := post(client, erigon_url, fmt.Sprintf(storageRangeTemplate, b.Result.Hash, i, txn.To, *nextKey, 1024, req_id), &sr); err != nil {
						return fmt.Errorf("Could not get storageRange: %x: %v\n", txn.Hash, err)
					}
					if sr.Error != nil {
						fmt.Printf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
						break
					} else {
						nextKey = sr.Result.NextKey
						for k, v := range sr.Result.Storage {
							sm[k] = v
							if v.Key == nil {
								fmt.Printf("No key for sec key: %x\n", k)
							} else if k != crypto.Keccak256Hash(v.Key[:]) {
								fmt.Printf("Different sec key: %x %x (%x), value %x\n", k, crypto.Keccak256Hash(v.Key[:]), *(v.Key), v.Value)
							} else {
								fmt.Printf("Keys: %x %x, value %x\n", *(v.Key), k, v.Value)
							}
						}
					}
				}
				fmt.Printf("storageRange: %d\n", len(sm))
			}
		}

		if prevBn < bn && bn%1000 == 0 {
			// Checking modified accounts
			req_id++
			accountRangeTemplate := `{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":[%d, %d],"id":%d}` //nolint
			var ma DebugModifiedAccounts
			if err := post(client, erigon_url, fmt.Sprintf(accountRangeTemplate, prevBn, bn, req_id), &ma); err != nil {
				return fmt.Errorf("Could not get modified accounts: %v\n", err)
			}
			if ma.Error != nil {
				return fmt.Errorf("Error getting modified accounts: %d %s\n", ma.Error.Code, ma.Error.Message)
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(ma.Result))
			prevBn = bn
		}
	}
	return nil
}
