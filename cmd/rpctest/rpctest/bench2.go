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
	"github.com/erigontech/erigon/common/crypto"
)

func Bench2(erigon_url string) error {

	setRoutes(erigon_url, "")
	reqGen := &RequestGenerator{}

	lastBlock, err := reqGen.latestBlockNumber()
	if err != nil {
		return err
	}
	fmt.Printf("Last block: %d\n", lastBlock)
	firstBn := uint64(1720000 - 2)
	prevBn := firstBn
	for bn := firstBn; bn <= lastBlock; bn++ {
		var b EthBlockByNumber
		if res := reqGen.Erigon("eth_getBlockByNumber", reqGen.getBlockByNumber(bn, true /* withTxs */), &b); res.Err != nil {
			return fmt.Errorf("Could not retrieve block %d: %w\n", bn, res.Err)
		}
		if b.Error != nil {
			fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
		}

		for i := range b.Result.Transactions {
			txn := &b.Result.Transactions[i]
			if txn.To != nil && txn.Gas.ToInt().Uint64() > 21000 {
				// Request storage range
				sm := make(map[common.Hash]storageEntry)
				nextKey := &common.Hash{}
				for nextKey != nil {
					var sr DebugStorageRange
					if res := reqGen.Erigon("debug_storageRangeAt", reqGen.storageRangeAt(b.Result.Hash, i, txn.To, *nextKey), &sr); res.Err != nil {
						return fmt.Errorf("Could not get storageRange: %x: %w\n", txn.Hash, res.Err)
					}
					if sr.Error != nil {
						fmt.Printf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
						break
					} else {
						nextKey = sr.Result.NextKey
						for k, v := range sr.Result.Storage {
							sm[k] = v
							switch {
							case v.Key == nil:
								fmt.Printf("No key for sec key: %x\n", k)
							case k != crypto.Keccak256Hash(v.Key[:]):
								fmt.Printf("Different sec key: %x %x (%x), value %x\n", k, crypto.Keccak256Hash(v.Key[:]), *(v.Key), v.Value)
							default:
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
			var ma DebugModifiedAccounts
			if res := reqGen.Erigon("debug_getModifiedAccountsByNumber", reqGen.getModifiedAccountsByNumber(prevBn, bn), &ma); res.Err != nil {
				return fmt.Errorf("Could not get modified accounts: %w\n", res.Err)
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
