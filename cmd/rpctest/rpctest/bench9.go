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
	"github.com/erigontech/erigon/core/state"
)

// bench9 tests eth_getProof
func Bench9(erigonURL, gethURL string, needCompare, latest bool) error {
	setRoutes(erigonURL, gethURL)

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
	lastBlock := blockNumber.Number
	fmt.Printf("Last block: %d\n", lastBlock)
	// Go back 256 blocks
	bn := uint64(lastBlock) - 256
	page := common.Hash{}.Bytes()

	var resultsCh chan CallResult = nil
	if !needCompare {
		resultsCh = make(chan CallResult, 1000)
		defer close(resultsCh)
		go vegetaWrite(true, []string{"eth_getProof"}, resultsCh)
	}

	for len(page) > 0 {
		accRangeTG := make(map[common.Address]state.DumpAccount)
		var sr DebugAccountRange

		res = reqGen.Erigon("debug_accountRange", reqGen.accountRange(bn, page, 256), &sr)

		if res.Err != nil {
			return fmt.Errorf("Could not get accountRange (Erigon): %v\n", res.Err)
		}

		getProofBn := bn
		if latest {
			getProofBn = 0 // latest
		}

		if sr.Error != nil {
			fmt.Printf("Error getting accountRange (Erigon): %d %s\n", sr.Error.Code, sr.Error.Message)
			break
		} else {
			page = sr.Result.Next
			for k, v := range sr.Result.Accounts {
				accRangeTG[k] = v
			}
		}
		for address, dumpAcc := range accRangeTG {
			var proof EthGetProof

			var storageList []common.Hash
			// And now with the storage, if present
			if len(dumpAcc.Storage) > 0 {
				for key := range dumpAcc.Storage {
					storageList = append(storageList, common.HexToHash(key))
					if len(storageList) > 100 {
						break
					}
				}
			}
			res = reqGen.Erigon("eth_getProof", reqGen.getProof(getProofBn, address, storageList), &proof)
			if res.Err != nil {
				return fmt.Errorf("Could not get getProof (Erigon): %v\n", res.Err)
			}
			if proof.Error != nil {
				fmt.Printf("Error getting getProof (Erigon): %d %s\n", proof.Error.Code, proof.Error.Message)
				break
			}
			if needCompare {
				var gethProof EthGetProof

				res = reqGen.Geth("eth_getProof", reqGen.getProof(getProofBn, address, storageList), &gethProof)
				if res.Err != nil {
					return fmt.Errorf("Could not get getProof (geth): %v\n", res.Err)
				}
				if gethProof.Error != nil {
					fmt.Printf("Error getting getProof (geth): %d %s\n", gethProof.Error.Code, gethProof.Error.Message)
					break
				}
				if !compareProofs(&proof, &gethProof) {
					fmt.Printf("Proofs are different\n")
					break
				}
			} else {
				resultsCh <- res
			}
		}
	}
	return nil
}
