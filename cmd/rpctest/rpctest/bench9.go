package rpctest

import (
	"fmt"
	"net/http"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/state"
)

// bench9 tests eth_getProof
func Bench9(erigonURL, gethURL string, needCompare bool) {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	var res CallResult
	reqGen := &RequestGenerator{
		client: client,
	}

	reqGen.reqID++
	var blockNumber EthBlockNumber
	res = reqGen.Erigon("eth_blockNumber", reqGen.blockNumber(), &blockNumber)
	if res.Err != nil {
		fmt.Printf("Could not get block number: %v\n", res.Err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
	}
	lastBlock := blockNumber.Number
	fmt.Printf("Last block: %d\n", lastBlock)
	// Go back 256 blocks
	bn := uint64(lastBlock) - 256
	page := libcommon.Hash{}.Bytes()

	for len(page) > 0 {
		accRangeTG := make(map[libcommon.Address]state.DumpAccount)
		var sr DebugAccountRange
		reqGen.reqID++
		res = reqGen.Erigon("debug_accountRange", reqGen.accountRange(bn, page, 256), &sr)

		if res.Err != nil {
			fmt.Printf("Could not get accountRange (Erigon): %v\n", res.Err)
			return
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
			reqGen.reqID++
			var storageList []libcommon.Hash
			// And now with the storage, if present
			if len(dumpAcc.Storage) > 0 {
				for key := range dumpAcc.Storage {
					storageList = append(storageList, libcommon.HexToHash(key))
					if len(storageList) > 100 {
						break
					}
				}
			}
			res = reqGen.Erigon("eth_getProof", reqGen.getProof(bn, address, storageList), &proof)
			if res.Err != nil {
				fmt.Printf("Could not get getProof (Erigon): %v\n", res.Err)
				return
			}
			if proof.Error != nil {
				fmt.Printf("Error getting getProof (Erigon): %d %s\n", proof.Error.Code, proof.Error.Message)
				break
			}
			if needCompare {
				var gethProof EthGetProof
				reqGen.reqID++
				res = reqGen.Geth("eth_getProof", reqGen.getProof(bn, address, storageList), &gethProof)
				if res.Err != nil {
					fmt.Printf("Could not get getProof (geth): %v\n", res.Err)
					return
				}
				if gethProof.Error != nil {
					fmt.Printf("Error getting getProof (geth): %d %s\n", gethProof.Error.Code, gethProof.Error.Message)
					break
				}
				if !compareProofs(&proof, &gethProof) {
					fmt.Printf("Proofs are different\n")
					break
				}
			}
		}
	}
}
