package rpctest

import (
	"fmt"
	"net/http"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/crypto"
)

func Bench2(erigon_url string) {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	req_id := 0
	req_id++
	blockNumTemplate := `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":%d}`
	var blockNumber EthBlockNumber
	if err := post(client, erigon_url, fmt.Sprintf(blockNumTemplate, req_id), &blockNumber); err != nil {
		fmt.Printf("Could not get block number: %v\n", err)
		return
	}
	if blockNumber.Error != nil {
		fmt.Printf("Error getting block number: %d %s\n", blockNumber.Error.Code, blockNumber.Error.Message)
		return
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
			fmt.Printf("Could not retrieve block %d: %v\n", bn, err)
			return
		}
		if b.Error != nil {
			fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
		}

		for i, tx := range b.Result.Transactions {
			if tx.To != nil && tx.Gas.ToInt().Uint64() > 21000 {
				// Request storage range
				// blockHash libcommon.Hash, txIndex int, contractAddress libcommon.Address, keyStart hexutil.Bytes, maxResult int
				req_id++
				storageRangeTemplate := `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}` //nolint
				sm := make(map[libcommon.Hash]storageEntry)
				nextKey := &libcommon.Hash{}
				for nextKey != nil {
					var sr DebugStorageRange
					if err := post(client, erigon_url, fmt.Sprintf(storageRangeTemplate, b.Result.Hash, i, tx.To, *nextKey, 1024, req_id), &sr); err != nil {
						fmt.Printf("Could not get storageRange: %x: %v\n", tx.Hash, err)
						return
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
				fmt.Printf("Could not get modified accounts: %v\n", err)
				return
			}
			if ma.Error != nil {
				fmt.Printf("Error getting modified accounts: %d %s\n", ma.Error.Code, ma.Error.Message)
				return
			}
			fmt.Printf("Done blocks %d-%d, modified accounts: %d\n", prevBn, bn, len(ma.Result))
			prevBn = bn
		}
	}
}
