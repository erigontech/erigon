package rpctest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

func FixState(chaindata string, url string) {
	stateDb := ethdb.MustOpen(chaindata)
	defer stateDb.Close()
	engine := ethash.NewFullFaker()
	chainConfig := params.MainnetChainConfig
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	bc, errOpen := core.NewBlockChain(stateDb, nil, chainConfig, engine, vm.Config{}, nil, txCacher)
	if errOpen != nil {
		panic(errOpen)
	}
	defer bc.Stop()
	currentBlock := bc.CurrentBlock()
	blockNum := currentBlock.NumberU64()
	blockHash := currentBlock.Hash()
	fmt.Printf("Block number: %d\n", blockNum)
	fmt.Printf("Block root hash: %x\n", currentBlock.Root())
	reqID := 0
	roots := make(map[common.Hash]*accounts.Account)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	if err := stateDb.Walk(dbutils.HashedAccountsBucket, nil, 0, func(k, v []byte) (bool, error) {
		var addrHash common.Hash
		copy(addrHash[:], k[:32])
		if _, ok := roots[addrHash]; !ok {
			var account accounts.Account
			if ok, err2 := rawdb.ReadAccount(stateDb, addrHash, &account); err2 != nil {
				return false, err2
			} else if !ok {
				roots[addrHash] = nil
			} else {
				roots[addrHash] = &account
			}
		}

		return true, nil
	}); err != nil {
		panic(err)
	}
	for addrHash, account := range roots {
		if account != nil && account.Root != trie.EmptyRoot {
			sl := trie.NewSubTrieLoader(blockNum)
			contractPrefix := make([]byte, common.HashLength+common.IncarnationLength)
			copy(contractPrefix, addrHash[:])
			binary.BigEndian.PutUint64(contractPrefix[common.HashLength:], account.Incarnation)
			rl := trie.NewRetainList(0)
			subTries, err1 := sl.LoadSubTries(stateDb, blockNum, rl, nil /* HashCollector */, [][]byte{contractPrefix}, []int{8 * len(contractPrefix)}, false)
			if err1 != nil || subTries.Hashes[0] != account.Root {
				fmt.Printf("%x: error %v, got hash %x, expected hash %x\n", addrHash, err1, subTries.Hashes[0], account.Root)
				address, _ := stateDb.Get(dbutils.PreimagePrefix, addrHash[:])
				template := `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}`
				sm := make(map[common.Hash]storageEntry)
				nextKey := &common.Hash{}
				for nextKey != nil {
					reqID++
					var sr DebugStorageRange
					if err := post(client, url, fmt.Sprintf(template, blockHash, 0, address, *nextKey, 1024, reqID), &sr); err != nil {
						fmt.Printf("Could not get storageRange: %v\n", err)
						return
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
				fmt.Printf("Retrieved %d storage items from geth archive node\n", len(sm))
				for key, entry := range sm {
					var cKey [common.HashLength + common.IncarnationLength + common.HashLength]byte
					copy(cKey[:], addrHash[:])
					binary.BigEndian.PutUint64(cKey[common.HashLength:], account.Incarnation)
					copy(cKey[common.HashLength+common.IncarnationLength:], key[:])
					dbValue, _ := stateDb.Get(dbutils.HashedStorageBucket, cKey[:])
					value := bytes.TrimLeft(entry.Value[:], "\x00")
					if !bytes.Equal(dbValue, value) {
						fmt.Printf("Key: %x, value: %x, dbValue: %x\n", key, value, dbValue)
						if err := stateDb.Put(dbutils.HashedStorageBucket, cKey[:], value); err != nil {
							fmt.Printf("%v\n", err)
						}
					}
				}
				var cKey [common.HashLength + common.IncarnationLength + common.HashLength]byte
				copy(cKey[:], addrHash[:])
				binary.BigEndian.PutUint64(cKey[common.HashLength:], account.Incarnation)
				if err := stateDb.Walk(dbutils.HashedStorageBucket, cKey[:], 8*(common.HashLength+common.IncarnationLength), func(k, v []byte) (bool, error) {
					var kh common.Hash
					copy(kh[:], k[common.HashLength+common.IncarnationLength:])
					if _, ok := sm[kh]; !ok {
						fmt.Printf("Key: %x, dbValue: %x\n", kh, v)
						if err := stateDb.Delete(dbutils.HashedStorageBucket, k, nil); err != nil {
							fmt.Printf("%v\n", err)
						}
					}
					return true, nil
				}); err != nil {
					panic(err)
				}
			}
		}
	}
}
