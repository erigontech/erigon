package rpctest

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

func FixState(chaindata string, url string) {
	db := ethdb.MustOpen(chaindata).RwKV()
	defer db.Close()
	tx, err1 := db.BeginRw(context.Background())
	if err1 != nil {
		panic(err1)
	}
	defer tx.Rollback()
	currentBlock := rawdb.ReadCurrentBlock(tx)
	blockNum := currentBlock.NumberU64()
	blockHash := currentBlock.Hash()
	fmt.Printf("Block number: %d\n", blockNum)
	fmt.Printf("Block root hash: %x\n", currentBlock.Root())
	reqID := 0
	roots := make(map[common.Hash]*accounts.Account)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	c, err := tx.Cursor(dbutils.HashedAccountsBucket)
	if err != nil {
		panic(err)
	}
	defer c.Close()
	if err := ethdb.ForEach(c, func(k, v []byte) (bool, error) {
		var addrHash common.Hash
		copy(addrHash[:], k[:32])
		if _, ok := roots[addrHash]; !ok {
			var account accounts.Account
			if ok, err2 := rawdb.ReadAccount(tx, addrHash, &account); err2 != nil {
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
			contractPrefix := make([]byte, common.HashLength+common.IncarnationLength)
			copy(contractPrefix, addrHash[:])
			binary.BigEndian.PutUint64(contractPrefix[common.HashLength:], account.Incarnation)
			rl := trie.NewRetainList(0)
			loader := trie.NewFlatDBTrieLoader("checkRoots")
			if err := loader.Reset(rl, nil, nil, false); err != nil {
				panic(err)
			}
			root, err1 := loader.CalcTrieRoot(tx, contractPrefix, nil)
			if err1 != nil || root != account.Root {
				fmt.Printf("%x: error %v, got hash %x, expected hash %x\n", addrHash, err1, root, account.Root)
				address, _ := tx.GetOne(dbutils.PreimagePrefix, addrHash[:])
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
					dbValue, _ := tx.GetOne(dbutils.HashedStorageBucket, cKey[:])
					value := bytes.TrimLeft(entry.Value[:], "\x00")
					if !bytes.Equal(dbValue, value) {
						fmt.Printf("Key: %x, value: %x, dbValue: %x\n", key, value, dbValue)
						if err := tx.Put(dbutils.HashedStorageBucket, cKey[:], value); err != nil {
							fmt.Printf("%v\n", err)
						}
					}
				}
				var cKey [common.HashLength + common.IncarnationLength + common.HashLength]byte
				copy(cKey[:], addrHash[:])
				binary.BigEndian.PutUint64(cKey[common.HashLength:], account.Incarnation)
				c2, err := tx.Cursor(dbutils.HashedStorageBucket)
				if err != nil {
					panic(err)
				}
				if err := ethdb.Walk(c, cKey[:], 8*(common.HashLength+common.IncarnationLength), func(k, v []byte) (bool, error) {
					var kh common.Hash
					copy(kh[:], k[common.HashLength+common.IncarnationLength:])
					if _, ok := sm[kh]; !ok {
						fmt.Printf("Key: %x, dbValue: %x\n", kh, v)
						if err := tx.Delete(dbutils.HashedStorageBucket, k, nil); err != nil {
							fmt.Printf("%v\n", err)
						}
					}
					return true, nil
				}); err != nil {
					panic(err)
				}
				c2.Close()
			}
		}
	}
}
