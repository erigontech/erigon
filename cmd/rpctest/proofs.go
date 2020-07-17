package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func proofs(chaindata string, url string, block int) {
	fileName := "trie.txt"
	ethDb := ethdb.MustOpen(chaindata)
	defer ethDb.Close()
	var t *trie.Trie
	if _, err := os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			// Resolve 6 top levels of the accounts trie
			l := trie.NewSubTrieLoader(uint64(block))
			rl := trie.NewRetainList(6)
			subTries, err1 := l.LoadSubTries(ethDb, uint64(block), rl, nil /* HashCollector */, [][]byte{nil}, []int{0}, false)
			if err1 != nil {
				panic(err1)
			}
			fmt.Printf("Resolved with hash: %x\n", subTries.Hashes[0])
			f, err1 := os.Create(fileName)
			if err1 == nil {
				defer f.Close()
				t.Print(f)
			} else {
				panic(err1)
			}
			fmt.Printf("Saved trie to file\n")
		} else {
			panic(err)
		}
	} else {
		f, err1 := os.Open(fileName)
		if err1 == nil {
			defer f.Close()
			var err2 error
			t, err2 = trie.Load(f)
			if err2 != nil {
				panic(err2)
			}
			fmt.Printf("Restored from file with hash: %x\n", t.Hash())
		} else {
			panic(err1)
		}
	}
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	reqID := 0

	level := 0
	diffKeys := [][]byte{{}}
	var newDiffKeys [][]byte

	for len(diffKeys) > 0 && level < 6 {

		fmt.Printf("================================================\n")
		fmt.Printf("LEVEL %d, diffKeys: %d\n", level, len(diffKeys))
		fmt.Printf("================================================\n")
		for _, diffKey := range diffKeys {
			// Find account with the suitable hash
			var startKey common.Hash
			for i, d := range diffKey {
				if i%2 == 0 {
					startKey[i/2] |= (d << 4)
				} else {
					startKey[i/2] |= d
				}
			}
			var account common.Address
			var found bool
			err := ethDb.Walk(dbutils.PreimagePrefix, startKey[:], 4*len(diffKey), func(k, v []byte) (bool, error) {
				if len(v) == common.AddressLength {
					copy(account[:], v)
					found = true
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				fmt.Printf("Error when looking for suitable account for diffKey %x\n", diffKey)
				return
			}
			if !found {
				fmt.Printf("Could not find suitable account for diffKey %x\n", diffKey)
				return
			}
			reqID++
			template := `{"jsonrpc":"2.0","method":"eth_getProof","params":["0x%x",[],"0x%x"],"id":%d}`
			var proof EthGetProof
			if err := post(client, url, fmt.Sprintf(template, account, block, reqID), &proof); err != nil {
				fmt.Printf("Could not get block number: %v\n", err)
				return
			}
			if proof.Error != nil {
				fmt.Printf("Error retrieving proof: %d %s\n", proof.Error.Code, proof.Error.Message)
				return
			}
			if len(proof.Result.AccountProof) <= len(diffKey) {
				fmt.Printf("RPC result needs to be at least %d levels deep\n", len(diffKey)+1)
				return
			}
			p := proof.Result.AccountProof[len(diffKey)]
			b := common.FromHex(p)
			h := common.BytesToHash(crypto.Keccak256(b))
			hE, err := t.HashOfHexKey(diffKey)
			if err != nil {
				fmt.Printf("Error computing partial hash for %x: %v\n", diffKey, err)
				return
			}
			if h != hE {
				fmt.Printf("key %x: %x %x adding nibbles: ", diffKey, h, hE)
				var branch [17][]byte
				err = rlp.DecodeBytes(b, &branch)
				if err != nil {
					fmt.Printf("Error decoding: %v\n", err)
					fmt.Printf("%s\n", p)
				}
				// Expand keys further
				for nibble := byte(0); nibble < 16; nibble++ {
					newDiff := make([]byte, len(diffKey)+1)
					copy(newDiff, diffKey)
					newDiff[len(diffKey)] = nibble
					var proofHash common.Hash
					copy(proofHash[:], branch[nibble])
					if proofHash != (common.Hash{}) || err != nil {
						newHash, err := t.HashOfHexKey(newDiff)
						if err != nil {
							fmt.Printf("Error computing partial hash for %x: %v\n", newDiff, err)
						}
						if proofHash != newHash {
							newDiffKeys = append(newDiffKeys, newDiff)
							fmt.Printf("%x", nibble)
						}
					}
				}
				fmt.Printf("\n")
			} else {
				fmt.Printf("MATCH key %x: %x %x\n", diffKey, h, hE)
			}
		}
		diffKeys = newDiffKeys
		newDiffKeys = nil
		level++
	}
	fmt.Printf("\n\nRESULT:\n")
	for _, diffKey := range diffKeys {
		fmt.Printf("%x\n", diffKey)
	}
}

func fixState(chaindata string, url string) {
	stateDb := ethdb.MustOpen(chaindata)
	defer stateDb.Close()
	engine := ethash.NewFullFaker()
	chainConfig := params.MainnetChainConfig
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	bc, errOpen := core.NewBlockChain(stateDb, nil, chainConfig, engine, vm.Config{}, nil, nil, txCacher)
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

	if err := stateDb.Walk(dbutils.CurrentStateBucket, nil, 0, func(k, v []byte) (bool, error) {
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
				fmt.Printf("Retrived %d storage items from geth archive node\n", len(sm))
				for key, entry := range sm {
					var cKey [common.HashLength + common.IncarnationLength + common.HashLength]byte
					copy(cKey[:], addrHash[:])
					binary.BigEndian.PutUint64(cKey[common.HashLength:], account.Incarnation)
					copy(cKey[common.HashLength+common.IncarnationLength:], key[:])
					dbValue, _ := stateDb.Get(dbutils.CurrentStateBucket, cKey[:])
					value := bytes.TrimLeft(entry.Value[:], "\x00")
					if !bytes.Equal(dbValue, value) {
						fmt.Printf("Key: %x, value: %x, dbValue: %x\n", key, value, dbValue)
						if err := stateDb.Put(dbutils.CurrentStateBucket, cKey[:], value); err != nil {
							fmt.Printf("%v\n", err)
						}
					}
				}
				var cKey [common.HashLength + common.IncarnationLength + common.HashLength]byte
				copy(cKey[:], addrHash[:])
				binary.BigEndian.PutUint64(cKey[common.HashLength:], account.Incarnation)
				if err := stateDb.Walk(dbutils.CurrentStateBucket, cKey[:], 8*(common.HashLength+common.IncarnationLength), func(k, v []byte) (bool, error) {
					var kh common.Hash
					copy(kh[:], k[common.HashLength+common.IncarnationLength:])
					if _, ok := sm[kh]; !ok {
						fmt.Printf("Key: %x, dbValue: %x\n", kh, v)
						if err := stateDb.Delete(dbutils.CurrentStateBucket, k); err != nil {
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
