package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie"
)

type EthGetProof struct {
	CommonResponse
	Result AccountResult `json:"result"`
}

// Result structs for GetProof
type AccountResult struct {
	Address      common.Address  `json:"address"`
	AccountProof []string        `json:"accountProof"`
	Balance      *hexutil.Big    `json:"balance"`
	CodeHash     common.Hash     `json:"codeHash"`
	Nonce        hexutil.Uint64  `json:"nonce"`
	StorageHash  common.Hash     `json:"storageHash"`
	StorageProof []StorageResult `json:"storageProof"`
}
type StorageResult struct {
	Key   string       `json:"key"`
	Value *hexutil.Big `json:"value"`
	Proof []string     `json:"proof"`
}

func proofs(chaindata string, url string, block int) {
	fileName := "trie.txt"
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		panic(err)
	}
	defer ethDb.Close()
	var t *trie.Trie
	if _, err = os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			// Resolve 6 top levels of the accounts trie
			r := trie.NewResolver(6, true, uint64(block))
			t = trie.New(common.Hash{})
			req := t.NewResolveRequest(nil, []byte{}, 0, nil)
			r.AddRequest(req)
			err = r.ResolveWithDb(ethDb, uint64(block), false)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Resolved with hash: %x\n", t.Hash())
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
			err = ethDb.Walk(dbutils.PreimagePrefix, startKey[:], uint(4*len(diffKey)), func(k, v []byte) (bool, error) {
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
	stateDb, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		panic(err)
	}
	defer stateDb.Close()
	engine := ethash.NewFullFaker()
	chainConfig := params.MainnetChainConfig
	bc, err := core.NewBlockChain(stateDb, nil, chainConfig, engine, vm.Config{}, nil)
	if err != nil {
		panic(err)
	}
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

	err = stateDb.Walk(dbutils.StorageBucket, nil, 0, func(k, v []byte) (bool, error) {
		var addrHash common.Hash
		copy(addrHash[:], k[:32])
		if _, ok := roots[addrHash]; !ok {
			if enc, _ := stateDb.Get(dbutils.AccountsBucket, addrHash[:]); enc == nil {
				roots[addrHash] = nil
			} else {
				var account accounts.Account
				if err = account.DecodeForStorage(enc); err != nil {
					return false, err
				}
				roots[addrHash] = &account
			}
		}

		return true, nil
	})
	if err != nil {
		panic(err)
	}
	for addrHash, account := range roots {
		if account != nil && account.Root != trie.EmptyRoot {
			st := trie.New(account.Root)
			sr := trie.NewResolver(32, false, blockNum)
			key := []byte{}
			contractPrefix := make([]byte, common.HashLength+common.IncarnationLength)
			copy(contractPrefix, addrHash[:])
			binary.BigEndian.PutUint64(contractPrefix[common.HashLength:], ^account.Incarnation)
			streq := st.NewResolveRequest(contractPrefix, key, 0, account.Root[:])
			sr.AddRequest(streq)
			err = sr.ResolveWithDb(stateDb, blockNum, false)
			if err != nil {
				fmt.Printf("%x: %v\n", addrHash, err)
				address, _ := stateDb.Get(dbutils.PreimagePrefix, addrHash[:])
				template := `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}`
				sm := make(map[common.Hash]storageEntry)
				nextKey := &common.Hash{}
				for nextKey != nil {
					reqID++
					var sr DebugStorageRange
					if err = post(client, url, fmt.Sprintf(template, blockHash, 0, address, *nextKey, 1024, reqID), &sr); err != nil {
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
					binary.BigEndian.PutUint64(cKey[common.HashLength:], ^account.Incarnation)
					copy(cKey[common.HashLength+common.IncarnationLength:], key[:])
					dbValue, _ := stateDb.Get(dbutils.StorageBucket, cKey[:])
					value := bytes.TrimLeft(entry.Value[:], "\x00")
					if !bytes.Equal(dbValue, value) {
						fmt.Printf("Key: %x, value: %x, dbValue: %x\n", key, value, dbValue)
						if err = stateDb.Put(dbutils.StorageBucket, cKey[:], value); err != nil {
							fmt.Printf("%v\n", err)
						}
					}
				}
				var cKey [common.HashLength + common.IncarnationLength + common.HashLength]byte
				copy(cKey[:], addrHash[:])
				binary.BigEndian.PutUint64(cKey[common.HashLength:], ^account.Incarnation)
				err = stateDb.Walk(dbutils.StorageBucket, cKey[:], 8*(common.HashLength+common.IncarnationLength), func(k, v []byte) (bool, error) {
					var kh common.Hash
					copy(kh[:], k[common.HashLength+common.IncarnationLength:])
					if _, ok := sm[kh]; !ok {
						fmt.Printf("Key: %x, dbValue: %x\n", kh, v)
						if err = stateDb.Delete(dbutils.StorageBucket, k); err != nil {
							fmt.Printf("%v\n", err)
						}
					}
					return true, nil
				})
				if err != nil {
					panic(err)
				}
			}
		}
	}
	if err != nil {
		panic(err)
	}
}
