package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
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
	if _, err := os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			// Resolve 6 top levels of the accounts trie
			r := trie.NewResolver(6, true, uint64(block))
			t = trie.New(common.Hash{})
			req := t.NewResolveRequest(nil, []byte{}, 0, nil)
			r.AddRequest(req)
			err = r.ResolveWithDb(ethDb, uint64(block))
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
	diffKeys := [][]byte{[]byte{}}
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
			hE := t.HashOfHexKey(diffKey)
			if h != hE {
				fmt.Printf(":( key %x: %x %x adding nibbles: ", diffKey, h, hE)
				var branch [17][]byte
				err = rlp.DecodeBytes(b, &branch)
				if err != nil {
					fmt.Printf("Error decoding: %v\n", err)
				}
				// Expand keys further
				for nibble := byte(0); nibble < 16; nibble++ {
					newDiff := make([]byte, len(diffKey)+1)
					copy(newDiff, diffKey)
					newDiff[len(diffKey)] = nibble
					var proofHash common.Hash
					copy(proofHash[:], branch[nibble])
					newHash := t.HashOfHexKey(newDiff)
					if proofHash != newHash {
						newDiffKeys = append(newDiffKeys, newDiff)
						fmt.Printf("%x", nibble)
					}
				}
				fmt.Printf("\n")
			} else {
				fmt.Printf("MATCH key %x: %x %x\n", diffKey, h, hE)
			}
			/*
				for _, p := range proof.Result.AccountProof {
					var branch [17][]byte

					err = rlp.DecodeBytes(b, &branch)
					if err != nil {
						fmt.Printf("Error decoding: %v\n", err)
					}
					h := crypto.Keccak256(b)
					fmt.Printf("%x: %s\n", h, p)
				}
			*/
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
