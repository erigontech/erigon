package rpctest

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
)

func Proofs(chaindata string, url string, block uint64) {
	fileName := "trie.txt"
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	tx, err1 := db.Begin(context.Background(), ethdb.RW)
	if err1 != nil {
		panic(err1)
	}
	defer tx.Rollback()

	var t *trie.Trie
	if _, errf := os.Stat(fileName); errf != nil {
		if os.IsNotExist(errf) {
			// Resolve 6 top levels of the accounts trie
			rl := trie.NewRetainList(6)
			loader := trie.NewFlatDBTrieLoader("checkRoots")
			if err := loader.Reset(rl, nil, nil, false); err != nil {
				panic(err)
			}
			root, err := loader.CalcTrieRoot(tx.(ethdb.HasTx).Tx().(ethdb.RwTx), []byte{}, nil)
			if err != nil {
				panic(err)
			}
			fmt.Printf("Resolved with hash: %x\n", root)
			f, err1 := os.Create(fileName)
			if err1 == nil {
				defer f.Close()
				t.Print(f)
			} else {
				panic(err1)
			}
			fmt.Printf("Saved trie to file\n")
		} else {
			panic(errf)
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
			err := tx.Walk(dbutils.PreimagePrefix, startKey[:], 4*len(diffKey), func(k, v []byte) (bool, error) {
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
			if err = post(client, url, fmt.Sprintf(template, account, block, reqID), &proof); err != nil {
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
