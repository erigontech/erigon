package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
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

func proofs(chaindata string, url string, block int, account common.Address) {
	fileName := "trie.txt"
	ethDb, err := ethdb.NewBoltDatabase(chaindata)
	if err != nil {
		panic(err)
	}
	defer ethDb.Close()
	var t *trie.Trie
	if _, err := os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			// Resolve 5 top levels of the accounts trie
			r := trie.NewResolver(5, true, uint64(block))
			t = trie.New(common.Hash{})
			req := t.NewResolveRequest(nil, []byte{}, 0, nil)
			r.AddRequest(req)
			err = r.ResolveWithDb(ethDb, uint64(block))
			if err != nil {
				panic(err)
			}
			fmt.Printf("Resoled with hash: %x\n", t.Hash())
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
	for _, p := range proof.Result.AccountProof {
		b := common.FromHex(p)
		h := crypto.Keccak256(b)
		fmt.Printf("%x: %s\n", h, p)
	}
}
