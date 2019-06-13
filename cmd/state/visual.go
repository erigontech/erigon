package main

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func visual() {
	value := []byte{}
	tr := trie.New(common.Hash{}, true)
	keys := []string{"cat", "dog", "bird", "snake", "rat", "pig", "canary, hog", "rtj", "fkjkdf", "kjdfkjg", "dfkjg"}
	var keyHashes [][]byte
	for _, key := range keys {
		keyHash := crypto.Keccak256([]byte(key))
		keyHashes = append(keyHashes, keyHash[:8])
	}
	for _, keyHash := range keyHashes {
		tr.Update(keyHash, value, 0)
	}

	filename := "visual_trie.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	trie.Visual(tr, keyHashes[0], f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}
