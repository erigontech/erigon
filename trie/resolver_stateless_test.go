package trie

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
)

type testWitnessStorage []byte

func (s *testWitnessStorage) GetWitnessesForBlock(_ uint64, _ uint32) ([]byte, error) {
	return []byte(*s), nil
}

func buildTestTrie() *Trie {
	var trie Trie
	return &trie
}

func TestRebuildTrie(t *testing.T) {
	trie1 := buildTestTrie()
	trie2 := buildTestTrie()
	trie3 := buildTestTrie()

	w1, err := extractWitnessFromRootNode(trie1.root, 1, false, nil, nil)
	if err != nil {
		t.Error(err)
	}

	w2, err := extractWitnessFromRootNode(trie2.root, 1, false, nil, nil)
	if err != nil {
		t.Error(err)
	}

	w3, err := extractWitnessFromRootNode(trie3.root, 1, false, nil, nil)
	if err != nil {
		t.Error(err)
	}

	var buff bytes.Buffer
	w1.WriteTo(&buff)
	buff.WriteByte(byte(OpNewTrie))
	w2.WriteTo(&buff)
	buff.WriteByte(byte(OpNewTrie))
	w3.WriteTo(&buff)

	storage := testWitnessStorage(buff.Bytes())

	hookFunction := func(req *ResolveRequest, root node, rootHash common.Hash) error {
		fmt.Printf("rootNode=%v, rootHash=%s\n", root, rootHash.Hex())
		return nil
	}

	resolver := NewResolverStateless(make([]*ResolveRequest, 0), hookFunction)

	err = resolver.RebuildTrie(&storage, 1, 1)
	if err != nil {
		t.Error(err)
	}
}
