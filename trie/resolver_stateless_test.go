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

func generateKey(i int) []byte {
	return []byte(fmt.Sprintf("key-number-%05d", i))
}

func generateValue(i int) []byte {
	return []byte(fmt.Sprintf("value-number-%05d", i))
}

func buildTestTrie(numberOfNodes int) *Trie {
	trie := New(EmptyRoot)
	for i := 0; i < numberOfNodes; i++ {
		trie.Update(generateKey(i), generateValue(i), 1)
	}
	return trie
}

func TestRebuildTrie(t *testing.T) {
	trie1 := buildTestTrie(0)
	trie2 := buildTestTrie(10)
	trie3 := buildTestTrie(100)

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

	resolvedTries := make([]*Trie, 3)

	currentTrie := 0

	req1 := trie1.NewResolveRequest(nil, []byte{0x01}, 1, trie1.Hash().Bytes())
	req2 := trie2.NewResolveRequest(nil, []byte{0x02}, 1, trie2.Hash().Bytes())
	req21 := trie2.NewResolveRequest(nil, []byte{0x02}, 1, trie2.Hash().Bytes())
	req3 := trie3.NewResolveRequest(nil, []byte{0x03}, 1, trie3.Hash().Bytes())
	req31 := trie3.NewResolveRequest(nil, []byte{0x03}, 1, trie3.Hash().Bytes())

	hookFunction := func(req *ResolveRequest, root node, rootHash common.Hash) error {
		trie := New(rootHash)
		trie.root = root
		resolvedTries[currentTrie] = trie
		currentTrie++
		if !bytes.Equal(req.resolveHash.hash(), rootHash.Bytes()) {
			return fmt.Errorf("root hash mismatch: expected %x got %x\n",
				req.resolveHash.hash(), rootHash.Bytes())
		}
		return nil
	}

	// it should ignore duplicate resolve requests
	resolver := NewResolverStateless([]*ResolveRequest{req1, req2, req21, req3, req31}, hookFunction)

	err = resolver.RebuildTrie(&storage, 1, 1)
	if err != nil {
		t.Error(err)
	}

	var diff bytes.Buffer
	resolvedTries[0].PrintDiff(trie1, &diff)
	if diff.Len() > 0 {
		fmt.Errorf("tries are different: %s\n", string(diff.Bytes()))
	}

	resolvedTries[1].PrintDiff(trie2, &diff)
	if diff.Len() > 0 {
		fmt.Errorf("tries are different: %s\n", string(diff.Bytes()))
	}

	resolvedTries[2].PrintDiff(trie3, &diff)
	if diff.Len() > 0 {
		fmt.Errorf("tries are different: %s\n", string(diff.Bytes()))
	}
}
