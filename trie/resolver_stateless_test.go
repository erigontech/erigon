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
		trie.Update(generateKey(i), generateValue(i))
	}
	return trie
}

func TestRebuildTrie(t *testing.T) {
	trie1 := buildTestTrie(0)
	trie2 := buildTestTrie(10)
	trie3 := buildTestTrie(100)

	w1, err := extractWitnessFromRootNode(trie1.root, 1, false, nil)
	if err != nil {
		t.Error(err)
	}

	w2, err := extractWitnessFromRootNode(trie2.root, 1, false, nil)
	if err != nil {
		t.Error(err)
	}

	w3, err := extractWitnessFromRootNode(trie3.root, 1, false, nil)
	if err != nil {
		t.Error(err)
	}

	var buff bytes.Buffer
	_, err = w1.WriteTo(&buff)
	if err != nil {
		t.Error(err)
	}

	err = buff.WriteByte(byte(OpNewTrie))
	if err != nil {
		t.Error(err)
	}

	_, err = w2.WriteTo(&buff)
	if err != nil {
		t.Error(err)
	}

	err = buff.WriteByte(byte(OpNewTrie))
	if err != nil {
		t.Error(err)
	}

	_, err = w3.WriteTo(&buff)
	if err != nil {
		t.Error(err)
	}

	storage := testWitnessStorage(buff.Bytes())

	resolvedTries := make([]*Trie, 3)

	currentTrie := 0

	req1 := trie1.NewResolveRequest(nil, []byte{0x01}, 1)
	req2 := trie2.NewResolveRequest(nil, []byte{0x02}, 1)
	req21 := trie2.NewResolveRequest(nil, []byte{0x02}, 1)
	req3 := trie3.NewResolveRequest(nil, []byte{0x03}, 1,)
	req31 := trie3.NewResolveRequest(nil, []byte{0x03}, 1)

	hookFunction := func(hookNibbles []byte, root node, rootHash common.Hash) error {
		trie := New(rootHash)
		trie.root = root
		resolvedTries[currentTrie] = trie
		currentTrie++
		return nil
	}

	// it should ignore duplicate resolve requests
	resolver := NewResolverStateless([]*ResolveRequest{req1, req2, req21}, hookFunction)

	pos, err := resolver.RebuildTrie(&storage, 1, 1, 0)
	if err != nil {
		t.Error(err)
	}

	// we also support partial resolution with continuation (for storage tries)
	// so basically we first resolve accounts, then storages separately
	// but we still want to keep one entry in a DB per block, so we store the last read position
	// and then use it as a start
	resolver = NewResolverStateless([]*ResolveRequest{req3, req31}, hookFunction)
	_, err = resolver.RebuildTrie(&storage, 1, 1, pos)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(resolvedTries[0].Hash().Bytes(), trie1.Hash().Bytes()) {
		t.Errorf("tries are different")
	}

	if !bytes.Equal(resolvedTries[1].Hash().Bytes(), trie2.Hash().Bytes()) {
		t.Errorf("tries are different")
	}

	if !bytes.Equal(resolvedTries[2].Hash().Bytes(), trie3.Hash().Bytes()) {
		t.Errorf("tries are different")
	}
}
