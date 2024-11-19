package trie

import (
	"bytes"
	"fmt"
	"testing"
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

	w1, err := extractWitnessFromRootNode(trie1.RootNode, false, nil)
	if err != nil {
		t.Error(err)
	}

	w2, err := extractWitnessFromRootNode(trie2.RootNode, false, nil)
	if err != nil {
		t.Error(err)
	}

	w3, err := extractWitnessFromRootNode(trie3.RootNode, false, nil)
	if err != nil {
		t.Error(err)
	}

	var buff bytes.Buffer
	_, err = w1.WriteInto(&buff)
	if err != nil {
		t.Error(err)
	}

	err = buff.WriteByte(byte(OpNewTrie))
	if err != nil {
		t.Error(err)
	}

	_, err = w2.WriteInto(&buff)
	if err != nil {
		t.Error(err)
	}

	err = buff.WriteByte(byte(OpNewTrie))
	if err != nil {
		t.Error(err)
	}

	_, err = w3.WriteInto(&buff)
	if err != nil {
		t.Error(err)
	}

	storage := testWitnessStorage(buff.Bytes())

	loadedTries := make([]*Trie, 3)

	// it should ignore duplicate loaded requests
	loader := NewWitnessDbSubTrieLoader()

	subTries, pos, err := loader.LoadSubTries(&storage, 1, 1, 0, 2)
	if err != nil {
		t.Error(err)
	}
	currentTrie := 0
	for i, root := range subTries.roots {
		tr := New(subTries.Hashes[i])
		tr.RootNode = root
		loadedTries[currentTrie] = tr
		currentTrie++
	}

	// we also support partial resolution with continuation (for storage tries)
	// so basically we first load accounts, then storages separately
	// but we still want to keep one entry in a DB per block, so we store the last read position
	// and then use it as a start
	loader = NewWitnessDbSubTrieLoader()
	subTries, _, err = loader.LoadSubTries(&storage, 1, 1, pos, 1)
	if err != nil {
		t.Error(err)
	}
	for i, root := range subTries.roots {
		tr := New(subTries.Hashes[i])
		tr.RootNode = root
		loadedTries[currentTrie] = tr
		currentTrie++
	}

	if !bytes.Equal(loadedTries[0].Hash().Bytes(), trie1.Hash().Bytes()) {
		t.Errorf("tries are different")
	}

	if !bytes.Equal(loadedTries[1].Hash().Bytes(), trie2.Hash().Bytes()) {
		t.Errorf("tries are different")
	}

	if !bytes.Equal(loadedTries[2].Hash().Bytes(), trie3.Hash().Bytes()) {
		t.Errorf("tries are different")
	}
}
