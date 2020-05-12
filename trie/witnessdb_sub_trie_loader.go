package trie

import (
	"bytes"
	"io"
	"time"

	"github.com/ledgerwatch/turbo-geth/metrics"
)

var (
	trieWitnessDbSubTrieLoaderTimer = metrics.NewRegisteredTimer("trie/subtrieloader/witnessdb", nil)
)

type WitnessDbSubTrieLoader struct {
}

func NewWitnessDbSubTrieLoader() *WitnessDbSubTrieLoader {
	return &WitnessDbSubTrieLoader{}
}

func (wstl *WitnessDbSubTrieLoader) LoadSubTries(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64, count int) (SubTries, int64, error) {
	defer trieWitnessDbSubTrieLoaderTimer.UpdateSince(time.Now())

	serializedWitness, err := db.GetWitnessesForBlock(blockNr, trieLimit)
	if err != nil {
		return SubTries{}, 0, err
	}
	witnessReader := bytes.NewReader(serializedWitness)
	if _, err := witnessReader.Seek(startPos, io.SeekStart); err != nil {
		return SubTries{}, 0, err
	}
	var subTries SubTries
	for i := 0; i < count; i++ {
		witness, err := NewWitnessFromReader(witnessReader, false /*trace*/)
		if err != nil {
			return SubTries{}, 0, err
		}
		trie, err := BuildTrieFromWitness(witness, false /*is-binary*/, false /*trace*/)
		if err != nil {
			return SubTries{}, 0, err
		}
		rootNode := trie.root
		rootHash := trie.Hash()
		subTries.roots = append(subTries.roots, rootNode)
		subTries.Hashes = append(subTries.Hashes, rootHash)
	}
	bytesRead := int64(len(serializedWitness) - witnessReader.Len())
	return subTries, bytesRead, nil
}
