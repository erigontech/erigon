package trie

import (
	"bytes"
	"io"
	"time"

	"github.com/erigontech/erigon-lib/metrics"
)

var (
	trieWitnessDbSubTrieLoaderTimer = metrics.NewHistTimer("trie_subtrieloader_witnessdb")
)

type WitnessDbSubTrieLoader struct {
}

func NewWitnessDbSubTrieLoader() *WitnessDbSubTrieLoader {
	return &WitnessDbSubTrieLoader{}
}

func (wstl *WitnessDbSubTrieLoader) LoadSubTries(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64, count int) (SubTries, int64, error) {
	defer trieWitnessDbSubTrieLoaderTimer.ObserveDuration(time.Now())

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
		trie, err := BuildTrieFromWitness(witness, false /*trace*/)
		if err != nil {
			return SubTries{}, 0, err
		}
		rootNode := trie.RootNode
		rootHash := trie.Hash()
		subTries.roots = append(subTries.roots, rootNode)
		subTries.Hashes = append(subTries.Hashes, rootHash)
	}
	bytesRead := int64(len(serializedWitness) - witnessReader.Len())
	return subTries, bytesRead, nil
}
