package trie

import (
	"bytes"
	"io"
	"time"

	"github.com/ledgerwatch/turbo-geth/metrics"
)

var (
	trieResolveStatelessTimer = metrics.NewRegisteredTimer("trie/resolve/stateless", nil)
)

type ResolverStateless struct {
}

func NewResolverStateless() *ResolverStateless {
	return &ResolverStateless{}
}

func (r *ResolverStateless) RebuildTrie(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64, count int) (SubTries, int64, error) {
	defer trieResolveStatelessTimer.UpdateSince(time.Now())

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
