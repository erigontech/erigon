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
	hookFunction hookFunction
}

func NewResolverStateless(hookFunction hookFunction) *ResolverStateless {
	return &ResolverStateless{
		hookFunction: hookFunction,
	}
}

func (r *ResolverStateless) RebuildTrie(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64, hooks [][]byte) (int64, error) {
	defer trieResolveStatelessTimer.UpdateSince(time.Now())

	serializedWitness, err := db.GetWitnessesForBlock(blockNr, trieLimit)
	if err != nil {
		return 0, err
	}
	witnessReader := bytes.NewReader(serializedWitness)
	if _, err := witnessReader.Seek(startPos, io.SeekStart); err != nil {
		return 0, err
	}
	for _, hookNibbles := range hooks {
		witness, err := NewWitnessFromReader(witnessReader, false /*trace*/)
		if err != nil {
			return 0, err
		}
		trie, err := BuildTrieFromWitness(witness, false /*is-binary*/, false /*trace*/)
		if err != nil {
			return 0, err
		}
		rootNode := trie.root
		rootHash := trie.Hash()
		err = r.hookFunction(hookNibbles, rootNode, rootHash)
		if err != nil {
			return 0, err
		}
	}
	bytesRead := int64(len(serializedWitness) - witnessReader.Len())
	return bytesRead, nil
}
