package trie

import (
	"bytes"
	"fmt"
)

type ResolverStateless struct {
	requests     []*ResolveRequest
	hookFunction hookFunction
}

func NewResolverStateless(requests []*ResolveRequest, hookFunction hookFunction) *ResolverStateless {
	return &ResolverStateless{
		requests:     requests,
		hookFunction: hookFunction,
	}
}

func (r *ResolverStateless) RebuildTrie(db WitnessStorage, blockNr uint64, trieLimit uint32) error {
	serializedWitness, err := db.GetWitnessesForBlock(blockNr, trieLimit)
	if err != nil {
		return err
	}

	fmt.Printf("serialized=%v\n", serializedWitness)

	witnessReader := bytes.NewReader(serializedWitness)

	for witnessReader.Len() > 0 {
		fmt.Printf("iteration N\n")

		witness, err := NewWitnessFromReader(witnessReader, true /*trace*/)

		fmt.Printf("witness = %+v, err = %v\n", witness, err)

		if err != nil {
			return err
		}

		trie, _, err := BuildTrieFromWitness(witness, false /*is-binary*/, false /*trace*/)
		if err != nil {
			return err
		}
		rootNode := trie.root
		rootHash := trie.Hash()
		/*FIXME: fix later*/
		err = r.hookFunction(nil, rootNode, rootHash)
		if err != nil {
			return err
		}
	}

	return nil
}
