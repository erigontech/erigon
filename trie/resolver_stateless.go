package trie

import "bytes"

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
	witnessReader := bytes.NewReader(serializedWitness)

	for {
		witness, err := NewWitnessFromReader(witnessReader, false /*trace*/)
		if err != nil {
			return err
		}
		if witness == nil {
			break
		}

	}

	panic("not implemented")
}
