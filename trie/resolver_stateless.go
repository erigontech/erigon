package trie

import (
	"bytes"
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

	witnessReader := bytes.NewReader(serializedWitness)

	var prevReq *ResolveRequest
	requestIndex := 0

	for witnessReader.Len() > 0 {
		req := r.requests[requestIndex]
		if prevReq == nil ||
			!bytes.Equal(req.contract, prevReq.contract) ||
			!bytes.Equal(req.resolveHex[:req.resolvePos], prevReq.resolveHex[:prevReq.resolvePos]) {
			witness, err := NewWitnessFromReader(witnessReader, false /*trace*/)

			if err != nil {
				return err
			}

			trie, _, err := BuildTrieFromWitness(witness, false /*is-binary*/, false /*trace*/)
			if err != nil {
				return err
			}
			rootNode := trie.root
			rootHash := trie.Hash()

			err = r.hookFunction(req, rootNode, rootHash)
			if err != nil {
				return err
			}
			prevReq = req
		}
		requestIndex++
	}

	return nil
}
