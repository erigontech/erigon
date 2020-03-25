package trie

import (
	"bytes"
	"io"
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

func (r *ResolverStateless) RebuildTrie(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64) (int64, error) {
	serializedWitness, err := db.GetWitnessesForBlock(blockNr, trieLimit)
	if err != nil {
		return 0, err
	}

	witnessReader := bytes.NewReader(serializedWitness)
	if _, err := witnessReader.Seek(startPos, io.SeekStart); err != nil {
		return 0, err
	}

	var prevReq *ResolveRequest
	requestIndex := 0

	for witnessReader.Len() > 0 && requestIndex < len(r.requests) {
		req := r.requests[requestIndex]
		if prevReq == nil ||
			!bytes.Equal(req.contract, prevReq.contract) ||
			!bytes.Equal(req.resolveHex[:req.resolvePos], prevReq.resolveHex[:prevReq.resolvePos]) {
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

			err = r.hookFunction(req, rootNode, rootHash)
			if err != nil {
				return 0, err
			}
			prevReq = req
		}
		requestIndex++
	}

	bytesRead := int64(len(serializedWitness) - witnessReader.Len())
	return bytesRead, nil
}
