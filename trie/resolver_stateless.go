package trie

import "github.com/ledgerwatch/turbo-geth/ethdb"

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

func (r *ResolverStateless) RebuildTrie(db ethdb.Database, blockNr uint64) error {
	panic("not implemented")
}
