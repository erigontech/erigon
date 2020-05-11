package trie

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var emptyHash [32]byte

// SubTrie is a result of loading sub-trie from either flat db
// or witness db. It encapsulates sub-trie root (which is of the un-exported type `node`)
// If the loading is done for verification and testing purposes, then usually only
// sub-tree root hash would be queried
type SubTries struct {
	Hooks [][]byte // Attachment points for each returned sub-trie. These are in nibbles
	Hashes []common.Hash // Root hashes of the sub-tries
	roots []node   // Sub-tries
}

type ResolveFunc func(*Resolver, *ResolveSet, [][]byte, []int, [][]byte) (SubTries, error)

// Resolver looks up (resolves) some keys and corresponding values from a database.
// One resolver per trie (prefix).
// See also ResolveRequest in trie.go
type Resolver struct {
	blockNr          uint64
	codeRequests     []*ResolveRequestForCode
}

func NewResolver(blockNr uint64) *Resolver {
	tr := Resolver{
		codeRequests: []*ResolveRequestForCode{},
		blockNr:      blockNr,
	}
	return &tr
}

func (tr *Resolver) Reset(blockNr uint64) {
	tr.blockNr = blockNr
	tr.codeRequests = tr.codeRequests[:0]
}

// AddCodeRequest add a request for code resolution
func (tr *Resolver) AddCodeRequest(req *ResolveRequestForCode) {
	tr.codeRequests = append(tr.codeRequests, req)
}

// Various values of the account field set
const (
	AccountFieldNonceOnly     uint32 = 0x01
	AccountFieldBalanceOnly   uint32 = 0x02
	AccountFieldStorageOnly   uint32 = 0x04
	AccountFieldCodeOnly      uint32 = 0x08
	AccountFieldSSizeOnly     uint32 = 0x10
	AccountFieldSetNotAccount uint32 = 0x00
)

// ResolveWithDb resolves and hooks subtries using a state database.
func (tr *Resolver) ResolveWithDb(db ethdb.Database, blockNr uint64, rs *ResolveSet, dbPrefixes [][]byte, fixedbits []int, hooks [][]byte, trace bool) (SubTries, error) {
	return tr.ResolveStateful(db, rs, dbPrefixes, fixedbits, hooks, trace)
}

func (tr *Resolver) ResolveStateful(db ethdb.Database, rs *ResolveSet, dbPrefixes [][]byte, fixedbits []int, hooks [][]byte, trace bool) (SubTries, error) {
	resolver := NewResolverStateful()
	subTries, err := resolver.RebuildTrie(db, rs, dbPrefixes, fixedbits, hooks, trace)
	if err != nil {
		return subTries, err
	}
	if err = resolver.AttachRequestedCode(db, tr.codeRequests); err != nil {
		return subTries, err
	}
	return subTries, nil
}

// ResolveStateless resolves and hooks subtries using a witnesses database instead of
// the state DB.
func (tr *Resolver) ResolveStateless(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64, hooks [][]byte) (SubTries, int64, error) {
	resolver := NewResolverStateless()
	// we expect CodeNodes to be already attached to the trie in stateless resolution
	return resolver.RebuildTrie(db, blockNr, trieLimit, startPos, hooks)
}
