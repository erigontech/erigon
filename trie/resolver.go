package trie

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var emptyHash [32]byte

type ResolveFunc func(*Resolver, *ResolveSet, [][]byte, []int, [][]byte) error

// Resolver looks up (resolves) some keys and corresponding values from a database.
// One resolver per trie (prefix).
// See also ResolveRequest in trie.go
type Resolver struct {
	t                *Trie
	collectWitnesses bool // if true, stores witnesses for all the subtries that are being resolved
	blockNr          uint64
	codeRequests     []*ResolveRequestForCode
	witnesses        []*Witness // list of witnesses for resolved subtries, nil if `collectWitnesses` is false
}

func NewResolver(t *Trie, blockNr uint64) *Resolver {
	tr := Resolver{
		t:            t,
		codeRequests: []*ResolveRequestForCode{},
		blockNr:      blockNr,
	}
	return &tr
}

func (tr *Resolver) Reset(blockNr uint64) {
	tr.blockNr = blockNr
	tr.codeRequests = tr.codeRequests[:0]
	tr.witnesses = nil
	tr.collectWitnesses = false
}

func (tr *Resolver) CollectWitnesses(c bool) {
	tr.collectWitnesses = c
}

// PopCollectedWitnesses returns all the collected witnesses and clears the storage in this resolver
func (tr *Resolver) PopCollectedWitnesses() []*Witness {
	result := tr.witnesses
	tr.witnesses = nil
	return result
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
func (tr *Resolver) ResolveWithDb(db ethdb.Database, blockNr uint64, rs *ResolveSet, dbPrefixes [][]byte, fixedbits []int, hooks [][]byte, trace bool) error {
	return tr.ResolveStateful(db, rs, dbPrefixes, fixedbits, hooks, trace)
}

func (tr *Resolver) ResolveStateful(db ethdb.Database, rs *ResolveSet, dbPrefixes [][]byte, fixedbits []int, hooks [][]byte, trace bool) error {
	var hf hookFunction
	if tr.collectWitnesses {
		hf = tr.extractWitnessAndHookSubtrie
	} else {
		hf = tr.hookSubtrie
	}

	resolver := NewResolverStateful(hf)
	if err := resolver.RebuildTrie(db, rs, dbPrefixes, fixedbits, hooks, trace); err != nil {
		return err
	}
	return resolver.AttachRequestedCode(db, tr.codeRequests)
}

// ResolveStateless resolves and hooks subtries using a witnesses database instead of
// the state DB.
func (tr *Resolver) ResolveStateless(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64, hooks [][]byte) (int64, error) {
	resolver := NewResolverStateless(tr.hookSubtrie)
	// we expect CodeNodes to be already attached to the trie in stateless resolution
	return resolver.RebuildTrie(db, blockNr, trieLimit, startPos, hooks)
}

func (tr *Resolver) hookSubtrie(hookNibbles []byte, hbRoot node, hbHash common.Hash) error {
	return tr.t.hook(hookNibbles, hbRoot, hbHash[:])
}

func (tr *Resolver) extractWitnessAndHookSubtrie(hookNibbles []byte, hbRoot node, hbHash common.Hash) error {
	if tr.witnesses == nil {
		tr.witnesses = make([]*Witness, 0)
	}

	witness, err := extractWitnessFromRootNode(hbRoot, tr.blockNr, false /*tr.hb.trace*/, nil)
	if err != nil {
		return fmt.Errorf("error while extracting witness for resolver: %w", err)
	}

	tr.witnesses = append(tr.witnesses, witness)

	return tr.hookSubtrie(hookNibbles, hbRoot, hbHash)
}
