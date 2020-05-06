package trie

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

var emptyHash [32]byte

type ResolveFunc func(*Resolver) error

// Resolver looks up (resolves) some keys and corresponding values from a database.
// One resolver per trie (prefix).
// See also ResolveRequest in trie.go
type Resolver struct {
	historical       bool
	collectWitnesses bool // if true, stores witnesses for all the subtries that are being resolved
	blockNr          uint64
	requests         []*ResolveRequest
	codeRequests     []*ResolveRequestForCode
	witnesses        []*Witness // list of witnesses for resolved subtries, nil if `collectWitnesses` is false
}

func NewResolver(blockNr uint64) *Resolver {
	tr := Resolver{
		requests:     []*ResolveRequest{},
		codeRequests: []*ResolveRequestForCode{},
		blockNr:      blockNr,
	}
	return &tr
}

func (tr *Resolver) Reset(blockNr uint64) {
	tr.blockNr = blockNr
	tr.requests = tr.requests[:0]
	tr.codeRequests = tr.codeRequests[:0]
	tr.witnesses = nil
	tr.collectWitnesses = false
	tr.historical = false
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

func (tr *Resolver) SetHistorical(h bool) {
	tr.historical = h
}

// AddCodeRequest add a request for code resolution
func (tr *Resolver) AddCodeRequest(req *ResolveRequestForCode) {
	tr.codeRequests = append(tr.codeRequests, req)
}

// Resolver implements sort.Interface
// and sorts by resolve requests
// (more general requests come first)
func (tr *Resolver) Len() int {
	return len(tr.requests)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (tr *Resolver) Less(i, j int) bool {
	ci := tr.requests[i]
	cj := tr.requests[j]
	if ci.contract == nil && cj.contract != nil {
		contractAsNibbles := pool.GetBuffer(256)
		defer pool.PutBuffer(contractAsNibbles)
		DecompressNibbles(cj.contract[:common.HashLength], &contractAsNibbles.B)
		hex1 := ci.resolveHex
		if len(hex1) == 65 {
			hex1 = hex1[:64]
		}
		c := bytes.Compare(hex1[:ci.resolvePos], contractAsNibbles.B[:ci.resolvePos])
		if c != 0 {
			return c < 0
		}
	} else if ci.contract != nil && cj.contract == nil {
		contractAsNibbles := pool.GetBuffer(256)
		defer pool.PutBuffer(contractAsNibbles)
		DecompressNibbles(ci.contract[:common.HashLength], &contractAsNibbles.B)
		hex1 := cj.resolveHex
		if len(hex1) == 65 {
			hex1 = hex1[:64]
		}
		c := bytes.Compare(contractAsNibbles.B[:cj.resolvePos], hex1[:cj.resolvePos])
		if c != 0 {
			return c < 0
		}
	}

	m := min(ci.resolvePos, cj.resolvePos)
	c := bytes.Compare(ci.contract, cj.contract)
	if c != 0 {
		return c < 0
	}
	c = bytes.Compare(ci.resolveHex[:m], cj.resolveHex[:m])
	if c != 0 {
		return c < 0
	}
	return ci.resolvePos < cj.resolvePos
}

func (tr *Resolver) Swap(i, j int) {
	tr.requests[i], tr.requests[j] = tr.requests[j], tr.requests[i]
}

func (tr *Resolver) AddRequest(req *ResolveRequest) {
	tr.requests = append(tr.requests, req)
}

func (tr *Resolver) Print() {
	for _, req := range tr.requests {
		fmt.Printf("%s\n", req.String())
	}
}

// Various values of the account field set
const (
	AccountFieldNonceOnly           uint32 = 0x01
	AccountFieldBalanceOnly         uint32 = 0x02
	AccountFieldRootOnly            uint32 = 0x04
	AccountFieldCodeHashOnly        uint32 = 0x08
	AccountFieldSSizeOnly           uint32 = 0x10
	AccountFieldSetNotAccount       uint32 = 0x00
	AccountFieldSetNotContract      uint32 = 0x03 // Bit 0 is set for nonce, bit 1 is set for balance
	AccountFieldSetContract         uint32 = 0x0f // Bits 0-3 are set for nonce, balance, storageRoot and codeHash
	AccountFieldSetContractWithSize uint32 = 0x1f // Bits 0-4 are set for nonce, balance, storageRoot, codeHash and storageSize
)

// ResolveWithDb resolves and hooks subtries using a state database.
func (tr *Resolver) ResolveWithDb(db ethdb.Database, blockNr uint64, trace bool) error {
	return tr.ResolveStateful(db, blockNr, trace)
}

func (tr *Resolver) ResolveStateful(db ethdb.Database, blockNr uint64, trace bool) error {
	var hf hookFunction
	if tr.collectWitnesses {
		hf = tr.extractWitnessAndHookSubtrie
	} else {
		hf = hookSubtrie
	}

	sort.Stable(tr)
	resolver := NewResolverStateful(tr.requests, hf)
	if err := resolver.RebuildTrie(db, blockNr, tr.historical, trace); err != nil {
		return err
	}
	return resolver.AttachRequestedCode(db, tr.codeRequests)
}

// ResolveStateless resolves and hooks subtries using a witnesses database instead of
// the state DB.
func (tr *Resolver) ResolveStateless(db WitnessStorage, blockNr uint64, trieLimit uint32, startPos int64) (int64, error) {
	sort.Stable(tr)
	resolver := NewResolverStateless(tr.requests, hookSubtrie)
	// we expect CodeNodes to be already attached to the trie in stateless resolution
	return resolver.RebuildTrie(db, blockNr, trieLimit, startPos)
}

func hookSubtrie(currentReq *ResolveRequest, hbRoot node, hbHash common.Hash) error {
	if currentReq.RequiresRLP {
		hasher := newHasher(false)
		defer returnHasherToPool(hasher)
		h, err := hasher.hashChildren(hbRoot, 0)
		if err != nil {
			return err
		}
		currentReq.NodeRLP = h
	}

	var hookKey []byte
	if currentReq.contract == nil {
		hookKey = currentReq.resolveHex[:currentReq.resolvePos]
	} else {
		contractHex := keybytesToHex(currentReq.contract)
		if len(currentReq.contract) == 32 {
			contractHex = contractHex[:len(contractHex)-1] // No incarnation in contract
		} else {
			contractHex = contractHex[:len(contractHex)-1-16] // Remove terminal nibble and incarnation bytes
		}
		hookKey = append(contractHex, currentReq.resolveHex[:currentReq.resolvePos]...)
	}

	currentReq.t.hook(hookKey, hbRoot)
	if len(currentReq.resolveHash) > 0 && !bytes.Equal(currentReq.resolveHash, hbHash[:]) {
		return fmt.Errorf("mismatching hash: %s %x for prefix %x, resolveHex %x, resolvePos %d",
			currentReq.resolveHash, hbHash, currentReq.contract, currentReq.resolveHex, currentReq.resolvePos)
	}

	return nil
}

func (tr *Resolver) extractWitnessAndHookSubtrie(currentReq *ResolveRequest, hbRoot node, hbHash common.Hash) error {
	if tr.witnesses == nil {
		tr.witnesses = make([]*Witness, 0)
	}

	witness, err := extractWitnessFromRootNode(hbRoot, tr.blockNr, false /*tr.hb.trace*/, nil)
	if err != nil {
		return fmt.Errorf("error while extracting witness for resolver: %w", err)
	}

	tr.witnesses = append(tr.witnesses, witness)

	return hookSubtrie(currentReq, hbRoot, hbHash)
}
