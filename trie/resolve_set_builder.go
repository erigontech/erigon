package trie

import "github.com/ledgerwatch/turbo-geth/common"

// ResolveSetBuilder is the structure that accumulates the set of keys that were read or changes (touched) during
// the execution of a block. It also tracks the contract codes that were created and used during the execution
// of a block
type ResolveSetBuilder struct {
	touches        [][]byte                 // Read/change set of account keys (account hashes)
	storageTouches [][]byte                 // Read/change set of storage keys (account hashes concatenated with storage key hashes)
	proofCodes     map[common.Hash]struct{} // Contract codes that have been accessed (codeHash)
	createdCodes   map[common.Hash]struct{} // Contract codes that were created (deployed) (codeHash)
}

// NewResolveSetBuilder creates new ProofGenerator and initialised its maps
func NewResolveSetBuilder() *ResolveSetBuilder {
	return &ResolveSetBuilder{
		proofCodes:   make(map[common.Hash]struct{}),
		createdCodes: make(map[common.Hash]struct{}),
	}
}

// AddTouch adds a key (in KEY encoding) into the read/change set of account keys
func (pg *ResolveSetBuilder) AddTouch(touch []byte) {
	pg.touches = append(pg.touches, common.CopyBytes(touch))
}

// AddStorageTouch adds a key (in KEY encoding) into the read/change set of storage keys
func (pg *ResolveSetBuilder) AddStorageTouch(touch []byte) {
	pg.storageTouches = append(pg.storageTouches, common.CopyBytes(touch))
}

// ExtractTouches returns accumulated read/change sets and clears them for the next block's execution
func (pg *ResolveSetBuilder) ExtractTouches() ([][]byte, [][]byte) {
	touches := pg.touches
	storageTouches := pg.storageTouches
	pg.touches = nil
	pg.storageTouches = nil
	return touches, storageTouches
}

// extractCodeTouches returns the set of all contract codes that were required during the block's execution
// but were not created during that same block. It also clears the set for the next block's execution
func (pg *ResolveSetBuilder) extractCodeTouches() map[common.Hash]struct{} {
	proofCodes := pg.proofCodes
	pg.proofCodes = make(map[common.Hash]struct{})
	pg.createdCodes = make(map[common.Hash]struct{})
	return proofCodes
}

// ReadCode registers that given contract code has been accessed during current block's execution
func (pg *ResolveSetBuilder) ReadCode(codeHash common.Hash) {
	if _, ok := pg.proofCodes[codeHash]; !ok {
		pg.proofCodes[codeHash] = struct{}{}
	}
}

// CreateCode registers that given contract code has been created (deployed) during current block's execution
func (pg *ResolveSetBuilder) CreateCode(codeHash common.Hash) {
	if _, ok := pg.proofCodes[codeHash]; !ok {
		pg.createdCodes[codeHash] = struct{}{}
	}
}

func (pg *ResolveSetBuilder) Build(isBinary bool) *ResolveSet {
	var rs *ResolveSet
	if isBinary {
		rs = NewBinaryResolveSet(0)
	} else {
		rs = NewResolveSet(0)
	}

	touches, storageTouches := pg.ExtractTouches()
	codeTouches := pg.extractCodeTouches()

	for _, touch := range touches {
		rs.AddKey(touch)
	}
	for _, touch := range storageTouches {
		rs.AddKey(touch)
	}
	for codeHash, _ := range codeTouches {
		rs.AddCodeTouch(codeHash)
	}

	return rs
}
