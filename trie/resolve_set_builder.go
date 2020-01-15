package trie

import "github.com/ledgerwatch/turbo-geth/common"

// ResolveSetBuilder is the structure that accumulates the set of keys that were read or changes (touched) during
// the execution of a block. It also tracks the contract codes that were created and used during the execution
// of a block
type ResolveSetBuilder struct {
	touches        [][]byte               // Read/change set of account keys (account hashes)
	storageTouches [][]byte               // Read/change set of storage keys (account hashes concatenated with storage key hashes)
	proofCodes     map[common.Hash][]byte // Contract codes that have been accessed
	createdCodes   map[common.Hash][]byte // Contract codes that were created (deployed)
}

// NewResolveSetBuilder creates new ProofGenerator and initialised its maps
func NewResolveSetBuilder() *ResolveSetBuilder {
	return &ResolveSetBuilder{
		proofCodes:   make(map[common.Hash][]byte),
		createdCodes: make(map[common.Hash][]byte),
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

// extractCodeMap returns the map of all contract codes that were required during the block's execution
// but were not created during that same block. It also clears the maps for the next block's execution
func (pg *ResolveSetBuilder) extractCodeMap() map[common.Hash][]byte {
	proofCodes := pg.proofCodes
	pg.proofCodes = make(map[common.Hash][]byte)
	pg.createdCodes = make(map[common.Hash][]byte)
	return proofCodes
}

// ReadCode registers that given contract code has been accessed during current block's execution
func (pg *ResolveSetBuilder) ReadCode(codeHash common.Hash, code []byte) {
	if _, ok := pg.createdCodes[codeHash]; !ok {
		pg.proofCodes[codeHash] = code
	}
}

// CreateCode registers that given contract code has been created (deployed) during current block's execution
func (pg *ResolveSetBuilder) CreateCode(codeHash common.Hash, code []byte) {
	if _, ok := pg.proofCodes[codeHash]; !ok {
		pg.createdCodes[codeHash] = code
	}
}

func (pg *ResolveSetBuilder) Build(isBinary bool) (*ResolveSet, CodeMap) {
	var rs *ResolveSet
	if isBinary {
		rs = NewBinaryResolveSet(0)
	} else {
		rs = NewResolveSet(0)
	}

	touches, storageTouches := pg.ExtractTouches()

	for _, touch := range touches {
		rs.AddKey(touch)
	}
	for _, touch := range storageTouches {
		rs.AddKey(touch)
	}
	codeMap := pg.extractCodeMap()
	return rs, codeMap
}
