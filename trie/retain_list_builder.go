package trie

import "github.com/ledgerwatch/turbo-geth/common"

// RetainListBuilder is the structure that accumulates the list of keys that were read or changes (touched) during
// the execution of a block. It also tracks the contract codes that were created and used during the execution
// of a block
type RetainListBuilder struct {
	touches        [][]byte                 // Read/change set of account keys (account hashes)
	storageTouches [][]byte                 // Read/change set of storage keys (account hashes concatenated with storage key hashes)
	proofCodes     map[common.Hash]struct{} // Contract codes that have been accessed (codeHash)
	createdCodes   map[common.Hash]struct{} // Contract codes that were created (deployed) (codeHash)
}

// NewRetainListBuilder creates new ProofGenerator and initialised its maps
func NewRetainListBuilder() *RetainListBuilder {
	return &RetainListBuilder{
		proofCodes:   make(map[common.Hash]struct{}),
		createdCodes: make(map[common.Hash]struct{}),
	}
}

// AddTouch adds a key (in KEY encoding) into the read/change set of account keys
func (rlb *RetainListBuilder) AddTouch(touch []byte) {
	rlb.touches = append(rlb.touches, common.CopyBytes(touch))
}

// AddStorageTouch adds a key (in KEY encoding) into the read/change set of storage keys
func (rlb *RetainListBuilder) AddStorageTouch(touch []byte) {
	rlb.storageTouches = append(rlb.storageTouches, common.CopyBytes(touch))
}

// ExtractTouches returns accumulated read/change sets and clears them for the next block's execution
func (rlb *RetainListBuilder) ExtractTouches() ([][]byte, [][]byte) {
	touches := rlb.touches
	storageTouches := rlb.storageTouches
	rlb.touches = nil
	rlb.storageTouches = nil
	return touches, storageTouches
}

// extractCodeTouches returns the set of all contract codes that were required during the block's execution
// but were not created during that same block. It also clears the set for the next block's execution
func (rlb *RetainListBuilder) extractCodeTouches() map[common.Hash]struct{} {
	proofCodes := rlb.proofCodes
	rlb.proofCodes = make(map[common.Hash]struct{})
	rlb.createdCodes = make(map[common.Hash]struct{})
	return proofCodes
}

// ReadCode registers that given contract code has been accessed during current block's execution
func (rlb *RetainListBuilder) ReadCode(codeHash common.Hash) {
	if _, ok := rlb.proofCodes[codeHash]; !ok {
		rlb.proofCodes[codeHash] = struct{}{}
	}
}

// CreateCode registers that given contract code has been created (deployed) during current block's execution
func (rlb *RetainListBuilder) CreateCode(codeHash common.Hash) {
	if _, ok := rlb.proofCodes[codeHash]; !ok {
		rlb.createdCodes[codeHash] = struct{}{}
	}
}

func (rlb *RetainListBuilder) Build(isBinary bool) *RetainList {
	var rl *RetainList
	if isBinary {
		rl = NewBinaryRetainList(0)
	} else {
		rl = NewRetainList(0)
	}

	touches, storageTouches := rlb.ExtractTouches()
	codeTouches := rlb.extractCodeTouches()

	for _, touch := range touches {
		rl.AddKey(touch)
	}
	for _, touch := range storageTouches {
		rl.AddKey(touch)
	}
	for codeHash := range codeTouches {
		rl.AddCodeTouch(codeHash)
	}

	return rl
}
