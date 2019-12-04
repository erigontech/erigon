package trie

import (
	"github.com/ledgerwatch/turbo-geth/common"
)

func expandKeyBin(hex []byte, nibble byte) []byte {
	// We actually will never receive anything except [0_1] in `hex`, so we can just
	// reuse the existing functinality.
	return expandKeyHex(hex, nibble)
}

// MakeBlockWitness constructs block witness from the given trie and the
// list of keys that need to be accessible in such witness
func (bwb *BlockWitnessBuilder) MakeBlockWitnessBin(t *BinaryTrie, rs *ResolveSet, codeMap map[common.Hash][]byte) error {
	hr := newHasher(false)
	defer returnHasherToPool(hr)
	return bwb.makeBlockWitness(t.root, []byte{}, rs, hr, true, codeMap, expandKeyBin)
}
