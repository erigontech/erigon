package trie

import (
	"github.com/ledgerwatch/turbo-geth/common"
)

func expandKeyBin(hex []byte, nibble byte) []byte {
	return expandKeyHex(hex, nibble)
	/*
		fmt.Printf("expandKeyBin hex=%x nibble=%x\n", hex, nibble)
		if nibble == 16 {
			// doesn't differ there
			return expandKeyHex(hex, nibble)
		}
		result := make([]byte, len(hex)+4)
		copy(result, hex)
		writeHexNibbleToBinary(result[len(hex):], nibble)
		return result
	*/
}

// MakeBlockWitness constructs block witness from the given trie and the
// list of keys that need to be accessible in such witness
func (bwb *BlockWitnessBuilder) MakeBlockWitnessBin(t *BinaryTrie, rs *ResolveSet, codeMap map[common.Hash][]byte) error {
	hr := newHasher(false)
	defer returnHasherToPool(hr)
	return bwb.makeBlockWitness(t.root, []byte{}, rs, hr, true, codeMap, expandKeyBin)
}
