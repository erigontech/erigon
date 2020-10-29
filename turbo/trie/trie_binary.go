package trie

import (
	"github.com/ledgerwatch/turbo-geth/common"
)

// BinaryTrie is a binary trie represnentation.
// Extracted to simplify type checks, etc
type BinaryTrie Trie

func (b *BinaryTrie) Trie() *Trie {
	return (*Trie)(b)
}

// HexToBin transforms a hexary trie into binary representation
// (where the keys can only contain symbols [0,1])
func HexToBin(hexTrie *Trie) *BinaryTrie {
	binaryTrie := NewBinary(common.Hash{})
	transformSubTrie(hexTrie.root, []byte{}, binaryTrie, keyHexToBin)
	return (*BinaryTrie)(binaryTrie)
}

func keyHexToBin(hex []byte) []byte {
	binLen := len(hex) * 4
	if hex[len(hex)-1] == 16 {
		binLen -= 3
	}
	bin := make([]byte, binLen)

	qi := 0

	for _, h := range hex {
		qi += writeHexNibbleToBinary(bin[qi:], h)
	}

	return bin
}

func writeHexNibbleToBinary(buff []byte, nibble byte) int {
	written := 0
	if nibble == 16 {
		buff[written] = 16
		written++
	} else {
		for shift := 3; shift >= 0; shift-- {
			buff[written] = normalize(nibble & (1 << uint(shift)))
			written++
		}
	}

	return written
}

func normalize(n byte) byte {
	if n > 0 {
		return 1
	}
	return 0
}
