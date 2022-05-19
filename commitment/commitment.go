package commitment

import (
	"github.com/ledgerwatch/erigon-lib/common/length"
)

// Trie represents commitment variant.
type Trie interface {
	ProcessUpdates(plainKeys, hashedKeys [][]byte, updates []Update) (branchNodeUpdates map[string][]byte, err error)

	// RootHash produces root hash of the trie
	RootHash() (hash []byte, err error)

	// Variant returns commitment trie variant
	Variant() TrieVariant

	// Reset Drops everything from the trie
	Reset()

	ResetFns(
		branchFn func(prefix []byte) ([]byte, error),
		accountFn func(plainKey []byte, cell *Cell) error,
		storageFn func(plainKey []byte, cell *Cell) error,
	)

	// Makes trie more verbose
	SetTrace(bool)
}

type TrieVariant string

const (
	// HexPatriciaHashed used as default commitment approach
	VariantHexPatriciaTrie TrieVariant = "hex-patricia-hashed"
	// Experimental mode with binary key representation
	VariantBinPatriciaTrie TrieVariant = "bin-patricia-hashed"
)

func InitializeTrie(tv TrieVariant) Trie {
	switch tv {
	case VariantBinPatriciaTrie:
		return NewBinPatriciaHashed(length.Addr, nil, nil, nil)
	case VariantHexPatriciaTrie:
		fallthrough
	default:
		return NewHexPatriciaHashed(length.Addr, nil, nil, nil)
	}
}
