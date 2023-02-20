package cryptopool

import (
	"hash"
	"sync"

	"golang.org/x/crypto/sha3"
)

// hasherPool holds LegacyKeccak hashers.
var hasherPool = sync.Pool{
	New: func() interface{} {
		return sha3.NewLegacyKeccak256()
	},
}

func NewLegacyKeccak256() hash.Hash {
	h := hasherPool.Get().(hash.Hash)
	h.Reset()
	return h
}
func ReturnToPoolKeccak256(h hash.Hash) { hasherPool.Put(h) }
