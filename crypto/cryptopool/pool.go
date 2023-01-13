package cryptopool

import (
	"hash"
	"sync"

	"golang.org/x/crypto/sha3"
)

var pool = sync.Pool{
	New: func() interface{} {
		return sha3.NewLegacyKeccak256()
	},
}

func GetLegacyKeccak256() hash.Hash {
	h := pool.Get().(hash.Hash)
	h.Reset()
	return h
}
func ReturnLegacyKeccak256(h hash.Hash) { pool.Put(h) }
