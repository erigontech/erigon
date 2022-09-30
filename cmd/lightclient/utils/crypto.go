package utils

import (
	"crypto/sha256"
	"hash"
	"sync"
)

var hasherPool = sync.Pool{
	New: func() interface{} {
		return sha256.New()
	},
}

func Keccak256(data []byte) [32]byte {
	h, ok := hasherPool.Get().(hash.Hash)
	if !ok {
		h = sha256.New()
	}
	defer hasherPool.Put(h)
	h.Reset()

	var b [32]byte

	h.Write(data)
	h.Sum(b[:0])

	return b
}
