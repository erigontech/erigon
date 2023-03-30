package common

import (
	"hash"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"golang.org/x/crypto/sha3"
)

// keccakState wraps sha3.state. In addition to the usual hash methods, it also supports
// Read to get a variable amount of data from the hash state. Read is faster than Sum
// because it doesn't copy the internal state, but also modifies the internal state.
type keccakState interface {
	hash.Hash
	Read([]byte) (int, error)
}

type Hasher struct {
	Sha keccakState
}

var hashersPool = sync.Pool{
	New: func() any {
		return &Hasher{Sha: sha3.NewLegacyKeccak256().(keccakState)}
	},
}

func NewHasher() *Hasher {
	h := hashersPool.Get().(*Hasher)
	h.Sha.Reset()
	return h
}
func ReturnHasherToPool(h *Hasher) { hashersPool.Put(h) }

func HashData(data []byte) (libcommon.Hash, error) {
	h := NewHasher()
	defer ReturnHasherToPool(h)

	_, err := h.Sha.Write(data)
	if err != nil {
		return libcommon.Hash{}, err
	}

	var buf libcommon.Hash
	_, err = h.Sha.Read(buf[:])
	if err != nil {
		return libcommon.Hash{}, err
	}
	return buf, nil
}
