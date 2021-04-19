package common

import (
	"fmt"
	"hash"

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

var hasherPool = make(chan *Hasher, 128)

func NewHasher() *Hasher {
	var h *Hasher
	select {
	case h = <-hasherPool:
	default:
		h = &Hasher{Sha: sha3.NewLegacyKeccak256().(keccakState)}
	}
	return h
}

func ReturnHasherToPool(h *Hasher) {
	select {
	case hasherPool <- h:
	default:
		fmt.Printf("Allowing Hasher to be garbage collected, pool is full\n")
	}
}

func HashData(data []byte) (Hash, error) {
	h := NewHasher()
	defer ReturnHasherToPool(h)
	h.Sha.Reset()

	_, err := h.Sha.Write(data)
	if err != nil {
		return Hash{}, err
	}

	var buf Hash
	_, err = h.Sha.Read(buf[:])
	if err != nil {
		return Hash{}, err
	}
	return buf, nil
}
