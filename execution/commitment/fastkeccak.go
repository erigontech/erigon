package commitment

import (
	keccak "github.com/Giulio2002/fastkeccak"
)

// fastKeccakState wraps fastkeccak.Hasher to implement the keccakState interface.
// It provides the same streaming hash interface but uses platform-optimized assembly
// (NEON SHA3 on arm64, unrolled permutation on amd64) for significantly faster hashing.
type fastKeccakState struct {
	h keccak.Hasher
}

func newFastKeccak() *fastKeccakState {
	return &fastKeccakState{}
}

func (f *fastKeccakState) Write(p []byte) (int, error) {
	f.h.Write(p)
	return len(p), nil
}

func (f *fastKeccakState) Sum(b []byte) []byte {
	hash := f.h.Sum256()
	return append(b, hash[:]...)
}

func (f *fastKeccakState) Reset() {
	f.h.Reset()
}

func (f *fastKeccakState) Size() int      { return 32 }
func (f *fastKeccakState) BlockSize() int { return 136 }

func (f *fastKeccakState) Read(p []byte) (int, error) {
	hash := f.h.Sum256()
	return copy(p, hash[:]), nil
}
