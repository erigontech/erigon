package merkle_tree

import (
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/prysmaticlabs/gohashtree"
)

var globalHasher *merkleHasher

const initialBufferSize = 0 // it is whatever

// merkleHasher is used internally to provide shared buffer internally to the merkle_tree package.
type merkleHasher struct {
	// internalBuffer is the shared buffer we use for each operation
	internalBuffer [][32]byte
	// mu is the lock to ensure thread safety
	mu sync.Mutex
}

func newMerkleHasher() *merkleHasher {
	return &merkleHasher{
		internalBuffer: make([][32]byte, initialBufferSize),
	}
}

// merkleizeTrieLeaves returns intermediate roots of given leaves.
func (m *merkleHasher) merkleizeTrieLeaves(leaves [][32]byte) ([32]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	layer := m.getBuffer(len(leaves) / 2)
	for len(leaves) > 1 {
		if !utils.IsPowerOf2(uint64(len(leaves))) {
			return [32]byte{}, fmt.Errorf("hash layer is a non power of 2: %d", len(leaves))
		}
		if err := gohashtree.Hash(layer, leaves); err != nil {
			return [32]byte{}, err
		}
		leaves = layer[:len(leaves)/2]
	}
	return leaves[0], nil
}

// getBuffer provides buffer of given size.
func (m *merkleHasher) getBuffer(size int) [][32]byte {
	if size > len(m.internalBuffer) {
		m.internalBuffer = make([][32]byte, size*2)
	}
	return m.internalBuffer[:size]
}
