package merkle_tree

import (
	"bytes"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
)

const OptimalMaxTreeCacheDepth = 12

type MerkleTree struct {
	computeLeaf func(idx int, out []byte)
	layers      [][]byte // Flat hash-layers
	leavesCount int

	hashBuf [64]byte // buffer to store the input for hash(hash1, hash2)
	limit   *uint64  // Optional limit for the number of leaves (this will enable limit-oriented hashing)
}

// Layout of the layers:

// 0: leaves
// ... intermediate layers
// Root is not stored in the layers, rootrecomputed on demand

// Initialize initializes the Merkle tree with the given number of leaves and the maximum depth of the tree cache.
func (m *MerkleTree) Initialize(leavesCount, maxTreeCacheDepth int, computeLeaf func(idx int, out []byte), limitOptional *uint64) {
	m.computeLeaf = computeLeaf
	m.layers = make([][]byte, maxTreeCacheDepth)
	m.leavesCount = leavesCount
	firstLayerSize := leavesCount * length.Hash
	capacity := (firstLayerSize / 2) * 3
	m.layers[0] = make([]byte, firstLayerSize, capacity)
	if limitOptional != nil {
		m.limit = new(uint64)
		*m.limit = *limitOptional
	}
}

// MarkLeafAsDirty resets the leaf at the given index, so that it will be recomputed on the next call to ComputeRoot.
func (m *MerkleTree) MarkLeafAsDirty(idx int) {
	for i := 0; i < len(m.layers); i++ {
		currDivisor := 1 << i
		layerSize := (int(m.leavesCount) + (currDivisor - 1)) / currDivisor
		if layerSize == 0 {
			break
		}
		if m.layers[i] == nil {
			capacity := (layerSize / 2) * 3
			if capacity == 0 {
				capacity = 1024
			}
			m.layers[i] = make([]byte, layerSize, capacity)
		}
		copy(m.layers[i][(idx/currDivisor)*length.Hash:], ZeroHashes[0][:])
		if layerSize == 1 {
			break
		}
	}
}

func (m *MerkleTree) AppendLeaf() {
	/*
		Step 1: Append a new dirty leaf
		Step 2: Extend each layer with the new leaf when needed (1.5x extension)
	*/
	for i := 0; i < len(m.layers); i++ {
		m.extendLayer(i)
	}
	m.leavesCount++
}

// extendLayer extends the layer with the given index by 1.5x, by marking the new leaf as dirty.
func (m *MerkleTree) extendLayer(layerIdx int) {
	if layerIdx == 0 {
		m.layers[0] = append(m.layers[0], ZeroHashes[0][:]...)
		return
	}
	// find previous layer nodes count and round  to the next power of 2
	prevLayerNodeCount := len(m.layers[layerIdx-1]) / length.Hash
	newExpectendLayerNodeCount := prevLayerNodeCount / 2
	if newExpectendLayerNodeCount == 0 {
		m.layers[layerIdx] = m.layers[layerIdx][:0]
		return
	}
	if prevLayerNodeCount%2 != 0 {
		newExpectendLayerNodeCount++
	}

	newLayerSize := newExpectendLayerNodeCount * length.Hash

	if m.layers[layerIdx] == nil {
		capacity := (newLayerSize / 2) * 3
		m.layers[layerIdx] = make([]byte, newLayerSize, capacity)
	} else {
		if newLayerSize > cap(m.layers[layerIdx]) {
			capacity := (newLayerSize / 2) * 3
			tmp := m.layers[layerIdx]
			m.layers[layerIdx] = make([]byte, newLayerSize, capacity)
			copy(m.layers[layerIdx], tmp)
		}
		m.layers[layerIdx] = m.layers[layerIdx][:newLayerSize]
		copy(m.layers[layerIdx][newLayerSize-length.Hash:], ZeroHashes[0][:])
	}
}

// ComputeRoot computes the root of the Merkle tree.
func (m *MerkleTree) ComputeRoot() libcommon.Hash {
	var root libcommon.Hash
	if len(m.layers) == 0 {
		return ZeroHashes[0]
	}
	if len(m.layers[0]) == 0 {
		if m.limit == nil {
			return ZeroHashes[0]
		}
		return ZeroHashes[GetDepth(*m.limit)]
	}

	// Compute the root
	for i := 0; i < len(m.layers); i++ {
		m.computeLayer(i)
	}
	// Find last layer with more than 0 elements
	for i := 0; i < len(m.layers); i++ {
		if len(m.layers[i]) == 0 {
			m.finishHashing(i-1, root[:])
			return root
		}
	}
	m.finishHashing(len(m.layers)-1, root[:])
	return root
}

func (m *MerkleTree) finishHashing(lastLayerIdx int, root []byte) {
	if m.limit == nil {
		if err := MerkleRootFromFlatFromIntermediateLevel(m.layers[lastLayerIdx], root[:], len(m.layers[0]), lastLayerIdx); err != nil {
			panic(err)
		}
		return
	}

	if err := MerkleRootFromFlatFromIntermediateLevelWithLimit(m.layers[lastLayerIdx], root[:], int(*m.limit), lastLayerIdx); err != nil {
		panic(err)
	}
}

func (m *MerkleTree) computeLayer(layerIdx int) {
	currentDivisor := 1 << uint(layerIdx)
	if m.layers[layerIdx] == nil {
		// find previous layer nodes count and round  to the next power of 2
		prevLayerNodeCount := len(m.layers[layerIdx-1]) / length.Hash
		newExpectendLayerNodeCount := prevLayerNodeCount / 2
		if newExpectendLayerNodeCount == 0 {
			m.layers[layerIdx] = m.layers[layerIdx][:0]
			return
		}
		if prevLayerNodeCount%2 != 0 {
			newExpectendLayerNodeCount++
		}
		newLayerSize := newExpectendLayerNodeCount * length.Hash
		capacity := (newLayerSize / 2) * 3
		m.layers[layerIdx] = make([]byte, newLayerSize, capacity)
	}
	if len(m.layers[layerIdx]) == 0 {
		return
	}
	iterations := m.leavesCount / currentDivisor
	if m.leavesCount%currentDivisor != 0 {
		iterations++
	}

	for i := 0; i < iterations; i++ {
		fromOffset := i * length.Hash
		toOffset := (i + 1) * length.Hash
		if !bytes.Equal(m.layers[layerIdx][fromOffset:toOffset], ZeroHashes[0][:]) {
			continue
		}
		if layerIdx == 0 {
			m.computeLeaf(i, m.layers[layerIdx][fromOffset:toOffset])
			continue
		}
		childFromOffset := (i * 2) * length.Hash
		childToOffset := (i*2 + 2) * length.Hash
		if childToOffset > len(m.layers[layerIdx-1]) {
			copy(m.hashBuf[:length.Hash], m.layers[layerIdx-1][childFromOffset:])
			copy(m.hashBuf[length.Hash:], ZeroHashes[layerIdx-1][:])
		} else {
			copy(m.hashBuf[:], m.layers[layerIdx-1][childFromOffset:childToOffset])
		}
		if err := HashByteSlice(m.layers[layerIdx][fromOffset:toOffset], m.hashBuf[:]); err != nil {
			panic(err)
		}
	}
}
