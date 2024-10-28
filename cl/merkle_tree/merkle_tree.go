package merkle_tree

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"sync/atomic"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon/common/math"
)

func ceil(num, divisor int) int {
	return (num + (divisor - 1)) / divisor
}

type HashTreeEncodable interface {
	WriteMerkleTree(w io.Writer) error
	ReadMerkleTree(r io.Reader) error
}

const OptimalMaxTreeCacheDepth = 12

type MerkleTree struct {
	computeLeaf func(idx int, out []byte)
	layers      [][]byte // Flat hash-layers
	leavesCount int

	hashBuf [64]byte // buffer to store the input for hash(hash1, hash2)
	limit   *uint64  // Optional limit for the number of leaves (this will enable limit-oriented hashing)

	dirtyLeaves []atomic.Bool
	mu          sync.RWMutex
}

var _ HashTreeEncodable = (*MerkleTree)(nil)

// Layout of the layers:

// 0-n: intermediate layers
// Root is not stored in the layers, root is recomputed on demand
// The first layer is not the leaf layer, but the first intermediate layer, the leaf layer is not stored in the layers.

// Initialize initializes the Merkle tree with the given number of leaves and the maximum depth of the tree cache.
func (m *MerkleTree) Initialize(leavesCount, maxTreeCacheDepth int, computeLeaf func(idx int, out []byte), limitOptional *uint64) {
	m.computeLeaf = computeLeaf
	m.layers = make([][]byte, maxTreeCacheDepth)
	m.leavesCount = leavesCount
	firstLayerSize := ((leavesCount + 1) / 2) * length.Hash
	capacity := (firstLayerSize / 2) * 3
	m.layers[0] = make([]byte, firstLayerSize, capacity)
	if limitOptional != nil {
		m.limit = new(uint64)
		*m.limit = *limitOptional
	}
	m.dirtyLeaves = make([]atomic.Bool, leavesCount)
}

func (m *MerkleTree) MarkLeafAsDirty(idx int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.dirtyLeaves[idx].Store(true)
}

// MarkLeafAsDirty resets the leaf at the given index, so that it will be recomputed on the next call to ComputeRoot.
func (m *MerkleTree) markLeafAsDirty(idx int) {
	for i := 0; i < len(m.layers); i++ {
		currDivisor := 1 << (i + 1) // i+1 because the first layer is not the leaf layer
		layerSize := (m.leavesCount + (currDivisor - 1)) / currDivisor
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
	m.mu.RLock()
	defer m.mu.RUnlock()
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
	var prevLayerNodeCount int
	if layerIdx == 0 {
		prevLayerNodeCount = m.leavesCount + 1
	} else {
		prevLayerNodeCount = len(m.layers[layerIdx-1]) / length.Hash
	}
	// find previous layer nodes count and round  to the next power of 2
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
	m.dirtyLeaves = append(m.dirtyLeaves, atomic.Bool{})
}

// ComputeRoot computes the root of the Merkle tree.
func (m *MerkleTree) ComputeRoot() libcommon.Hash {
	m.mu.Lock()
	defer m.mu.Unlock()
	var root libcommon.Hash
	if len(m.layers) == 0 {
		return ZeroHashes[0]
	}
	for idx := range m.dirtyLeaves {
		if m.dirtyLeaves[idx].Load() {
			m.markLeafAsDirty(idx)
			m.dirtyLeaves[idx].Store(false)
		}
	}

	if m.leavesCount == 0 {
		if m.limit == nil {
			return ZeroHashes[0]
		}
		return ZeroHashes[GetDepth(*m.limit)]
	}

	if m.leavesCount <= 3 {
		buf := make([]byte, 0, 3*length.Hash)
		for i := 0; i < m.leavesCount; i++ {
			m.computeLeaf(i, m.hashBuf[:length.Hash])
			buf = append(buf, m.hashBuf[:length.Hash]...)
		}
		if m.limit != nil {
			if err := MerkleRootFromFlatFromIntermediateLevelWithLimit(buf, root[:], int(*m.limit), 0); err != nil {
				panic(err)
			}
			return root
		}
		if err := MerkleRootFromFlatFromIntermediateLevel(buf, root[:], m.leavesCount*length.Hash, 0); err != nil {
			panic(err)
		}
		return root
	}

	if len(m.layers[0]) == length.Hash {
		var node libcommon.Hash
		m.computeLeaf(0, node[:])
		if m.limit != nil {
			if err := MerkleRootFromFlatFromIntermediateLevelWithLimit(node[:], root[:], int(*m.limit), 0); err != nil {
				panic(err)
			}
			return root
		}
		return node
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

func (m *MerkleTree) CopyInto(other *MerkleTree) {
	other.computeLeaf = m.computeLeaf
	other.layers = make([][]byte, len(m.layers))
	for i := 0; i < len(m.layers); i++ {
		other.layers[i] = make([]byte, len(m.layers[i]))
		copy(other.layers[i], m.layers[i])
	}
	other.leavesCount = m.leavesCount
	other.limit = m.limit
}

func (m *MerkleTree) finishHashing(lastLayerIdx int, root []byte) {
	if m.limit == nil {
		if err := MerkleRootFromFlatFromIntermediateLevel(m.layers[lastLayerIdx], root, m.leavesCount*length.Hash, lastLayerIdx+1); err != nil {
			panic(err)
		}
		return
	}

	if err := MerkleRootFromFlatFromIntermediateLevelWithLimit(m.layers[lastLayerIdx], root, int(*m.limit), lastLayerIdx+1); err != nil {
		panic(err)
	}
}

func (m *MerkleTree) computeLayer(layerIdx int) {
	currentDivisor := 1 << uint(layerIdx+1)
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

	iterations := ceil(m.leavesCount, currentDivisor)

	for i := 0; i < iterations; i++ {
		fromOffset := i * length.Hash
		toOffset := (i + 1) * length.Hash
		if !bytes.Equal(m.layers[layerIdx][fromOffset:toOffset], ZeroHashes[0][:]) {
			continue
		}
		if layerIdx == 0 {
			// leaf layer is always dirty
			leafIndexBegin := i * 2
			m.computeLeaf(leafIndexBegin, m.hashBuf[:length.Hash])
			if leafIndexBegin == m.leavesCount-1 {
				copy(m.hashBuf[length.Hash:], ZeroHashes[0][:])
			} else {
				m.computeLeaf(leafIndexBegin+1, m.hashBuf[length.Hash:])
			}
			if err := HashByteSlice(m.layers[layerIdx][fromOffset:toOffset], m.hashBuf[:]); err != nil {
				panic(err)
			}
			continue
		}
		childFromOffset := (i * 2) * length.Hash
		childToOffset := (i*2 + 2) * length.Hash
		if childToOffset > len(m.layers[layerIdx-1]) {
			copy(m.hashBuf[:length.Hash], m.layers[layerIdx-1][childFromOffset:])
			copy(m.hashBuf[length.Hash:], ZeroHashes[layerIdx][:])
		} else {
			copy(m.hashBuf[:], m.layers[layerIdx-1][childFromOffset:childToOffset])
		}
		if err := HashByteSlice(m.layers[layerIdx][fromOffset:toOffset], m.hashBuf[:]); err != nil {
			panic(err)
		}
	}
}

// Write writes the Merkle tree to the given writer.
func (m *MerkleTree) WriteMerkleTree(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(m.layers))); err != nil {
		return err
	}
	for _, layer := range m.layers {
		if err := binary.Write(w, binary.BigEndian, uint32(len(layer))); err != nil {
			return err
		}
		if _, err := w.Write(layer); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, uint32(m.leavesCount)); err != nil {
		return err
	}
	if m.limit != nil {
		if err := binary.Write(w, binary.BigEndian, *m.limit); err != nil {
			return err
		}
	} else {
		if err := binary.Write(w, binary.BigEndian, uint64(math.MaxUint64)); err != nil {
			return err
		}
	}
	return nil
}

// Read reads the Merkle tree from the given reader.
func (m *MerkleTree) ReadMerkleTree(r io.Reader) error {
	var layersCount uint32
	if err := binary.Read(r, binary.BigEndian, &layersCount); err != nil {
		return err
	}
	m.layers = make([][]byte, layersCount)
	for i := 0; i < int(layersCount); i++ {
		var layerSize uint32
		if err := binary.Read(r, binary.BigEndian, &layerSize); err != nil {
			return err
		}
		m.layers[i] = make([]byte, layerSize)
		if _, err := io.ReadFull(r, m.layers[i]); err != nil {
			return err
		}
	}
	leavesCount := uint32(0)

	if err := binary.Read(r, binary.BigEndian, &leavesCount); err != nil {
		return err
	}
	m.leavesCount = int(leavesCount)
	var limit uint64
	if err := binary.Read(r, binary.BigEndian, &limit); err != nil {
		return err
	}
	if limit == math.MaxUint64 {
		m.limit = nil
	} else {
		m.limit = new(uint64)
		*m.limit = limit
	}
	return nil
}
