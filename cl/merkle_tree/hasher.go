package merkle_tree

import (
	"fmt"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/prysmaticlabs/gohashtree"
)

var globalHasher *merkleHasher

const initialBufferSize = 0 // it is whatever

// merkleHasher is used internally to provide shared buffer internally to the merkle_tree package.
type merkleHasher struct {
	// internalBuffer is the shared buffer we use for each operation
	internalBuffer           [][32]byte
	internalFlatBuffer       []byte
	internalBufferForSSZList [][32]byte
	// mu is the lock to ensure thread safety
	mu  sync.Mutex
	mu2 sync.Mutex // lock onto ssz list buffer
}

func newMerkleHasher() *merkleHasher {
	return &merkleHasher{
		internalBuffer:     make([][32]byte, initialBufferSize),
		internalFlatBuffer: make([]byte, initialBufferSize*32),
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

// merkleizeTrieLeaves returns intermediate roots of given leaves.
func (m *merkleHasher) merkleizeTrieLeavesFlat(leaves []byte, out []byte) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	layer := m.getBufferFromFlat(leaves)
	for len(layer) > 1 {
		if err := gohashtree.Hash(layer, layer); err != nil {
			return err
		}
		layer = layer[:len(layer)/2]
	}
	copy(out, layer[0][:])
	return
}

func (m *merkleHasher) hashByteSlice(out []byte, in []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.getBufferFromFlat(in)
	o := make([][32]byte, len(l)/2)
	err := gohashtree.Hash(o, l)
	if err != nil {
		return err
	}
	for i := range o {
		copy(out[i*32:(i+1)*32], o[i][:])
	}
	return nil
}

// getBuffer provides buffer of given size.
func (m *merkleHasher) getBuffer(size int) [][32]byte {
	if size > len(m.internalBuffer) {
		m.internalBuffer = make([][32]byte, size*2)
	}
	return m.internalBuffer[:size]
}

// getBuffer provides buffer of given size.
func (m *merkleHasher) getBufferForSSZList(size int) [][32]byte {
	if size > len(m.internalBufferForSSZList) {
		m.internalBufferForSSZList = make([][32]byte, size*2)
	}
	return m.internalBufferForSSZList[:size]
}

func (m *merkleHasher) getBufferFromFlat(xs []byte) [][32]byte {
	buf := m.getBuffer(len(xs) / 32)
	for i := 0; i < len(xs)/32; i = i + 1 {
		copy(buf[i][:], xs[i*32:(i+1)*32])
	}
	return buf
}

// getBuffer provides buffer of given size.
func (m *merkleHasher) getFlatBuffer(size int) []byte {
	if size > len(m.internalFlatBuffer) {
		m.internalFlatBuffer = make([]byte, size)
	}
	return m.internalFlatBuffer[:size]
}

func (m *merkleHasher) transactionsListRoot(transactions [][]byte) ([32]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	txCount := uint64(len(transactions))

	leaves := m.getBuffer(len(transactions))
	for i, transaction := range transactions {
		transactionLength := uint64(len(transaction))
		packedTransactions := packBits(transaction) // Pack transactions
		transactionsBaseRoot, err := MerkleizeVector(packedTransactions, 33554432)
		if err != nil {
			return [32]byte{}, err
		}

		lengthRoot := Uint64Root(transactionLength)
		leaves[i] = utils.Keccak256(transactionsBaseRoot[:], lengthRoot[:])
	}
	transactionsBaseRoot, err := MerkleizeVector(leaves, 1048576)
	if err != nil {
		return libcommon.Hash{}, err
	}

	countRoot := Uint64Root(txCount)

	return utils.Keccak256(transactionsBaseRoot[:], countRoot[:]), nil
}
