// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package qmtree

import (
	"bytes"
	"encoding/binary"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// KeyIndexEntry records the latest txNum at which a (domain, key) pair was
// written. KeyHash is keccak256(domain_byte || key_bytes), providing a uniform
// 32-byte identifier for sorting and proof generation.
type KeyIndexEntry struct {
	KeyHash common.Hash
	TxNum   uint64
}

// KeyIndex maintains a sorted, committed mapping from key hashes to their
// latest-write txNum. It supports:
//   - O(log n) inclusion proofs: "key K was last written at txNum T"
//   - O(log n) non-membership proofs: "key K was never written"
//
// The Merkle root is computed over entries sorted by KeyHash ascending.
// The root plus leaf count commits to the full content of the index.
//
// Integration: call UpdateKey after each transaction to record the written keys,
// then call Root() after each block to obtain the committed key-set root.
type KeyIndex struct {
	index  map[common.Hash]uint64 // key hash → latest txNum
	sorted []KeyIndexEntry        // sorted snapshot; rebuilt when dirty
	dirty  bool                   // true when index has changed since last sort
}

// NewKeyIndex creates an empty KeyIndex.
func NewKeyIndex() *KeyIndex {
	return &KeyIndex{
		index: make(map[common.Hash]uint64),
	}
}

// UpdateKey records that keyHash was written at txNum.
// In serial execution txNum is always monotonically increasing per key,
// but the method is safe to call in any order: it keeps the maximum.
func (ki *KeyIndex) UpdateKey(keyHash common.Hash, txNum uint64) {
	existing, ok := ki.index[keyHash]
	if !ok || txNum > existing {
		ki.index[keyHash] = txNum
		ki.dirty = true
	}
}

// Len returns the number of distinct keys in the index.
func (ki *KeyIndex) Len() int { return len(ki.index) }

// ensureSorted rebuilds the sorted slice from the index map if dirty.
func (ki *KeyIndex) ensureSorted() {
	if !ki.dirty {
		return
	}
	ki.sorted = ki.sorted[:0]
	for k, v := range ki.index {
		ki.sorted = append(ki.sorted, KeyIndexEntry{KeyHash: k, TxNum: v})
	}
	sort.Slice(ki.sorted, func(i, j int) bool {
		return bytes.Compare(ki.sorted[i].KeyHash[:], ki.sorted[j].KeyHash[:]) < 0
	})
	ki.dirty = false
}

// entryLeafHash computes the Merkle leaf hash for a single KeyIndexEntry.
//
//	leaf = keccak256(keyHash[32] || txNum_LE8[8])
func entryLeafHash(e KeyIndexEntry) common.Hash {
	var buf [40]byte
	copy(buf[:32], e.KeyHash[:])
	binary.LittleEndian.PutUint64(buf[32:40], e.TxNum)
	return crypto.Keccak256Hash(buf[:])
}

// Root returns the Merkle root over all entries sorted by KeyHash.
// Empty index returns the zero hash.
func (ki *KeyIndex) Root() common.Hash {
	ki.ensureSorted()
	return keyIndexMerkleRoot(ki.sorted)
}

// keyIndexMerkleRoot computes a binary Merkle root over sorted entries.
// Leaf hash: keccak256(keyHash || txNum_LE8).
// Internal node: keccak256(left || right).
// Odd-length layers: the trailing element is carried up unchanged (no duplication).
func keyIndexMerkleRoot(entries []KeyIndexEntry) common.Hash {
	if len(entries) == 0 {
		return common.Hash{}
	}
	layer := make([]common.Hash, len(entries))
	for i, e := range entries {
		layer[i] = entryLeafHash(e)
	}
	return merkleRootFromHashes(layer)
}

// merkleRootFromHashes reduces a slice of leaf/node hashes to a root.
func merkleRootFromHashes(layer []common.Hash) common.Hash {
	for len(layer) > 1 {
		next := make([]common.Hash, 0, (len(layer)+1)/2)
		for i := 0; i < len(layer); i += 2 {
			if i+1 < len(layer) {
				h := crypto.Keccak256Hash(layer[i][:], layer[i+1][:])
				next = append(next, h)
			} else {
				next = append(next, layer[i]) // odd: carry up
			}
		}
		layer = next
	}
	return layer[0]
}

// merkleProofPath returns the sibling hashes from leaf at leafIdx up to the root.
// The caller must also pass the leaf count so that the verifier can recompute
// positions.
func merkleProofPath(leaves []common.Hash, leafIdx int) []common.Hash {
	layer := make([]common.Hash, len(leaves))
	copy(layer, leaves)

	var path []common.Hash
	pos := leafIdx
	for len(layer) > 1 {
		sibIdx := pos ^ 1
		if sibIdx < len(layer) {
			path = append(path, layer[sibIdx])
		} else {
			// Odd trailing leaf: no sibling; the node carries up as-is.
			// We record the zero hash as a sentinel so the verifier can
			// reconstruct the same carry-up behaviour.
			path = append(path, common.Hash{})
		}
		// Build next layer.
		next := make([]common.Hash, 0, (len(layer)+1)/2)
		for i := 0; i < len(layer); i += 2 {
			if i+1 < len(layer) {
				next = append(next, crypto.Keccak256Hash(layer[i][:], layer[i+1][:]))
			} else {
				next = append(next, layer[i])
			}
		}
		layer = next
		pos >>= 1
	}
	return path
}

// verifyMerklePath recomputes the root from a leaf hash, its index, the path
// of sibling hashes, and the total leaf count.
func verifyMerklePath(leaf common.Hash, leafIdx int, leafCount int, path []common.Hash) common.Hash {
	h := leaf
	pos := leafIdx
	n := leafCount
	for _, sibling := range path {
		if n == 1 {
			break // reached root
		}
		sibIdx := pos ^ 1
		if sibIdx >= n {
			// Odd trailing: no sibling; carry up.
			// sibling recorded as zero sentinel — ignore it.
		} else if pos%2 == 0 {
			// pos is left child
			h = crypto.Keccak256Hash(h[:], sibling[:])
		} else {
			// pos is right child
			h = crypto.Keccak256Hash(sibling[:], h[:])
		}
		n = (n + 1) / 2
		pos >>= 1
	}
	return h
}

// ExclusionProof proves one of two things about a key hash and a committed
// KeyIndex root:
//
//   - Exists=true: the key was last written at TxNum. The Merkle path proves
//     the entry (KeyHash, TxNum) is a leaf in the KeyIndex at position LeafIndex.
//
//   - Exists=false: the key was never written. PrevEntry and NextEntry are the
//     adjacent entries in sorted order (either may be nil if the key falls
//     outside the range). Their positions (PrevIndex, NextIndex) and paths
//     prove they are in the KeyIndex and are consecutive (NextIndex == PrevIndex+1),
//     with no entry between them for the queried KeyHash.
type ExclusionProof struct {
	KeyHash   common.Hash
	Root      common.Hash
	LeafCount int // total entries in the KeyIndex at this root

	// Inclusion (Exists=true):
	Exists    bool
	TxNum     uint64
	LeafIndex int           // position of this entry in sorted order
	Path      []common.Hash // sibling hashes bottom-up

	// Non-membership (Exists=false):
	PrevEntry *KeyIndexEntry
	PrevIndex int
	PrevPath  []common.Hash
	NextEntry *KeyIndexEntry
	NextIndex int
	NextPath  []common.Hash
}

// GetProof returns an ExclusionProof for the given keyHash against the current
// state of the index. SyncRoot or Root() should be called first to ensure
// the sorted slice is up to date.
func (ki *KeyIndex) GetProof(keyHash common.Hash) *ExclusionProof {
	ki.ensureSorted()

	leaves := make([]common.Hash, len(ki.sorted))
	for i, e := range ki.sorted {
		leaves[i] = entryLeafHash(e)
	}
	root := merkleRootFromHashes(append([]common.Hash(nil), leaves...))

	proof := &ExclusionProof{
		KeyHash:   keyHash,
		Root:      root,
		LeafCount: len(ki.sorted),
	}

	// Binary search for the key hash.
	idx := sort.Search(len(ki.sorted), func(i int) bool {
		return bytes.Compare(ki.sorted[i].KeyHash[:], keyHash[:]) >= 0
	})

	if idx < len(ki.sorted) && ki.sorted[idx].KeyHash == keyHash {
		// Inclusion proof.
		proof.Exists = true
		proof.TxNum = ki.sorted[idx].TxNum
		proof.LeafIndex = idx
		if len(leaves) > 0 {
			proof.Path = merkleProofPath(leaves, idx)
		}
	} else {
		// Non-membership proof: adjacent entries.
		if idx > 0 {
			prev := ki.sorted[idx-1]
			proof.PrevEntry = &prev
			proof.PrevIndex = idx - 1
			proof.PrevPath = merkleProofPath(leaves, idx-1)
		}
		if idx < len(ki.sorted) {
			next := ki.sorted[idx]
			proof.NextEntry = &next
			proof.NextIndex = idx
			proof.NextPath = merkleProofPath(leaves, idx)
		}
	}
	return proof
}

// Verify checks that the proof is internally consistent against its Root and
// LeafCount. Returns true if the proof is valid.
func (ep *ExclusionProof) Verify() bool {
	if ep.Exists {
		return ep.verifyInclusion()
	}
	return ep.verifyNonMembership()
}

func (ep *ExclusionProof) verifyInclusion() bool {
	if ep.LeafCount == 0 {
		return false
	}
	if ep.LeafIndex < 0 || ep.LeafIndex >= ep.LeafCount {
		return false
	}
	entry := KeyIndexEntry{KeyHash: ep.KeyHash, TxNum: ep.TxNum}
	leaf := entryLeafHash(entry)
	computed := verifyMerklePath(leaf, ep.LeafIndex, ep.LeafCount, ep.Path)
	return computed == ep.Root
}

func (ep *ExclusionProof) verifyNonMembership() bool {
	// Verify prev entry membership (if present).
	if ep.PrevEntry != nil {
		if bytes.Compare(ep.PrevEntry.KeyHash[:], ep.KeyHash[:]) >= 0 {
			return false // prev must be strictly less
		}
		if ep.PrevIndex < 0 || ep.PrevIndex >= ep.LeafCount {
			return false
		}
		prevLeaf := entryLeafHash(*ep.PrevEntry)
		computed := verifyMerklePath(prevLeaf, ep.PrevIndex, ep.LeafCount, ep.PrevPath)
		if computed != ep.Root {
			return false
		}
	}

	// Verify next entry membership (if present).
	if ep.NextEntry != nil {
		if bytes.Compare(ep.NextEntry.KeyHash[:], ep.KeyHash[:]) <= 0 {
			return false // next must be strictly greater
		}
		if ep.NextIndex < 0 || ep.NextIndex >= ep.LeafCount {
			return false
		}
		nextLeaf := entryLeafHash(*ep.NextEntry)
		computed := verifyMerklePath(nextLeaf, ep.NextIndex, ep.LeafCount, ep.NextPath)
		if computed != ep.Root {
			return false
		}
	}

	// Verify adjacency: next must immediately follow prev in sorted order.
	if ep.PrevEntry != nil && ep.NextEntry != nil {
		if ep.NextIndex != ep.PrevIndex+1 {
			return false
		}
	}

	// If index is empty, both will be nil — valid non-membership.
	return true
}

// KeyHash computes the canonical key hash for a (domain, key) pair:
//
//	keccak256(domain_byte || key_bytes)
func KeyHash(domain byte, key []byte) common.Hash {
	buf := make([]byte, 1+len(key))
	buf[0] = domain
	copy(buf[1:], key)
	return crypto.Keccak256Hash(buf)
}
