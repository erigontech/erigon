package qmtree

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// Witness proves that a single transaction was correctly executed and
// included in the qmtree. It bundles a Merkle inclusion proof with the
// four leaf hash components so a verifier can:
//  1. Recompute the leaf hash from components
//  2. Verify Merkle inclusion against the tree root
type Witness struct {
	Proof            ProofPath
	PreStateHash     common.Hash
	StateChangeHash  common.Hash
	TransitionHash   common.Hash
	PreviousLeafHash common.Hash
}

// LeafHash recomputes the leaf hash from the four components.
//
//	leaf = keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
func (w *Witness) LeafHash() common.Hash {
	return ComputeLeafHash(w.PreStateHash, w.StateChangeHash, w.TransitionHash, w.PreviousLeafHash)
}

// ComputeLeafHash computes the four-component leaf hash.
func ComputeLeafHash(preState, stateChange, transition, prevLeaf common.Hash) common.Hash {
	h := crypto.NewKeccakState()
	defer crypto.ReturnToPool(h)
	h.Write(preState[:])
	h.Write(stateChange[:])
	h.Write(transition[:])
	h.Write(prevLeaf[:])
	var leaf common.Hash
	h.Read(leaf[:])
	return leaf
}

// Verify checks that:
//  1. The recomputed leaf hash matches the proof's leaf entry hash
//  2. The Merkle inclusion proof is valid against the root
//
// Uses complete mode (Check with complete=true) so that verification
// works on both freshly-generated and deserialized proofs. Intermediate
// SelfHash values are recomputed from the bottom up; if any sibling hash
// is wrong the computed root won't match.
func (w *Witness) Verify(hasher Hasher) error {
	leaf := w.LeafHash()
	if leaf != w.Proof.LeftOfTwig[0].SelfHash {
		return fmt.Errorf("leaf hash mismatch: computed %x, proof has %x", leaf, w.Proof.LeftOfTwig[0].SelfHash)
	}
	return w.Proof.Check(hasher, true)
}

// RangeWitness proves a contiguous range of transactions were correctly
// executed and included in the qmtree. Only the first and last entries
// carry full Merkle proofs; intermediate entries are verified by hash
// chain linking (each leaf commits to previousLeafHash).
//
// For a single-entry range, FirstProof == LastProof and Leaves has one element.
type RangeWitness struct {
	FirstProof ProofPath   // Merkle proof for the first entry in the range
	LastProof  ProofPath   // Merkle proof for the last entry in the range
	Leaves     []LeafData  // All leaves in the range, in serial number order
}

// LeafData holds the four components needed to recompute a leaf hash.
type LeafData struct {
	TxNum        uint64
	PreStateHash     common.Hash
	StateChangeHash  common.Hash
	TransitionHash   common.Hash
	PreviousLeafHash common.Hash
}

// LeafHash recomputes the leaf hash from components.
func (ld *LeafData) LeafHash() common.Hash {
	return ComputeLeafHash(ld.PreStateHash, ld.StateChangeHash, ld.TransitionHash, ld.PreviousLeafHash)
}

// Verify checks a range witness:
//  1. Each leaf hash is correctly computed from its components
//  2. Hash chain links: leaf[i].PreviousLeafHash == leafHash(leaf[i-1]) for i > 0
//  3. Serial numbers are contiguous
//  4. First leaf's Merkle proof is valid
//  5. Last leaf's Merkle proof is valid (if range has more than one entry)
func (rw *RangeWitness) Verify(hasher Hasher) error {
	if len(rw.Leaves) == 0 {
		return errors.New("empty range witness")
	}

	// Verify serial numbers are contiguous.
	firstSN := rw.Leaves[0].TxNum
	for i, ld := range rw.Leaves {
		if ld.TxNum != firstSN+uint64(i) {
			return fmt.Errorf("non-contiguous serial numbers: expected %d, got %d at index %d",
				firstSN+uint64(i), ld.TxNum, i)
		}
	}

	// Compute all leaf hashes and verify chain linking.
	leafHashes := make([]common.Hash, len(rw.Leaves))
	for i := range rw.Leaves {
		leafHashes[i] = rw.Leaves[i].LeafHash()

		// For entries after the first, verify that previousLeafHash matches
		// the computed hash of the prior leaf.
		if i > 0 && rw.Leaves[i].PreviousLeafHash != leafHashes[i-1] {
			return fmt.Errorf("chain break at index %d (sn=%d): previousLeafHash %x != computed %x",
				i, rw.Leaves[i].TxNum, rw.Leaves[i].PreviousLeafHash, leafHashes[i-1])
		}
	}

	// Verify first Merkle proof.
	if rw.FirstProof.TxNum != firstSN {
		return fmt.Errorf("first proof serial number mismatch: %d != %d", rw.FirstProof.TxNum, firstSN)
	}
	if leafHashes[0] != rw.FirstProof.LeftOfTwig[0].SelfHash {
		return fmt.Errorf("first leaf hash mismatch: computed %x, proof has %x",
			leafHashes[0], rw.FirstProof.LeftOfTwig[0].SelfHash)
	}
	if err := rw.FirstProof.Check(hasher, true); err != nil {
		return fmt.Errorf("first proof invalid: %w", err)
	}

	// Verify last Merkle proof (if range has more than one entry).
	lastIdx := len(rw.Leaves) - 1
	if lastIdx > 0 {
		lastSN := rw.Leaves[lastIdx].TxNum
		if rw.LastProof.TxNum != lastSN {
			return fmt.Errorf("last proof serial number mismatch: %d != %d", rw.LastProof.TxNum, lastSN)
		}
		if leafHashes[lastIdx] != rw.LastProof.LeftOfTwig[0].SelfHash {
			return fmt.Errorf("last leaf hash mismatch: computed %x, proof has %x",
				leafHashes[lastIdx], rw.LastProof.LeftOfTwig[0].SelfHash)
		}
		if err := rw.LastProof.Check(hasher, true); err != nil {
			return fmt.Errorf("last proof invalid: %w", err)
		}
	}

	// Verify both proofs agree on the root.
	if lastIdx > 0 && rw.FirstProof.Root != rw.LastProof.Root {
		return fmt.Errorf("root mismatch: first proof %x, last proof %x",
			rw.FirstProof.Root, rw.LastProof.Root)
	}

	return nil
}

// Root returns the tree root from the witness.
func (rw *RangeWitness) Root() common.Hash {
	return rw.FirstProof.Root
}

// --- Serialization ---

// WitnessToBytes serializes a Witness to bytes.
// Format: [4 bytes: proof length][proof bytes][4 hashes: 128 bytes]
func WitnessToBytes(w *Witness) []byte {
	proofBytes := w.Proof.ToBytes()
	buf := make([]byte, 4+len(proofBytes)+4*32)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(proofBytes)))
	copy(buf[4:], proofBytes)
	off := 4 + len(proofBytes)
	copy(buf[off:], w.PreStateHash[:])
	off += 32
	copy(buf[off:], w.StateChangeHash[:])
	off += 32
	copy(buf[off:], w.TransitionHash[:])
	off += 32
	copy(buf[off:], w.PreviousLeafHash[:])
	return buf
}

// BytesToWitness deserializes a Witness from bytes.
func BytesToWitness(bz []byte) (*Witness, error) {
	if len(bz) < 4 {
		return nil, errors.New("witness bytes too short")
	}
	proofLen := int(binary.LittleEndian.Uint32(bz[0:4]))
	if len(bz) < 4+proofLen+128 {
		return nil, fmt.Errorf("witness bytes too short: need %d, have %d", 4+proofLen+128, len(bz))
	}
	proof, err := BytesToProofPath(bz[4 : 4+proofLen])
	if err != nil {
		return nil, fmt.Errorf("proof deserialization failed: %w", err)
	}
	w := &Witness{Proof: *proof}
	off := 4 + proofLen
	copy(w.PreStateHash[:], bz[off:off+32])
	off += 32
	copy(w.StateChangeHash[:], bz[off:off+32])
	off += 32
	copy(w.TransitionHash[:], bz[off:off+32])
	off += 32
	copy(w.PreviousLeafHash[:], bz[off:off+32])
	return w, nil
}

// RangeWitnessToBytes serializes a RangeWitness to bytes.
// Format:
//
//	[4 bytes: first proof len][first proof bytes]
//	[4 bytes: last proof len][last proof bytes]
//	[4 bytes: leaf count]
//	For each leaf: [8 bytes: serial num][4 * 32 bytes: hashes]
func RangeWitnessToBytes(rw *RangeWitness) []byte {
	firstBytes := rw.FirstProof.ToBytes()
	lastBytes := rw.LastProof.ToBytes()
	leafSize := 8 + 4*32 // txNum + 4 hashes
	totalSize := 4 + len(firstBytes) + 4 + len(lastBytes) + 4 + len(rw.Leaves)*leafSize
	buf := make([]byte, 0, totalSize)

	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(firstBytes)))
	buf = append(buf, tmp[:]...)
	buf = append(buf, firstBytes...)

	binary.LittleEndian.PutUint32(tmp[:], uint32(len(lastBytes)))
	buf = append(buf, tmp[:]...)
	buf = append(buf, lastBytes...)

	binary.LittleEndian.PutUint32(tmp[:], uint32(len(rw.Leaves)))
	buf = append(buf, tmp[:]...)

	var snBuf [8]byte
	for _, ld := range rw.Leaves {
		binary.LittleEndian.PutUint64(snBuf[:], ld.TxNum)
		buf = append(buf, snBuf[:]...)
		buf = append(buf, ld.PreStateHash[:]...)
		buf = append(buf, ld.StateChangeHash[:]...)
		buf = append(buf, ld.TransitionHash[:]...)
		buf = append(buf, ld.PreviousLeafHash[:]...)
	}
	return buf
}

// BytesToRangeWitness deserializes a RangeWitness from bytes.
func BytesToRangeWitness(bz []byte) (*RangeWitness, error) {
	if len(bz) < 12 {
		return nil, errors.New("range witness bytes too short")
	}

	off := 0
	firstLen := int(binary.LittleEndian.Uint32(bz[off : off+4]))
	off += 4
	if off+firstLen > len(bz) {
		return nil, errors.New("first proof extends beyond buffer")
	}
	firstProof, err := BytesToProofPath(bz[off : off+firstLen])
	if err != nil {
		return nil, fmt.Errorf("first proof deserialization failed: %w", err)
	}
	off += firstLen

	if off+4 > len(bz) {
		return nil, errors.New("missing last proof length")
	}
	lastLen := int(binary.LittleEndian.Uint32(bz[off : off+4]))
	off += 4
	if off+lastLen > len(bz) {
		return nil, errors.New("last proof extends beyond buffer")
	}
	lastProof, err := BytesToProofPath(bz[off : off+lastLen])
	if err != nil {
		return nil, fmt.Errorf("last proof deserialization failed: %w", err)
	}
	off += lastLen

	if off+4 > len(bz) {
		return nil, errors.New("missing leaf count")
	}
	leafCount := int(binary.LittleEndian.Uint32(bz[off : off+4]))
	off += 4

	leafSize := 8 + 4*32
	if off+leafCount*leafSize > len(bz) {
		return nil, fmt.Errorf("leaf data extends beyond buffer: need %d, have %d",
			off+leafCount*leafSize, len(bz))
	}

	leaves := make([]LeafData, leafCount)
	for i := range leaves {
		leaves[i].TxNum = binary.LittleEndian.Uint64(bz[off : off+8])
		off += 8
		copy(leaves[i].PreStateHash[:], bz[off:off+32])
		off += 32
		copy(leaves[i].StateChangeHash[:], bz[off:off+32])
		off += 32
		copy(leaves[i].TransitionHash[:], bz[off:off+32])
		off += 32
		copy(leaves[i].PreviousLeafHash[:], bz[off:off+32])
		off += 32
	}

	return &RangeWitness{
		FirstProof: *firstProof,
		LastProof:  *lastProof,
		Leaves:     leaves,
	}, nil
}
