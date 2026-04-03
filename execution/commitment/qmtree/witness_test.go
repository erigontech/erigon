package qmtree

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// makeLeafEntry creates a tree entry whose hash is the four-component leaf hash.
func makeLeafEntry(txNum uint64, preState, stateChange, transition, prevLeaf common.Hash) *testEntry {
	h := ComputeLeafHash(preState, stateChange, transition, prevLeaf)
	return &testEntry{txNum: txNum, hash: h}
}

// buildWitnessTree creates a tree with N entries using chained leaf hashes
// and returns the tree, leaf data, and hasher.
func buildWitnessTree(t *testing.T, n int) (*Tree, []LeafData, *Keccak256Hasher) {
	t.Helper()
	hasher := &Keccak256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)
	leaves := make([]LeafData, n)
	var prevLeaf common.Hash

	for i := 0; i < n; i++ {
		ld := LeafData{
			TxNum:        uint64(i),
			PreStateHash:     hashFromUint(uint64(i*4 + 1)),
			StateChangeHash:  hashFromUint(uint64(i*4 + 2)),
			TransitionHash:   hashFromUint(uint64(i*4 + 3)),
			PreviousLeafHash: prevLeaf,
		}
		leafHash := ld.LeafHash()
		entry := &testEntry{txNum: uint64(i), hash: leafHash}
		_, err := tree.AppendEntry(entry)
		require.NoError(t, err)
		leaves[i] = ld
		prevLeaf = leafHash
	}

	// Sync tree to compute root.
	nList := tree.FlushFiles(0, 0)
	nList = tree.upperTree.EvictTwigs(hasher, nList, 0, 0)
	_, _ = tree.upperTree.SyncUpperNodes(hasher, nList, tree.youngestTwigId)

	return tree, leaves, hasher
}

func hashFromUint(v uint64) common.Hash {
	var h common.Hash
	h[0] = byte(v)
	h[1] = byte(v >> 8)
	h[2] = byte(v >> 16)
	h[3] = byte(v >> 24)
	return h
}

// ---------------------------------------------------------------------------
// ComputeLeafHash
// ---------------------------------------------------------------------------

func TestComputeLeafHash_Determinism(t *testing.T) {
	a := ComputeLeafHash(hashFromUint(1), hashFromUint(2), hashFromUint(3), hashFromUint(4))
	b := ComputeLeafHash(hashFromUint(1), hashFromUint(2), hashFromUint(3), hashFromUint(4))
	require.Equal(t, a, b)
}

func TestComputeLeafHash_DifferentInputs(t *testing.T) {
	a := ComputeLeafHash(hashFromUint(1), hashFromUint(2), hashFromUint(3), hashFromUint(4))
	b := ComputeLeafHash(hashFromUint(1), hashFromUint(2), hashFromUint(3), hashFromUint(5))
	require.NotEqual(t, a, b)
}

// ---------------------------------------------------------------------------
// Single Witness
// ---------------------------------------------------------------------------

func TestWitness_Verify(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 16)

	for i := 0; i < 16; i++ {
		proof, err := tree.GetProof(uint64(i))
		require.NoError(t, err)
		w := &Witness{
			Proof:            proof,
			PreStateHash:     leaves[i].PreStateHash,
			StateChangeHash:  leaves[i].StateChangeHash,
			TransitionHash:   leaves[i].TransitionHash,
			PreviousLeafHash: leaves[i].PreviousLeafHash,
		}
		require.NoError(t, w.Verify(hasher), "entry %d", i)
	}
}

func TestWitness_VerifyFailsOnTamperedLeaf(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 8)

	proof, err := tree.GetProof(3)
	require.NoError(t, err)
	w := &Witness{
		Proof:            proof,
		PreStateHash:     leaves[3].PreStateHash,
		StateChangeHash:  leaves[3].StateChangeHash,
		TransitionHash:   hashFromUint(999), // tampered
		PreviousLeafHash: leaves[3].PreviousLeafHash,
	}
	err = w.Verify(hasher)
	require.Error(t, err)
	require.Contains(t, err.Error(), "leaf hash mismatch")
}

func TestWitness_SerializationRoundtrip(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 8)

	proof, err := tree.GetProof(5)
	require.NoError(t, err)
	w := &Witness{
		Proof:            proof,
		PreStateHash:     leaves[5].PreStateHash,
		StateChangeHash:  leaves[5].StateChangeHash,
		TransitionHash:   leaves[5].TransitionHash,
		PreviousLeafHash: leaves[5].PreviousLeafHash,
	}

	bz := WitnessToBytes(w)
	w2, err := BytesToWitness(bz)
	require.NoError(t, err)

	// Verify the deserialized witness.
	require.NoError(t, w2.Verify(hasher))
	require.Equal(t, w.PreStateHash, w2.PreStateHash)
	require.Equal(t, w.StateChangeHash, w2.StateChangeHash)
	require.Equal(t, w.TransitionHash, w2.TransitionHash)
	require.Equal(t, w.PreviousLeafHash, w2.PreviousLeafHash)
	require.Equal(t, w.Proof.TxNum, w2.Proof.TxNum)
	require.Equal(t, w.Proof.Root, w2.Proof.Root)
}

// ---------------------------------------------------------------------------
// Range Witness
// ---------------------------------------------------------------------------

func TestRangeWitness_SingleEntry(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 8)

	proof, err := tree.GetProof(3)
	require.NoError(t, err)
	rw := &RangeWitness{
		FirstProof: proof,
		LastProof:  proof, // same for single entry
		Leaves:     []LeafData{leaves[3]},
	}
	require.NoError(t, rw.Verify(hasher))
}

func TestRangeWitness_MultipleEntries(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 16)

	// Range: entries 3..7
	firstProof, err := tree.GetProof(3)
	require.NoError(t, err)
	lastProof, err := tree.GetProof(7)
	require.NoError(t, err)

	rw := &RangeWitness{
		FirstProof: firstProof,
		LastProof:  lastProof,
		Leaves:     leaves[3:8], // indices 3,4,5,6,7
	}
	require.NoError(t, rw.Verify(hasher))
}

func TestRangeWitness_FailsOnBrokenChain(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 16)

	firstProof, err := tree.GetProof(3)
	require.NoError(t, err)
	lastProof, err := tree.GetProof(5)
	require.NoError(t, err)

	// Copy leaves so we can tamper without affecting original.
	tamperedLeaves := make([]LeafData, 3)
	copy(tamperedLeaves, leaves[3:6])
	tamperedLeaves[1].PreviousLeafHash = hashFromUint(999) // break chain

	rw := &RangeWitness{
		FirstProof: firstProof,
		LastProof:  lastProof,
		Leaves:     tamperedLeaves,
	}
	err = rw.Verify(hasher)
	require.Error(t, err)
	require.Contains(t, err.Error(), "chain break")
}

func TestRangeWitness_FailsOnNonContiguousSN(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 16)

	firstProof, err := tree.GetProof(3)
	require.NoError(t, err)
	lastProof, err := tree.GetProof(7)
	require.NoError(t, err)

	// Skip entry 5 — non-contiguous.
	rw := &RangeWitness{
		FirstProof: firstProof,
		LastProof:  lastProof,
		Leaves:     []LeafData{leaves[3], leaves[4], leaves[6], leaves[7]},
	}
	err = rw.Verify(hasher)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-contiguous")
}

func TestRangeWitness_FailsOnEmpty(t *testing.T) {
	rw := &RangeWitness{Leaves: nil}
	err := rw.Verify(&Keccak256Hasher{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty")
}

func TestRangeWitness_SerializationRoundtrip(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 16)

	firstProof, err := tree.GetProof(2)
	require.NoError(t, err)
	lastProof, err := tree.GetProof(6)
	require.NoError(t, err)

	rw := &RangeWitness{
		FirstProof: firstProof,
		LastProof:  lastProof,
		Leaves:     leaves[2:7],
	}

	bz := RangeWitnessToBytes(rw)
	rw2, err := BytesToRangeWitness(bz)
	require.NoError(t, err)

	require.NoError(t, rw2.Verify(hasher))
	require.Equal(t, len(rw.Leaves), len(rw2.Leaves))
	require.Equal(t, rw.FirstProof.Root, rw2.FirstProof.Root)
	require.Equal(t, rw.LastProof.Root, rw2.LastProof.Root)
	for i := range rw.Leaves {
		require.Equal(t, rw.Leaves[i].TxNum, rw2.Leaves[i].TxNum)
		require.Equal(t, rw.Leaves[i].TransitionHash, rw2.Leaves[i].TransitionHash)
	}
}

// ---------------------------------------------------------------------------
// Root consistency
// ---------------------------------------------------------------------------

func TestRangeWitness_RootMatchesTreeRoot(t *testing.T) {
	tree, leaves, hasher := buildWitnessTree(t, 10)

	// Get the root by syncing again (idempotent).
	root := common.Hash(tree.SyncAndRoot(hasher))

	firstProof, err := tree.GetProof(0)
	require.NoError(t, err)
	lastProof, err := tree.GetProof(9)
	require.NoError(t, err)

	rw := &RangeWitness{
		FirstProof: firstProof,
		LastProof:  lastProof,
		Leaves:     leaves,
	}
	require.NoError(t, rw.Verify(hasher))
	require.Equal(t, root, rw.Root())
}
