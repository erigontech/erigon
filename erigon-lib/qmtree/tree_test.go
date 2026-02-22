package qmtree

import (
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

// testEntry is a minimal Entry implementation for use in tree tests.
type testEntry struct {
	sn   uint64
	hash common.Hash
}

func (e *testEntry) Hash() common.Hash    { return e.hash }
func (e *testEntry) SerialNumber() uint64 { return e.sn }
func (e *testEntry) Len() int64           { return 0 }

func makeEntry(sn uint64) *testEntry {
	var h common.Hash
	binary.LittleEndian.PutUint64(h[:], sn+1) // non-zero hash
	return &testEntry{sn: sn, hash: h}
}

// ---------------------------------------------------------------------------
// TwigMT.Clone
// ---------------------------------------------------------------------------

func TestTwigMTCloneIsIndependent(t *testing.T) {
	mt := make(TwigMT, 4096)
	mt[100][0] = 0xAB
	clone := mt.Clone()
	// mutating the clone must not affect the original
	clone[100][0] = 0xFF
	require.Equal(t, byte(0xAB), mt[100][0])
	require.NotEqual(t, mt[100], clone[100])
}

// ---------------------------------------------------------------------------
// ActiveBits exact boundary
// ---------------------------------------------------------------------------

func TestActiveBitsExactBoundary(t *testing.T) {
	bits := ActiveBits{}
	// 2047 is the last valid index
	require.NotPanics(t, func() { bits.SetBit(2047) })
	require.NotPanics(t, func() { bits.ClearBit(2047) })
	require.NotPanics(t, func() { bits.GetBit(2047) })
	// 2048 must panic (would write ab[256] which is OOB for [256]byte)
	require.Panics(t, func() { bits.SetBit(2048) })
	require.Panics(t, func() { bits.ClearBit(2048) })
	require.Panics(t, func() { bits.GetBit(2048) })
}

// ---------------------------------------------------------------------------
// NodePos encoding round-trip
// ---------------------------------------------------------------------------

func TestNodePosRoundtrip(t *testing.T) {
	cases := []struct {
		level uint8
		n     uint64
	}{
		{13, 0},
		{13, 1},
		{20, 42},
		{63, (1 << 56) - 1},
		{0, 0},
	}
	for _, tc := range cases {
		pos := nodePos(tc.level, tc.n)
		require.Equal(t, uint64(tc.level), pos.Level(), "level for (%d,%d)", tc.level, tc.n)
		require.Equal(t, tc.n, pos.Nth(), "nth for (%d,%d)", tc.level, tc.n)
	}
}

// ---------------------------------------------------------------------------
// GetShardIdxAndKey invertibility
// ---------------------------------------------------------------------------

func TestGetShardIdxAndKeyInvertible(t *testing.T) {
	for _, id := range []uint64{0, 1, 2, 3, 4, 5, 7, 11, 100, 2047, 2048} {
		s, k := GetShardIdxAndKey(id)
		require.Equal(t, id, uint64(s)+k*TWIG_SHARD_COUNT, "twigId=%d", id)
	}
}

// ---------------------------------------------------------------------------
// MaxNAtLevel
// ---------------------------------------------------------------------------

func TestMaxNAtLevel(t *testing.T) {
	// youngestTwigId=0, shift=1 → 0>>1=0
	require.Equal(t, uint64(0), MaxNAtLevel(0, FIRST_LEVEL_ABOVE_TWIG))
	// youngestTwigId=1, shift=1 → 1>>1=0
	require.Equal(t, uint64(0), MaxNAtLevel(1, FIRST_LEVEL_ABOVE_TWIG))
	// youngestTwigId=3, shift=1 → 3>>1=1
	require.Equal(t, uint64(1), MaxNAtLevel(3, FIRST_LEVEL_ABOVE_TWIG))
	// youngestTwigId=7, level=FIRST+1, shift=2 → 7>>2=1
	require.Equal(t, uint64(1), MaxNAtLevel(7, FIRST_LEVEL_ABOVE_TWIG+1))
	// youngestTwigId=8, level=FIRST, shift=1 → 8>>1=4
	require.Equal(t, uint64(4), MaxNAtLevel(8, FIRST_LEVEL_ABOVE_TWIG))
}

// ---------------------------------------------------------------------------
// EdgeNodesToBytes round-trip
// ---------------------------------------------------------------------------

func TestEdgeNodesToBytes(t *testing.T) {
	nodes := []EdgeNode{
		{nodePos(13, 5), [32]byte{1, 2, 3}},
		{nodePos(14, 2), [32]byte{5, 6, 7}},
	}
	bz := EdgeNodesToBytes(nodes)
	require.Len(t, bz, len(nodes)*40)

	for i, n := range nodes {
		pos := NodePos(binary.LittleEndian.Uint64(bz[i*40:]))
		require.Equal(t, n.pos, pos, "pos[%d]", i)
		require.Equal(t, n.value[:], bz[i*40+8:(i+1)*40], "value[%d]", i)
	}
}

// ---------------------------------------------------------------------------
// TwigMT.Sync partial update
// ---------------------------------------------------------------------------

func TestTwigMTSyncPartial(t *testing.T) {
	hasher := &Sha256Hasher{}
	mt := hasher.nullMtForTwig()

	// Overwrite leaf at position 2 (array index 2048+2=2050)
	mt[2050] = common.Hash{1, 2, 3}
	mt.Sync(hasher, 2, 2)

	// Parent of leaves 2050 and 2051 lives at index (2048+2)/2 = 1025
	expected := hasher.nodeHash(0, mt[2050][:], mt[2051][:])
	require.Equal(t, expected, mt[1025], "parent hash after partial sync")
}

// ---------------------------------------------------------------------------
// NewTree does not panic
// ---------------------------------------------------------------------------

func TestNewTreeDoesNotPanic(t *testing.T) {
	hasher := &Sha256Hasher{}
	require.NotPanics(t, func() {
		tree := NewTree(hasher, 0, nil, nil)
		require.NotNil(t, tree)
		require.Equal(t, uint64(0), tree.youngestTwigId)
	})
}

// ---------------------------------------------------------------------------
// AppendEntry activates the active bit
// ---------------------------------------------------------------------------

func TestAppendEntryActivatesBit(t *testing.T) {
	hasher := &Sha256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)

	_, err := tree.AppendEntry(makeEntry(0))
	require.NoError(t, err)
	require.True(t, tree.GetActiveBit(0))
}

// ---------------------------------------------------------------------------
// DeactiveEntry clears the active bit
// ---------------------------------------------------------------------------

func TestDeactiveEntry(t *testing.T) {
	hasher := &Sha256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)

	_, err := tree.AppendEntry(makeEntry(0))
	require.NoError(t, err)
	require.True(t, tree.GetActiveBit(0))

	tree.DeactiveEntry(0)
	require.False(t, tree.GetActiveBit(0))
}

// ---------------------------------------------------------------------------
// Filling a twig causes rollover to twig 1
// ---------------------------------------------------------------------------

func TestTwigRollover(t *testing.T) {
	hasher := &Sha256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)

	for i := uint64(0); i < LEAF_COUNT_IN_TWIG; i++ {
		_, err := tree.AppendEntry(makeEntry(i))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1), tree.youngestTwigId)
}

// ---------------------------------------------------------------------------
// ProofPath.Check does not panic on valid input (regression for OOB bug)
// ---------------------------------------------------------------------------

func TestProofCheckNoPanic(t *testing.T) {
	hasher := &Sha256Hasher{}
	path := ProofPath{
		SerialNum: 0,
		UpperPath: []ProofNode{{}},
	}
	// Should not panic; hash mismatches are expected but no OOB access
	require.NotPanics(t, func() { _ = path.Check(hasher, false) })
	require.NotPanics(t, func() { _ = path.Check(hasher, true) })
}

// ---------------------------------------------------------------------------
// ProofPath ToBytes / BytesToProofPath round-trip
// ---------------------------------------------------------------------------

func TestProofPathBytesRoundtrip(t *testing.T) {
	original := ProofPath{
		SerialNum: 12345,
		UpperPath: []ProofNode{{SelfHash: common.Hash{1}, PeerHash: common.Hash{2}}, {}},
		Root:      common.Hash{0xFF},
	}
	original.LeftOfTwig[0].SelfHash = common.Hash{3}
	original.RightOfTwig[0].SelfHash = common.Hash{4}

	bz := original.ToBytes()
	recovered, err := BytesToProofPath(bz)
	require.NoError(t, err)
	require.Equal(t, original.SerialNum, recovered.SerialNum)
	require.Equal(t, original.Root, recovered.Root)
	require.Len(t, recovered.UpperPath, len(original.UpperPath))
}

// syncTree runs the full three-phase sync needed to make proofs verifiable:
//   1. FlushFiles (Phase 1: sync youngest twig's merkle tree + active bits L1)
//   2. EvictTwigs (Phase 2: sync active bits L2/L3/Top; no actual eviction here)
//   3. SyncUpperNodes (update nodes above twig level)
func syncTree(t *testing.T, tree *Tree, hasher *Sha256Hasher) common.Hash {
	t.Helper()
	nList := tree.FlushFiles(0, 0)
	nList = tree.upperTree.EvictTwigs(hasher, nList, 0, 0)
	_, root := tree.upperTree.SyncUpperNodes(hasher, nList, tree.youngestTwigId)
	return common.Hash(root)
}

// ---------------------------------------------------------------------------
// End-to-end: GetProof + CheckProof on a small tree
// ---------------------------------------------------------------------------

func TestGetProofAndVerify(t *testing.T) {
	hasher := &Sha256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)

	const N = 16
	for i := uint64(0); i < N; i++ {
		_, err := tree.AppendEntry(makeEntry(i))
		require.NoError(t, err)
	}

	syncTree(t, tree, hasher)

	for i := uint64(0); i < N; i++ {
		proof, err := tree.GetProof(i)
		require.NoError(t, err, "GetProof(%d)", i)
		_, err = CheckProof(hasher, &proof)
		require.NoError(t, err, "CheckProof(%d)", i)
	}
}

// ---------------------------------------------------------------------------
// End-to-end: proof remains valid after deactivating entries
// ---------------------------------------------------------------------------

func TestProofAfterDeactivation(t *testing.T) {
	hasher := &Sha256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)

	const N = 8
	for i := uint64(0); i < N; i++ {
		_, err := tree.AppendEntry(makeEntry(i))
		require.NoError(t, err)
	}

	// Deactivate some entries
	tree.DeactiveEntry(2)
	tree.DeactiveEntry(5)

	syncTree(t, tree, hasher)

	// Active bits should be reflected correctly
	require.True(t, tree.GetActiveBit(0))
	require.False(t, tree.GetActiveBit(2))
	require.True(t, tree.GetActiveBit(3))
	require.False(t, tree.GetActiveBit(5))

	// All entries (active or not) should still produce valid proofs
	for i := uint64(0); i < N; i++ {
		proof, err := tree.GetProof(i)
		require.NoError(t, err, "GetProof(%d)", i)
		_, err = CheckProof(hasher, &proof)
		require.NoError(t, err, "CheckProof(%d) after deactivation", i)
	}
}

// ---------------------------------------------------------------------------
// Proof root is consistent across all entries in the same flush
// ---------------------------------------------------------------------------

func TestProofRootsAreConsistent(t *testing.T) {
	hasher := &Sha256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)

	const N = 10
	for i := uint64(0); i < N; i++ {
		_, err := tree.AppendEntry(makeEntry(i))
		require.NoError(t, err)
	}

	root := syncTree(t, tree, hasher)

	for i := uint64(0); i < N; i++ {
		proof, err := tree.GetProof(i)
		require.NoError(t, err)
		require.Equal(t, root, proof.Root, "root mismatch for entry %d", i)
	}
}

// ---------------------------------------------------------------------------
// Spanning two twigs: proof works across twig boundary
// ---------------------------------------------------------------------------

func TestGetProofAcrossTwigBoundary(t *testing.T) {
	hasher := &Sha256Hasher{}
	tree := NewTree(hasher, 0, nil, nil)

	// Fill twig 0 completely and add a few entries to twig 1
	const total = LEAF_COUNT_IN_TWIG + 4
	for i := uint64(0); i < total; i++ {
		_, err := tree.AppendEntry(makeEntry(i))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1), tree.youngestTwigId)

	syncTree(t, tree, hasher)

	// Twig 1 entries: proof can be retrieved (no disk storage needed for youngest twig)
	for _, sn := range []uint64{LEAF_COUNT_IN_TWIG, LEAF_COUNT_IN_TWIG + 1, LEAF_COUNT_IN_TWIG + 3} {
		proof, err := tree.GetProof(sn)
		require.NoError(t, err, "GetProof(sn=%d)", sn)
		_, err = CheckProof(hasher, &proof)
		require.NoError(t, err, "CheckProof(sn=%d)", sn)
	}
}
