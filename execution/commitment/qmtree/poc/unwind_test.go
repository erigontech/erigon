package poc

import (
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/qmtree"
	"github.com/stretchr/testify/require"
)

// buildTestTree appends n entries to a new tree and returns the tree,
// hasher, all entry hashes, and the root after all entries.
func buildTestTree(t *testing.T, n int) (*qmtree.Tree, *qmtree.Keccak256Hasher, []common.Hash) {
	t.Helper()
	hasher := &qmtree.Keccak256Hasher{}
	tree := qmtree.NewTree(hasher, 0, nil, nil)
	hashes := make([]common.Hash, n)
	for i := 0; i < n; i++ {
		entry := NewStateEntry(uint64(i), []StateChange{
			{Domain: kv.AccountsDomain, Key: []byte{byte(i >> 8), byte(i)}, Value: []byte{byte(i)}},
		})
		hashes[i] = entry.Hash()
		_, err := tree.AppendEntry(entry)
		require.NoError(t, err)
	}
	return tree, hasher, hashes
}

// rootAfterN builds a tree with exactly n entries and returns the root.
func rootAfterN(t *testing.T, n int) common.Hash {
	t.Helper()
	tree, hasher, _ := buildTestTree(t, n)
	return common.Hash(tree.SyncAndRoot(hasher))
}

// ---------------------------------------------------------------------------
// Unwind single block (remove last few entries)
// ---------------------------------------------------------------------------

func TestUnwindSingleBlock(t *testing.T) {
	// Build 20 entries, unwind to 15, check root matches a fresh 16-entry tree.
	tree, hasher, hashes := buildTestTree(t, 20)
	tree.SyncAndRoot(hasher)

	targetSN := uint64(15) // keep entries 0..15
	twigBase := targetSN &^ qmtree.TWIG_MASK
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	rootAfterUnwind := common.Hash(tree.SyncAndRoot(hasher))

	expected := rootAfterN(t, 16)
	require.Equal(t, expected, rootAfterUnwind, "unwound root must match fresh tree with same entries")
}

// ---------------------------------------------------------------------------
// Unwind multiple blocks
// ---------------------------------------------------------------------------

func TestUnwindMultipleBlocks(t *testing.T) {
	tree, hasher, hashes := buildTestTree(t, 100)
	tree.SyncAndRoot(hasher)

	// Unwind to entry 49 (keep 50 entries)
	targetSN := uint64(49)
	twigBase := targetSN &^ qmtree.TWIG_MASK
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	rootAfterUnwind := common.Hash(tree.SyncAndRoot(hasher))

	expected := rootAfterN(t, 50)
	require.Equal(t, expected, rootAfterUnwind)
}

// ---------------------------------------------------------------------------
// Unwind and re-append
// ---------------------------------------------------------------------------

func TestUnwindAndReappend(t *testing.T) {
	tree, hasher, hashes := buildTestTree(t, 30)
	tree.SyncAndRoot(hasher)

	// Unwind to 19
	targetSN := uint64(19)
	twigBase := targetSN &^ qmtree.TWIG_MASK
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	tree.SyncAndRoot(hasher)

	// Re-append different entries 20..29
	for i := 20; i < 30; i++ {
		entry := NewStateEntry(uint64(i), []StateChange{
			{Domain: kv.StorageDomain, Key: []byte{byte(i)}, Value: []byte{0xFF, byte(i)}},
		})
		_, err := tree.AppendEntry(entry)
		require.NoError(t, err)
	}
	rootNew := common.Hash(tree.SyncAndRoot(hasher))

	// Root should differ from the original 30-entry tree
	originalRoot := rootAfterN(t, 30)
	require.NotEqual(t, originalRoot, rootNew,
		"re-appending different data must produce a different root")

	// But it should also be non-zero
	require.NotEqual(t, common.Hash{}, rootNew)
}

// ---------------------------------------------------------------------------
// Unwind mid-twig
// ---------------------------------------------------------------------------

func TestUnwindMidTwig(t *testing.T) {
	// Build 1000 entries (all within first twig), unwind to 500
	tree, hasher, hashes := buildTestTree(t, 1000)
	tree.SyncAndRoot(hasher)

	targetSN := uint64(499)
	twigBase := targetSN &^ qmtree.TWIG_MASK
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	rootAfterUnwind := common.Hash(tree.SyncAndRoot(hasher))

	expected := rootAfterN(t, 500)
	require.Equal(t, expected, rootAfterUnwind)
}

// ---------------------------------------------------------------------------
// Unwind at twig boundary
// ---------------------------------------------------------------------------

func TestUnwindTwigBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping twig boundary test in short mode")
	}

	// Build exactly 2048+512 entries (crosses first twig boundary)
	total := qmtree.LEAF_COUNT_IN_TWIG + 512
	tree, hasher, hashes := buildTestTree(t, total)
	tree.SyncAndRoot(hasher)

	// Unwind to exactly the twig boundary - 1 (last entry of first twig)
	targetSN := uint64(qmtree.LEAF_COUNT_IN_TWIG - 1)
	twigBase := targetSN &^ qmtree.TWIG_MASK
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	rootAfterUnwind := common.Hash(tree.SyncAndRoot(hasher))

	expected := rootAfterN(t, qmtree.LEAF_COUNT_IN_TWIG)
	require.Equal(t, expected, rootAfterUnwind)
}

// ---------------------------------------------------------------------------
// Unwind idempotent
// ---------------------------------------------------------------------------

func TestUnwindIdempotent(t *testing.T) {
	tree, hasher, hashes := buildTestTree(t, 50)
	tree.SyncAndRoot(hasher)

	targetSN := uint64(29)
	twigBase := targetSN &^ qmtree.TWIG_MASK

	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	root1 := common.Hash(tree.SyncAndRoot(hasher))

	// Unwind again to the same point
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	root2 := common.Hash(tree.SyncAndRoot(hasher))

	require.Equal(t, root1, root2, "unwinding to same point twice must be idempotent")
}

// ---------------------------------------------------------------------------
// Unwind to zero
// ---------------------------------------------------------------------------

func TestUnwindToZero(t *testing.T) {
	tree, hasher, hashes := buildTestTree(t, 50)
	tree.SyncAndRoot(hasher)

	// Unwind to SN 0 (keep only first entry)
	tree.UnwindTo(0, hashes[0:1])
	rootAfterUnwind := common.Hash(tree.SyncAndRoot(hasher))

	expected := rootAfterN(t, 1)
	require.Equal(t, expected, rootAfterUnwind)
}

// ---------------------------------------------------------------------------
// Unwind proof consistency
// ---------------------------------------------------------------------------

func TestUnwindProofConsistency(t *testing.T) {
	tree, hasher, hashes := buildTestTree(t, 50)
	tree.SyncAndRoot(hasher)

	targetSN := uint64(24)
	twigBase := targetSN &^ qmtree.TWIG_MASK
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	tree.SyncAndRoot(hasher)

	// Get a proof for an entry that still exists
	proof, err := tree.GetProof(10)
	require.NoError(t, err)

	_, err = qmtree.CheckProof(hasher, &proof)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Unwind max depth (500 blocks simulated)
// ---------------------------------------------------------------------------

func TestUnwindMaxDepth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping max depth unwind test in short mode")
	}

	// Simulate 600 blocks with 3 tx each = 1800 entries, unwind 500 blocks (1500 entries)
	totalEntries := 1800
	tree, hasher, hashes := buildTestTree(t, totalEntries)
	tree.SyncAndRoot(hasher)

	// Keep first 300 entries (100 blocks * 3 tx)
	targetSN := uint64(299)
	twigBase := targetSN &^ qmtree.TWIG_MASK
	tree.UnwindTo(targetSN, hashes[twigBase:targetSN+1])
	rootAfterUnwind := common.Hash(tree.SyncAndRoot(hasher))

	expected := rootAfterN(t, 300)
	require.Equal(t, expected, rootAfterUnwind,
		"unwind of 500 blocks (1500 entries) must match fresh tree")
}
