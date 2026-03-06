package poc

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/qmtree"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// StateEntry hash determinism
// ---------------------------------------------------------------------------

func TestStateEntryHashDeterminism(t *testing.T) {
	// Same changes in different insertion order must produce the same hash.
	a := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}}
	b := StateChange{Domain: kv.StorageDomain, Key: []byte{0x02}, Value: []byte{0xBB}}

	e1 := NewStateEntry(0, []StateChange{a, b})
	e2 := NewStateEntry(0, []StateChange{b, a}) // reversed order

	require.Equal(t, e1.Hash(), e2.Hash(), "hash must be deterministic regardless of input order")
}

func TestStateEntryHashDeterminismSameDomain(t *testing.T) {
	a := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}}
	b := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x02}, Value: []byte{0xBB}}

	e1 := NewStateEntry(0, []StateChange{a, b})
	e2 := NewStateEntry(0, []StateChange{b, a})

	require.Equal(t, e1.Hash(), e2.Hash())
}

// ---------------------------------------------------------------------------
// StateEntry hash uniqueness
// ---------------------------------------------------------------------------

func TestStateEntryHashUniqueness(t *testing.T) {
	a := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}}
	b := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xBB}}

	e1 := NewStateEntry(0, []StateChange{a})
	e2 := NewStateEntry(0, []StateChange{b})

	require.NotEqual(t, e1.Hash(), e2.Hash(), "different values must produce different hashes")
}

func TestStateEntryHashUniqueDomains(t *testing.T) {
	a := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}}
	b := StateChange{Domain: kv.StorageDomain, Key: []byte{0x01}, Value: []byte{0xAA}}

	e1 := NewStateEntry(0, []StateChange{a})
	e2 := NewStateEntry(0, []StateChange{b})

	require.NotEqual(t, e1.Hash(), e2.Hash(), "different domains must produce different hashes")
}

func TestStateEntryHashUniqueKeys(t *testing.T) {
	a := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}}
	b := StateChange{Domain: kv.AccountsDomain, Key: []byte{0x02}, Value: []byte{0xAA}}

	e1 := NewStateEntry(0, []StateChange{a})
	e2 := NewStateEntry(0, []StateChange{b})

	require.NotEqual(t, e1.Hash(), e2.Hash(), "different keys must produce different hashes")
}

// ---------------------------------------------------------------------------
// StateEntry empty changes
// ---------------------------------------------------------------------------

func TestStateEntryEmptyChanges(t *testing.T) {
	e := NewStateEntry(42, nil)
	require.NotEqual(t, common.Hash{}, e.Hash(), "empty entry must have non-zero hash")
	require.Equal(t, uint64(42), e.SerialNumber())
	require.Nil(t, e.Changes())
}

func TestStateEntryEmptyVsSingleChange(t *testing.T) {
	empty := NewStateEntry(0, nil)
	single := NewStateEntry(0, []StateChange{
		{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}},
	})
	require.NotEqual(t, empty.Hash(), single.Hash())
}

// ---------------------------------------------------------------------------
// StateEntry implements qmtree.Entry
// ---------------------------------------------------------------------------

func TestStateEntryImplementsEntry(t *testing.T) {
	e := NewStateEntry(99, []StateChange{
		{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}},
	})
	// Verify it satisfies the interface
	var _ qmtree.Entry = e
	require.Equal(t, uint64(99), e.SerialNumber())
	require.Equal(t, int64(0), e.Len())
	require.NotEqual(t, common.Hash{}, e.Hash())
}

// ---------------------------------------------------------------------------
// Single block tree build (in-memory, no DB)
// ---------------------------------------------------------------------------

func TestSingleBlockTreeBuild(t *testing.T) {
	hasher := &qmtree.Keccak256Hasher{}
	tree := qmtree.NewTree(hasher, 0, nil, nil)

	// Simulate a block with 3 transactions
	entries := []*StateEntry{
		NewStateEntry(0, []StateChange{
			{Domain: kv.AccountsDomain, Key: []byte{0x01}, Value: []byte{0xAA}},
		}),
		NewStateEntry(1, []StateChange{
			{Domain: kv.AccountsDomain, Key: []byte{0x02}, Value: []byte{0xBB}},
			{Domain: kv.StorageDomain, Key: []byte{0x01, 0x00}, Value: []byte{0xCC}},
		}),
		NewStateEntry(2, nil), // empty tx
	}

	for _, e := range entries {
		_, err := tree.AppendEntry(e)
		require.NoError(t, err)
	}

	root := common.Hash(tree.SyncAndRoot(hasher))
	require.NotEqual(t, common.Hash{}, root, "root must be non-zero")

	// Determinism: sync again should produce the same root
	root2 := common.Hash(tree.SyncAndRoot(hasher))
	require.Equal(t, root, root2, "root must be deterministic")
}

// ---------------------------------------------------------------------------
// Multi-block tree build
// ---------------------------------------------------------------------------

func TestMultiBlockTreeBuild(t *testing.T) {
	hasher := &qmtree.Keccak256Hasher{}
	tree := qmtree.NewTree(hasher, 0, nil, nil)

	var sn uint64
	roots := make([]common.Hash, 10)

	for block := 0; block < 10; block++ {
		// Each block has 2-3 tx with varying changes
		txCount := 2 + (block % 2)
		for tx := 0; tx < txCount; tx++ {
			changes := []StateChange{
				{
					Domain: kv.AccountsDomain,
					Key:    []byte{byte(block), byte(tx)},
					Value:  []byte{byte(block + tx + 1)},
				},
			}
			entry := NewStateEntry(sn, changes)
			_, err := tree.AppendEntry(entry)
			require.NoError(t, err)
			sn++
		}
		roots[block] = common.Hash(tree.SyncAndRoot(hasher))
	}

	// Each block should produce a different root
	for i := 1; i < len(roots); i++ {
		require.NotEqual(t, roots[i-1], roots[i],
			"block %d and %d should have different roots", i-1, i)
	}

	// No root should be zero
	for i, r := range roots {
		require.NotEqual(t, common.Hash{}, r, "block %d root must be non-zero", i)
	}
}

// ---------------------------------------------------------------------------
// Multi-block determinism: rebuild tree with same data, same roots
// ---------------------------------------------------------------------------

func TestMultiBlockDeterminism(t *testing.T) {
	buildTree := func() []common.Hash {
		hasher := &qmtree.Keccak256Hasher{}
		tree := qmtree.NewTree(hasher, 0, nil, nil)
		var sn uint64
		roots := make([]common.Hash, 5)
		for block := 0; block < 5; block++ {
			for tx := 0; tx < 3; tx++ {
				entry := NewStateEntry(sn, []StateChange{
					{Domain: kv.AccountsDomain, Key: []byte{byte(block), byte(tx)}, Value: []byte{byte(sn)}},
				})
				_, err := tree.AppendEntry(entry)
				require.NoError(t, err)
				sn++
			}
			roots[block] = common.Hash(tree.SyncAndRoot(hasher))
		}
		return roots
	}

	roots1 := buildTree()
	roots2 := buildTree()
	require.Equal(t, roots1, roots2, "identical data must produce identical roots")
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

func TestWriteResultsCSV(t *testing.T) {
	results := []BlockResult{
		{BlockNum: 1, Root: common.Hash{0xAA}, TxCount: 3, Changes: 5, Elapsed: 100},
		{BlockNum: 2, Root: common.Hash{0xBB}, TxCount: 2, Changes: 3, Elapsed: 200},
	}

	var buf bytes.Buffer
	err := writeResultsCSV(&buf, results)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "block_num,root,tx_count,changes,elapsed_ns")
	require.Contains(t, output, "1,")
	require.Contains(t, output, "2,")
}

// ---------------------------------------------------------------------------
// Large tree: cross twig boundary
// ---------------------------------------------------------------------------

func TestTreeCrossTwigBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping large tree test in short mode")
	}

	hasher := &qmtree.Keccak256Hasher{}
	tree := qmtree.NewTree(hasher, 0, nil, nil)

	// Append enough entries to cross a twig boundary (2048 + 10)
	total := uint64(qmtree.LEAF_COUNT_IN_TWIG + 10)
	for sn := uint64(0); sn < total; sn++ {
		entry := NewStateEntry(sn, []StateChange{
			{Domain: kv.AccountsDomain, Key: []byte{byte(sn >> 8), byte(sn)}, Value: []byte{byte(sn)}},
		})
		_, err := tree.AppendEntry(entry)
		require.NoError(t, err)
	}

	root := common.Hash(tree.SyncAndRoot(hasher))
	require.NotEqual(t, common.Hash{}, root)

	// Verify a proof for an entry in the second twig
	proof, err := tree.GetProof(qmtree.LEAF_COUNT_IN_TWIG + 5)
	require.NoError(t, err)
	_, err = qmtree.CheckProof(hasher, &proof)
	require.NoError(t, err)
}
