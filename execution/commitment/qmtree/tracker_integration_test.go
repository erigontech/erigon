package qmtree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTracker_LoadFromDisk loads a real qmtree dataset from disk and verifies
// that witnesses can be generated and verified.
//
// To run against the n1 dataset:
//
//	QMTREE_SNAP_DIR=/erigon/qmdb-test-datadir/snapshots \
//	  go test ./execution/commitment/qmtree/... -run TestTracker_LoadFromDisk -v -timeout 120s
//
// The snap dir must be the parent of the qmtree/ subdirectory
// (i.e. <datadir>/snapshots, not <datadir>/snapshots/qmtree).
func TestTracker_LoadFromDisk(t *testing.T) {
	snapDir := os.Getenv("QMTREE_SNAP_DIR")
	if snapDir == "" {
		t.Skip("QMTREE_SNAP_DIR not set — point to <datadir>/snapshots")
	}

	const stepSize = 1_562_500
	tracker, err := NewTracker(snapDir, stepSize)
	require.NoError(t, err)
	defer tracker.Close()

	err = tracker.LoadFromDisk()
	require.NoError(t, err)

	require.Greater(t, tracker.NextSN, uint64(0), "expected entries after LoadFromDisk")

	root := tracker.SyncRoot()
	require.NotEqual(t, [32]byte{}, [32]byte(root), "root must be non-zero")

	t.Logf("loaded %d entries, root=%s", tracker.NextSN, root.Hex())

	// Verify witnesses for the first few serial numbers.
	hasher := &Keccak256Hasher{}
	checkCount := uint64(3)
	if tracker.NextSN < checkCount {
		checkCount = tracker.NextSN
	}
	for sn := uint64(0); sn < checkCount; sn++ {
		w, err := tracker.GetWitness(sn)
		require.NoError(t, err, "GetWitness sn=%d", sn)
		require.Equal(t, root, w.Proof.Root, "witness root must match tree root, sn=%d", sn)
		require.NoError(t, w.Verify(hasher), "witness should verify for sn=%d", sn)
		t.Logf("sn=%d verified OK (prevLeaf=%s)", sn, w.PreviousLeafHash.Hex())
	}

	// Also verify a witness near the middle and end of the loaded range.
	for _, sn := range []uint64{tracker.NextSN / 2, tracker.NextSN - 1} {
		w, err := tracker.GetWitness(sn)
		require.NoError(t, err, "GetWitness sn=%d", sn)
		require.Equal(t, root, w.Proof.Root, "witness root must match tree root, sn=%d", sn)
		require.NoError(t, w.Verify(hasher), "witness should verify for sn=%d", sn)
		t.Logf("sn=%d verified OK", sn)
	}
}
