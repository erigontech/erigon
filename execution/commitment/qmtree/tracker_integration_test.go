package qmtree

import (
	"fmt"
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

	hasher := &Keccak256Hasher{}

	// verifySN returns nil if the witness verifies, or the error.
	verifySN := func(sn uint64) error {
		w, err := tracker.GetWitness(sn)
		if err != nil {
			return fmt.Errorf("GetWitness: %w", err)
		}
		if root != w.Proof.Root {
			return fmt.Errorf("root mismatch: want %s, got %s", root.Hex(), w.Proof.Root.Hex())
		}
		return w.Verify(hasher)
	}

	// Verify witnesses for the first few serial numbers.
	checkCount := uint64(3)
	if tracker.NextSN < checkCount {
		checkCount = tracker.NextSN
	}
	for sn := uint64(0); sn < checkCount; sn++ {
		require.NoError(t, verifySN(sn), "witness should verify for sn=%d", sn)
		ld := tracker.leafData[sn]
		t.Logf("sn=%d verified OK (prevLeaf=%s)", sn, ld.PreviousLeafHash.Hex())
	}

	// Binary-search for the first failing SN to diagnose chain breaks.
	if tracker.NextSN > checkCount {
		firstFail := findFirstFailing(tracker.NextSN, checkCount, func(sn uint64) bool {
			return verifySN(sn) != nil
		})

		if firstFail == tracker.NextSN {
			// All pass — run the standard middle/end checks.
			for _, sn := range []uint64{tracker.NextSN / 2, tracker.NextSN - 1} {
				require.NoError(t, verifySN(sn), "witness should verify for sn=%d", sn)
				t.Logf("sn=%d verified OK", sn)
			}
			t.Logf("ALL %d witnesses verified OK", tracker.NextSN)
		} else {
			// Diagnose the first failure.
			diagnoseMismatch(t, tracker, hasher, firstFail)
			t.Fatalf("first failing SN: %d (twig %d, pos %d)",
				firstFail, firstFail>>TWIG_SHIFT, firstFail&TWIG_MASK)
		}
	}
}

// findFirstFailing returns the smallest sn in [lo, total) where failing(sn)==true,
// or returns total if none fail.
func findFirstFailing(total, lo uint64, failing func(uint64) bool) uint64 {
	hi := total
	for lo < hi {
		mid := (lo + hi) / 2
		if failing(mid) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// diagnoseMismatch prints detailed info about a failing SN and the SN before it.
func diagnoseMismatch(t *testing.T, tracker *Tracker, hasher Hasher, sn uint64) {
	t.Helper()

	ld := tracker.leafData[sn]
	computed := ld.LeafHash()
	t.Logf("=== Diagnosis for failing sn=%d (twig=%d, pos=%d) ===",
		sn, sn>>TWIG_SHIFT, sn&TWIG_MASK)
	t.Logf("  PreStateHash:    %s", ld.PreStateHash.Hex())
	t.Logf("  StateChangeHash: %s", ld.StateChangeHash.Hex())
	t.Logf("  TransitionHash:  %s", ld.TransitionHash.Hex())
	t.Logf("  PreviousLeafHash (from chain): %s", ld.PreviousLeafHash.Hex())
	t.Logf("  Computed LeafHash: %s", computed.Hex())

	if sn > 0 {
		prev := tracker.leafData[sn-1]
		prevLeafHash := prev.LeafHash()
		t.Logf("  leafData[sn-1].LeafHash(): %s", prevLeafHash.Hex())
		if ld.PreviousLeafHash != prevLeafHash {
			t.Logf("  CHAIN BREAK: PreviousLeafHash != leafHash(sn-1)")
		} else {
			t.Logf("  chain is consistent: PreviousLeafHash == leafHash(sn-1)")
		}
	}

	// Read what the twig file says the leaf hash is.
	proof, err := tracker.tree.GetProof(sn)
	if err != nil {
		t.Logf("  GetProof error: %v", err)
		return
	}
	storedLeaf := proof.LeftOfTwig[0].SelfHash
	t.Logf("  Stored leaf (twig file): %s", storedLeaf.Hex())
	if computed == storedLeaf {
		t.Logf("  MATCH: computed == stored (mismatch is elsewhere)")
	} else {
		t.Logf("  MISMATCH: computed=%s vs stored=%s", computed.Hex(), storedLeaf.Hex())
	}

	// Check the proof verification result.
	proof.LeftOfTwig[0].SelfHash = computed
	if err := proof.Check(hasher, true); err != nil {
		t.Logf("  Proof.Check with computed hash: FAIL (%v)", err)
	} else {
		t.Logf("  Proof.Check with computed hash: OK (computed hash gives valid proof)")
	}
}
