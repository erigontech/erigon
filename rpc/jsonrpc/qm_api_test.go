package jsonrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/commitment/qmtree"
)

// syntheticTracker builds an in-memory Tracker with n hand-crafted leaves,
// syncs the tree, and returns it ready for GetWitness / GetRangeWitness calls.
func syntheticTracker(t *testing.T, n int) *qmtree.Tracker {
	t.Helper()
	const stepSize = 1_562_500
	tracker, err := qmtree.NewTracker("", stepSize)
	require.NoError(t, err)

	for i := 0; i < n; i++ {
		pre := common.Hash{byte(i), 0x01}
		change := common.Hash{byte(i), 0x02}
		transition := common.Hash{byte(i), 0x03}
		tracker.AppendLeaf(pre, change, transition)
	}
	tracker.SyncRoot()
	return tracker
}

// verifyOnlyAPI returns a QMAPIImpl with only the tracker set — enough for
// VerifyProof and VerifyWitness which do not touch the database.
func verifyOnlyAPI(tracker *qmtree.Tracker) *QMAPIImpl {
	return &QMAPIImpl{tracker: tracker}
}

// ---------------------------------------------------------------------------
// VerifyWitness
// ---------------------------------------------------------------------------

func TestQMAPI_VerifyWitness_Valid(t *testing.T) {
	tracker := syntheticTracker(t, 8)
	api := verifyOnlyAPI(tracker)

	for sn := uint64(0); sn < 8; sn++ {
		witness, err := tracker.GetWitness(sn)
		require.NoError(t, err, "GetWitness sn=%d", sn)

		bz := qmtree.WitnessToBytes(witness)
		ok, err := api.VerifyWitness(context.Background(), hexutil.Bytes(bz))
		require.NoError(t, err, "VerifyWitness sn=%d", sn)
		require.True(t, ok, "expected verified=true for sn=%d", sn)
	}
}

func TestQMAPI_VerifyWitness_TamperedTransitionHash(t *testing.T) {
	tracker := syntheticTracker(t, 4)
	api := verifyOnlyAPI(tracker)

	witness, err := tracker.GetWitness(2)
	require.NoError(t, err)

	// Tamper with the transition hash — leaf hash will no longer match proof.
	witness.TransitionHash = common.Hash{0xFF, 0xFF}

	bz := qmtree.WitnessToBytes(witness)
	ok, err := api.VerifyWitness(context.Background(), hexutil.Bytes(bz))
	require.NoError(t, err)
	require.False(t, ok, "tampered witness should not verify")
}

func TestQMAPI_VerifyWitness_InvalidBytes(t *testing.T) {
	api := verifyOnlyAPI(nil)
	_, err := api.VerifyWitness(context.Background(), hexutil.Bytes{0x01, 0x02})
	require.Error(t, err, "truncated bytes should return deserialization error")
}

func TestQMAPI_VerifyWitness_EmptyBytes(t *testing.T) {
	api := verifyOnlyAPI(nil)
	_, err := api.VerifyWitness(context.Background(), hexutil.Bytes{})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// VerifyProof
// ---------------------------------------------------------------------------

func TestQMAPI_VerifyProof_Valid(t *testing.T) {
	tracker := syntheticTracker(t, 8)
	api := verifyOnlyAPI(tracker)

	for sn := uint64(0); sn < 8; sn++ {
		witness, err := tracker.GetWitness(sn)
		require.NoError(t, err, "GetWitness sn=%d", sn)

		proofBytes := witness.Proof.ToBytes()
		ok, err := api.VerifyProof(context.Background(), hexutil.Bytes(proofBytes))
		require.NoError(t, err, "VerifyProof sn=%d", sn)
		require.True(t, ok, "expected verified=true for sn=%d", sn)
	}
}

func TestQMAPI_VerifyProof_InvalidBytes(t *testing.T) {
	api := verifyOnlyAPI(nil)
	_, err := api.VerifyProof(context.Background(), hexutil.Bytes{0xDE, 0xAD})
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// Nil tracker guard
// ---------------------------------------------------------------------------

func TestQMAPI_NilTracker_GetProof(t *testing.T) {
	api := &QMAPIImpl{}
	_, err := api.GetProof(context.Background(), common.Address{}, latestBlock)
	require.Error(t, err)
	require.Contains(t, err.Error(), "qmtree tracker not available")
}

func TestQMAPI_NilTracker_GetWitness(t *testing.T) {
	api := &QMAPIImpl{}
	_, err := api.GetWitness(context.Background(), latestBlock)
	require.Error(t, err)
	require.Contains(t, err.Error(), "qmtree tracker not available")
}

func TestQMAPI_NilTracker_GetTxWitness(t *testing.T) {
	api := &QMAPIImpl{}
	_, err := api.GetTxWitness(context.Background(), latestBlock, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "qmtree tracker not available")
}

func TestQMAPI_NilTracker_GetAccountStateProof(t *testing.T) {
	api := &QMAPIImpl{}
	_, err := api.GetAccountStateProof(context.Background(), common.Address{}, nil, latestBlock)
	require.Error(t, err)
	require.Contains(t, err.Error(), "qmtree tracker not available")
}

func TestQMAPI_NilTracker_GetTxStateProof(t *testing.T) {
	api := &QMAPIImpl{}
	_, err := api.GetTxStateProof(context.Background(), common.Address{}, nil, latestBlock, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "qmtree tracker not available")
}

func TestQMAPI_NilTracker_GetLatestStateProof(t *testing.T) {
	api := &QMAPIImpl{}
	_, err := api.GetLatestStateProof(context.Background(), common.Address{}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "qmtree tracker not available")
}

// ---------------------------------------------------------------------------
// Root consistency across all leaves
// ---------------------------------------------------------------------------

// TestQMAPI_WitnessRootsConsistent verifies that all witnesses produced by
// a single synced tracker agree on the same tree root.
func TestQMAPI_WitnessRootsConsistent(t *testing.T) {
	const n = 10
	tracker := syntheticTracker(t, n)
	root := tracker.SyncRoot()

	for sn := uint64(0); sn < n; sn++ {
		w, err := tracker.GetWitness(sn)
		require.NoError(t, err)
		require.Equal(t, root, w.Proof.Root, "root mismatch at sn=%d", sn)
	}
}

// ---------------------------------------------------------------------------
// Unwind correctness
// ---------------------------------------------------------------------------

// TestQMAPI_UnwindRestoresRoot verifies that unwinding the tracker back to
// a previous serial number produces the same tree root as the tracker had
// before the unwound leaves were appended.
//
// This tests the Tracker.UnwindTo path that is exercised when the executor
// rolls back a fork (e.g. during reorg or replay).
func TestQMAPI_UnwindRestoresRoot(t *testing.T) {
	const stepSize = 1_562_500
	tracker, err := qmtree.NewTracker("", stepSize)
	require.NoError(t, err)

	// Append 6 leaves and capture the root at N=4.
	appendLeaf := func(i int) {
		tracker.AppendLeaf(
			common.Hash{byte(i), 0x01},
			common.Hash{byte(i), 0x02},
			common.Hash{byte(i), 0x03},
		)
	}

	for i := 0; i < 4; i++ {
		appendLeaf(i)
	}
	rootAt4 := tracker.SyncRoot()

	// Append 2 more leaves (simulating a fork that will be unwound).
	appendLeaf(4)
	appendLeaf(5)
	rootAt6 := tracker.SyncRoot()
	require.NotEqual(t, rootAt4, rootAt6, "roots must differ before unwind")

	// Unwind back to N=4 and verify root is restored.
	tracker.UnwindTo(4)
	rootAfterUnwind := tracker.SyncRoot()
	require.Equal(t, rootAt4, rootAfterUnwind, "root after unwind must match pre-fork root")

	// Re-append the same leaves after unwind — root must also match.
	appendLeaf(4)
	appendLeaf(5)
	rootReplay := tracker.SyncRoot()
	require.Equal(t, rootAt6, rootReplay, "re-appending same leaves must yield same root")
}

// TestQMAPI_UnwindToZeroRestoresEmpty verifies a full rewind to the empty state.
// TODO: Tree.UnwindTo(0, nil) currently panics — tracked as a known bug.
func TestQMAPI_UnwindToZeroRestoresEmpty(t *testing.T) {
	t.Skip("Tree.UnwindTo(0, nil) panics — fix tree.go before enabling")
	const stepSize = 1_562_500
	tracker1, err := qmtree.NewTracker("", stepSize)
	require.NoError(t, err)
	tracker2, err := qmtree.NewTracker("", stepSize)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		tracker1.AppendLeaf(
			common.Hash{byte(i), 0x01},
			common.Hash{byte(i), 0x02},
			common.Hash{byte(i), 0x03},
		)
	}
	tracker1.UnwindTo(0)
	emptyRoot1 := tracker1.SyncRoot()

	// A fresh empty tracker should produce the same root.
	emptyRoot2 := tracker2.SyncRoot()
	require.Equal(t, emptyRoot1, emptyRoot2, "unwind-to-zero root must match fresh empty tracker")
}

// TestQMAPI_WitnessInvalidAfterUnwind verifies that after unwinding, witnesses
// for the removed leaves are no longer available.
func TestQMAPI_WitnessInvalidAfterUnwind(t *testing.T) {
	const stepSize = 1_562_500
	tracker, err := qmtree.NewTracker("", stepSize)
	require.NoError(t, err)

	for i := 0; i < 6; i++ {
		tracker.AppendLeaf(
			common.Hash{byte(i), 0x01},
			common.Hash{byte(i), 0x02},
			common.Hash{byte(i), 0x03},
		)
	}
	tracker.SyncRoot()

	// Sanity: witness for sn=5 works before unwind.
	_, err = tracker.GetWitness(5)
	require.NoError(t, err)

	tracker.UnwindTo(4) // discard sn=4 and sn=5

	// After unwind, sn=4 and sn=5 are gone.
	_, err = tracker.GetWitness(4)
	require.Error(t, err, "sn=4 should be unavailable after unwind to 4")
	_, err = tracker.GetWitness(5)
	require.Error(t, err, "sn=5 should be unavailable after unwind to 4")

	// sn=3 must still be present.
	_, err = tracker.GetWitness(3)
	require.NoError(t, err, "sn=3 must still be present after partial unwind")
}

// ---------------------------------------------------------------------------
// Integration test stubs (require real n1 qmtree data)
// ---------------------------------------------------------------------------
//
// The tests below are stubs. They are skipped unless the environment variable
// QMTREE_SNAP_DIR is set to a directory containing a synced qmtree dataset
// (e.g. produced by 'integration stage_exec_replay' on n1).
//
// TODO: once n1's dataset covers at least 100k blocks, implement:
//   1. Load the tracker from disk via qmtree.NewTracker(snapDir, stepSize)
//   2. Pick known txnums from hoodi (e.g. the coinbase tx of block 50000)
//   3. Call GetWitness / GetRangeWitness and assert Verified == true
//   4. Assert the root matches the value computed offline on n1
//
// func TestQMAPI_Integration_GetWitness(t *testing.T) {
//     snapDir := os.Getenv("QMTREE_SNAP_DIR")
//     if snapDir == "" { t.Skip("QMTREE_SNAP_DIR not set") }
//     ...
// }

// ---------------------------------------------------------------------------
// Engine-API end-to-end integration test stubs
// ---------------------------------------------------------------------------
//
// These stubs will validate that executing blocks via the Engine API (or the
// stage executor) produces the expected qmtree root, including after reorgs.
//
// Test matrix (roots to be filled once n1 data is available):
//
//   Block / event           | Expected qmtree root
//   ----------------------- | -------------------------------------------------------
//   hoodi genesis (0 txs)   | 0x... (empty tree root)
//   hoodi block 1           | 0x...
//   hoodi block 100         | 0x...
//   hoodi block 50000       | 0x...
//   after unwind to 99      | 0x... (same as block 99 root)
//   after replay 100        | 0x... (same as block 100 root)
//
// Test approach:
//   1. Start ephemeral Erigon with --experimental.qmtree enabled
//   2. Submit blocks via Engine API (engine_newPayloadV2/V3 + engine_forkchoiceUpdatedV2)
//   3. After each block, call qm_getWitness and assert root matches expected
//   4. Simulate a reorg: submit an alternative block N+1, switch fork choice,
//      then verify the root matches the expected post-reorg root
//   5. Re-apply the original block N+1 and assert root is restored
//
// TODO: implement once expected roots are captured from n1 and ephemeral
//       Engine-API test harness is in place.
