package qmtree

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	dbcfg "github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
)

// TestTracker_MDBX_RoundTrip writes entries via AppendLeaf with an MDBX tx,
// then creates a fresh tracker and loads from MDBX, verifying that roots
// and witnesses match.
func TestTracker_MDBX_RoundTrip(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	const stepSize = 100
	const numEntries = 250 // 2.5 steps

	// Phase 1: write entries to MDBX.
	tracker1, err := NewTracker("", stepSize)
	require.NoError(t, err)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	tracker1.SetTx(tx)

	for i := 0; i < numEntries; i++ {
		var pre, sc, trans [32]byte
		pre[0] = byte(i)
		sc[1] = byte(i)
		trans[2] = byte(i)
		tracker1.AppendLeaf(pre, sc, trans)
	}
	tracker1.Flush()
	root1 := tracker1.SyncRoot()
	nextTxNum1 := tracker1.NextTxNum

	require.Equal(t, uint64(numEntries), nextTxNum1)
	require.NotEqual(t, [32]byte{}, [32]byte(root1))

	// Verify a witness from the first tracker.
	w1, err := tracker1.GetWitness(0)
	require.NoError(t, err)
	require.NoError(t, w1.Verify(&Keccak256Hasher{}))

	require.NoError(t, tx.Commit())

	// Phase 2: load into a fresh tracker from MDBX.
	tracker2, err := NewTracker("", stepSize)
	require.NoError(t, err)

	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	err = tracker2.LoadFromDB(roTx)
	require.NoError(t, err)
	require.Equal(t, nextTxNum1, tracker2.NextTxNum)

	root2 := tracker2.SyncRoot()
	require.Equal(t, root1, root2, "roots must match after LoadFromDB")

	// Verify the same witness from the loaded tracker.
	w2, err := tracker2.GetWitness(0)
	require.NoError(t, err)
	require.NoError(t, w2.Verify(&Keccak256Hasher{}))
	require.Equal(t, w1.Proof.Root, w2.Proof.Root)

	t.Logf("round-trip OK: %d entries, root=%s", nextTxNum1, root1.Hex())
}

// TestTracker_MDBX_WithKeyIndex tests that key writes survive round-trip.
func TestTracker_MDBX_WithKeyIndex(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	const stepSize = 100

	tracker, err := NewTracker("", stepSize)
	require.NoError(t, err)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	tracker.SetTx(tx)

	// Write entries with key hashes.
	for i := 0; i < 10; i++ {
		var pre, sc, trans [32]byte
		pre[0] = byte(i)
		tracker.AppendLeaf(pre, sc, trans)
		kh := KeyHash(0, []byte(fmt.Sprintf("key%d", i)))
		tracker.NotifyKeyWrites([]common.Hash{kh}, uint64(i))
	}
	tracker.Flush()
	require.NoError(t, tx.Commit())

	// Verify key-index entries in MDBX.
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	for i := 0; i < 10; i++ {
		kh := KeyHash(0, []byte(fmt.Sprintf("key%d", i)))
		txNum, found, err := GetKeyIndex(roTx, kh)
		require.NoError(t, err)
		require.True(t, found, "key%d not found in MDBX", i)
		require.Equal(t, uint64(i), txNum)
	}
}
