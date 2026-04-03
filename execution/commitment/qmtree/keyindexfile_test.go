package qmtree

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

func makeTestKeyHash(domain byte, i int) common.Hash {
	var buf [5]byte
	buf[0] = domain
	binary.BigEndian.PutUint32(buf[1:5], uint32(i))
	return crypto.Keccak256Hash(buf[:])
}

func TestKeyIndexFile_FlushAndLoad(t *testing.T) {
	dir := t.TempDir()
	kf, err := NewKeyIndexFile(dir)
	require.NoError(t, err)
	defer kf.Close()

	entries := make([]KeyIndexEntry, 100)
	for i := range entries {
		entries[i] = KeyIndexEntry{
			KeyHash: makeTestKeyHash(0, i),
			TxNum:   uint64(i * 17),
		}
	}

	err = kf.FlushDelta(context.Background(), entries, 0, 1)
	require.NoError(t, err)
	require.Equal(t, 1, kf.SegmentCount())

	// Lookup each key.
	for _, e := range entries {
		txNum, found := kf.Lookup(e.KeyHash)
		require.True(t, found, "key not found: %x", e.KeyHash)
		require.Equal(t, e.TxNum, txNum)
	}

	// Close and reload.
	kf.Close()

	kf2, err := NewKeyIndexFile(dir)
	require.NoError(t, err)
	defer kf2.Close()

	maxStep, err := kf2.LoadAll()
	require.NoError(t, err)
	require.Equal(t, uint64(1), maxStep)
	require.Equal(t, 1, kf2.SegmentCount())

	for _, e := range entries {
		txNum, found := kf2.Lookup(e.KeyHash)
		require.True(t, found)
		require.Equal(t, e.TxNum, txNum)
	}
}

func TestKeyIndexFile_IncrementalFlush(t *testing.T) {
	dir := t.TempDir()
	kf, err := NewKeyIndexFile(dir)
	require.NoError(t, err)
	defer kf.Close()

	// First flush: keys 0-49.
	entries1 := make([]KeyIndexEntry, 50)
	for i := range entries1 {
		entries1[i] = KeyIndexEntry{KeyHash: makeTestKeyHash(0, i), TxNum: uint64(i)}
	}
	require.NoError(t, kf.FlushDelta(context.Background(), entries1, 0, 1))

	// Second flush: keys 25-74 with newer txNums.
	entries2 := make([]KeyIndexEntry, 50)
	for i := range entries2 {
		entries2[i] = KeyIndexEntry{KeyHash: makeTestKeyHash(0, i+25), TxNum: uint64(i + 100)}
	}
	require.NoError(t, kf.FlushDelta(context.Background(), entries2, 1, 2))

	// Keys 25-49: newer segment wins (searched newest first).
	for i := 25; i < 50; i++ {
		txNum, found := kf.Lookup(makeTestKeyHash(0, i))
		require.True(t, found)
		require.Equal(t, uint64(i-25+100), txNum, "key %d", i)
	}
}

func TestKeyIndexFile_Truncate(t *testing.T) {
	dir := t.TempDir()
	kf, err := NewKeyIndexFile(dir)
	require.NoError(t, err)
	defer kf.Close()

	for step := uint64(0); step < 3; step++ {
		entries := []KeyIndexEntry{
			{KeyHash: makeTestKeyHash(0, int(step)), TxNum: step * 100},
		}
		require.NoError(t, kf.FlushDelta(context.Background(), entries, step, step+1))
	}
	require.Equal(t, 3, kf.SegmentCount())

	kf.TruncateAfterStep(2)
	require.Equal(t, 2, kf.SegmentCount())

	// Key from step 0 should still work.
	txNum, found := kf.Lookup(makeTestKeyHash(0, 0))
	require.True(t, found)
	require.Equal(t, uint64(0), txNum)
}
