package decodedstate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

func commitDecodedEntriesViaLivePath(t *testing.T, db kv.TemporalRwDB, blockNum uint64, entries []DecodedEntry) {
	t.Helper()

	tx, err := db.BeginTemporalRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, WriteEntriesToTx(tx, blockNum, entries))
	require.NoError(t, tx.Commit())
}

func decodedHistoryRowCount(t *testing.T, db kv.RoDB) int {
	t.Helper()

	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	cursor, err := tx.Cursor(kv.DecodedHistory)
	require.NoError(t, err)
	defer cursor.Close()

	count := 0
	for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
		require.NoError(t, err)
		count++
	}

	return count
}

func TestS_UNW_06_LiveCommitPathRestoresPreviousValueOnUnwind(t *testing.T) {
	db, store := newChainDBStore(t)
	slot := slotHash(0)
	key := keyHash(42)

	entryV1 := DecodedEntry{
		Contract:    runtimeContractAddr,
		MappingSlot: slot,
		Keys:        []common.Hash{key},
		Value:       hashU64(100),
		EntryType:   MappingEntry,
	}
	entryV2 := entryV1
	entryV2.Value = hashU64(200)

	commitDecodedEntriesViaLivePath(t, db, 10, []DecodedEntry{entryV1})
	commitDecodedEntriesViaLivePath(t, db, 11, []DecodedEntry{entryV2})

	require.NoError(t, store.Unwind(10))

	got, found, err := store.GetLatest(runtimeContractAddr, slot, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(
		t,
		entryV1.Value,
		got,
		"live execution writes must record decoded history so unwind can restore the pre-reorg value",
	)
}

func TestS_PRUNE_05_LiveCommitPathCreatesDecodedHistoryForPruning(t *testing.T) {
	db, _ := newChainDBStore(t)
	slot := slotHash(1)
	key := keyHash(7)

	entryV1 := DecodedEntry{
		Contract:    runtimeContractAddr,
		MappingSlot: slot,
		Keys:        []common.Hash{key},
		Value:       hashU64(11),
		EntryType:   MappingEntry,
	}
	entryV2 := entryV1
	entryV2.Value = hashU64(22)

	commitDecodedEntriesViaLivePath(t, db, 20, []DecodedEntry{entryV1})
	commitDecodedEntriesViaLivePath(t, db, 21, []DecodedEntry{entryV2})

	require.Greater(
		t,
		decodedHistoryRowCount(t, db),
		0,
		"live execution writes must create decoded history rows so prune can remove old decoded entries",
	)
}
