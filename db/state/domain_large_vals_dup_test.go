// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

const (
	testDupKeysTable = "TestLargeValsKeys"
	testDupValsTable = "TestLargeValsVals"
)

func newLargeValsDupTestDB(t *testing.T) kv.RwDB {
	t.Helper()
	db := mdbx.New(dbcfg.ChainDB, log.New()).InMem(t.TempDir()).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			testDupKeysTable: {Flags: kv.DupSort},
			testDupValsTable: {},
		}
	}).MustOpen()
	t.Cleanup(db.Close)
	return db
}

func putLargeValsDup(t *testing.T, tx kv.RwTx, bareKey []byte, invStep, seqID uint64, val []byte) {
	t.Helper()
	var dup [dupRecordLen]byte
	binary.BigEndian.PutUint64(dup[:8], invStep)
	binary.BigEndian.PutUint64(dup[8:], seqID)
	require.NoError(t, tx.Put(testDupKeysTable, bareKey, dup[:]))
	if seqID != deletionSeqID {
		var seqKey [8]byte
		binary.BigEndian.PutUint64(seqKey[:], seqID)
		require.NoError(t, tx.Put(testDupValsTable, seqKey[:], val))
	}
}

// TestDeleteLargeValuesDup_RemovesEveryDupAtStep pins that the helper clears the
// whole (bareKey, invStep) group. Repeated unwind/flush cycles can leave more
// than one dup at the same step, and a survivor would shadow the restored value
// and strand its valsTable row.
func TestDeleteLargeValuesDup_RemovesEveryDupAtStep(t *testing.T) {
	t.Parallel()
	db := newLargeValsDupTestDB(t)
	tx, err := db.BeginRw(t.Context()) //nolint:gocritic
	require.NoError(t, err)
	defer tx.Rollback()

	bareKey := []byte("dup-key-0000")
	const targetStep = 7
	putLargeValsDup(t, tx, bareKey, targetStep, 100, []byte("first"))
	putLargeValsDup(t, tx, bareKey, targetStep, 101, []byte("second"))
	// A dup at another step must survive untouched.
	putLargeValsDup(t, tx, bareKey, targetStep+1, 102, []byte("other-step"))

	keysCursor, err := tx.RwCursorDupSort(testDupKeysTable)
	require.NoError(t, err)
	defer keysCursor.Close()
	valsCursor, err := tx.RwCursor(testDupValsTable)
	require.NoError(t, err)
	defer valsCursor.Close()

	var invStep [8]byte
	binary.BigEndian.PutUint64(invStep[:], targetStep)
	require.NoError(t, deleteLargeValuesDup(keysCursor, valsCursor, bareKey, invStep[:]))

	var remainingSteps []uint64
	for _, dup, err := keysCursor.SeekExact(bareKey); dup != nil; _, dup, err = keysCursor.NextDup() {
		require.NoError(t, err)
		remainingSteps = append(remainingSteps, binary.BigEndian.Uint64(dup[:8]))
	}
	require.Equal(t, []uint64{targetStep + 1}, remainingSteps,
		"every dup at the target step must be gone")

	for _, seqID := range []uint64{100, 101} {
		var seqKey [8]byte
		binary.BigEndian.PutUint64(seqKey[:], seqID)
		v, err := tx.GetOne(testDupValsTable, seqKey[:])
		require.NoError(t, err)
		require.Empty(t, v, "valsTable row for seqID %d must be deleted", seqID)
	}

	var survivorKey [8]byte
	binary.BigEndian.PutUint64(survivorKey[:], 102)
	v, err := tx.GetOne(testDupValsTable, survivorKey[:])
	require.NoError(t, err)
	require.Equal(t, []byte("other-step"), v)
}
