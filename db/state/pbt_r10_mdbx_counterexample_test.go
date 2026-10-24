// Copyright 2026 The Erigon Authors
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

package state_test

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
)

type pbinR10SerialTx struct {
	kv.Tx
	busy atomic.Bool
}

func (tx *pbinR10SerialTx) Cursor(table string) (kv.Cursor, error) {
	if !tx.busy.CompareAndSwap(false, true) {
		return nil, errors.New("MDBX transaction used concurrently")
	}
	defer tx.busy.Store(false)
	time.Sleep(10 * time.Millisecond)
	return tx.Tx.Cursor(table)
}

func (tx *pbinR10SerialTx) CursorDupSort(table string) (kv.CursorDupSort, error) {
	if !tx.busy.CompareAndSwap(false, true) {
		return nil, errors.New("MDBX transaction used concurrently")
	}
	defer tx.busy.Store(false)
	time.Sleep(10 * time.Millisecond)
	return tx.Tx.CursorDupSort(table)
}

func TestPBinR10StampWalkUsesMDBXSerially(t *testing.T) {
	selectPBinLeafStreamHash(t)
	db, agg := testDbAndAggregatorv3(t, 8)
	writePBinLeafStreamStates(t, db, 8, []pbinLeafStreamAccount{{address: bytes.Repeat([]byte{0x80}, 20), nonce: 1, balance: 1}})
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	at := agg.BeginFilesRo()
	defer at.Close()
	guarded := &pbinR10SerialTx{Tx: tx}
	err = state.VerifyPBinSourceStamps(t.Context(), at, guarded, false, 15, 2, nil, nil)
	require.NoError(t, err, "valid MDBX rows must verify without concurrent access to their transaction; got %v", err)
}
