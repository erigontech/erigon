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

package state

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

func TestHistoryConvertTxNumLookups(t *testing.T) {
	const txs, stepSize, keys = 160, 16, 31
	db, ii, _ := filledInvIndexOfSize(t, txs, stepSize, keys, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for step := range kv.Step(txs/stepSize - 1) {
		require.NoError(t, ii.collateBuildIntegrate(t.Context(), step, tx, background.NewProgressSet()))
	}
	require.NoError(t, tx.Commit())

	ic := ii.beginForTests()
	defer ic.Close()
	require.NotEmpty(t, ic.files)
	filesEnd := ic.files[len(ic.files)-1].endTxNum
	for _, keyNum := range []uint64{1, 7, 13, 31} {
		var key [8]byte
		binary.BigEndian.PutUint64(key[:], keyNum)
		for txNum := uint64(0); txNum <= filesEnd+1; txNum++ {
			var want uint64
			for m := keyNum; m < txNum && m < filesEnd; m += keyNum {
				want = m
			}
			got, ok, err := lastWriteBefore(ic, key[:], txNum)
			require.NoError(t, err)
			require.Equal(t, want != 0, ok, "key %d txNum %d", keyNum, txNum)
			require.Equal(t, want, got, "key %d txNum %d", keyNum, txNum)
		}
		for _, f := range ic.files {
			var want []uint64
			for m := keyNum; m < f.endTxNum; m += keyNum {
				if m >= f.startTxNum {
					want = append(want, m)
				}
			}
			got, err := appendFileTxNums(nil, ic, f.startTxNum, f.endTxNum, key[:])
			require.NoError(t, err)
			require.Equal(t, want, got, "key %d file %d-%d", keyNum, f.startTxNum, f.endTxNum)
		}
	}
}
