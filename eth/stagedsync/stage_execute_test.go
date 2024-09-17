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

package stagedsync

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/params"
	"github.com/stretchr/testify/require"
)

func apply(tx kv.RwTx, logger log.Logger) (beforeBlock, afterBlock testGenHook, w state.StateWriter) {
	domains, err := libstate.NewSharedDomains(tx, logger)
	if err != nil {
		panic(err)
	}
	rs := state.NewStateV3(domains, logger)
	stateWriter := state.NewStateWriterBufferedV3(rs, nil)
	stateWriter.SetTx(tx)

	return func(n, from, numberOfBlocks uint64) {
			stateWriter.SetTxNum(context.Background(), n)
			stateWriter.ResetWriteSet()
		}, func(n, from, numberOfBlocks uint64) {
			txTask := &state.TxTask{
				BlockNum:   n,
				Rules:      params.TestRules,
				TxNum:      n,
				TxIndex:    0,
				Final:      true,
				WriteLists: stateWriter.WriteSet(),
			}
			txTask.AccountPrevs, txTask.AccountDels, txTask.StoragePrevs, txTask.CodePrevs = stateWriter.PrevAndDels()
			rs.SetTxNum(txTask.TxNum, txTask.BlockNum)
			if err := rs.ApplyState4(context.Background(), txTask); err != nil {
				panic(err)
			}
			_, err := rs.Domains().ComputeCommitment(context.Background(), true, txTask.BlockNum, "")
			if err != nil {
				panic(err)
			}

			if n == from+numberOfBlocks-1 {
				if err := domains.Flush(context.Background(), tx); err != nil {
					panic(err)
				}
			}
		}, stateWriter
}

func TestSaveReceipt(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db, _ := temporaltest.NewTestDB(t, dirs)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	doms, err := libstate.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer doms.Close()
	doms.SetTx(tx)

	doms.SetTxNum(0) // block1
	err = saveReceipt(doms, 0, 0)
	require.NoError(t, err)

	doms.SetTxNum(1) // block1
	err = saveReceipt(doms, 1, 1)
	require.NoError(t, err)

	doms.SetTxNum(2) // block1

	doms.SetTxNum(3) // block2
	err = saveReceipt(doms, 0, 0)
	require.NoError(t, err)

	doms.SetTxNum(4) // block2
	err = saveReceipt(doms, 4, 4)
	require.NoError(t, err)

	doms.SetTxNum(5) // block2

	err = doms.Flush(context.Background(), tx)
	require.NoError(t, err)

	ttx := tx.(kv.TemporalTx)
	v, ok, err := ttx.HistorySeek(kv.ReceiptHistory, rawtemporaldb.CumulativeGasUsedInBlockKey, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Empty(t, v)

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, rawtemporaldb.CumulativeGasUsedInBlockKey, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, rawtemporaldb.CumulativeGasUsedInBlockKey, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, rawtemporaldb.CumulativeGasUsedInBlockKey, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.HistorySeek(kv.ReceiptHistory, rawtemporaldb.CumulativeGasUsedInBlockKey, 4)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	//block1
	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey, nil, 0)
	require.NoError(t, err)
	require.False(t, ok)

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey, nil, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey, nil, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	//block2
	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey, nil, 3)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey, nil, 4)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(0), binary.BigEndian.Uint64(v))

	v, ok, err = ttx.DomainGetAsOf(kv.ReceiptDomain, rawtemporaldb.CumulativeGasUsedInBlockKey, nil, 5)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(4), binary.BigEndian.Uint64(v))
}
