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

package stagedsync_test

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
)

func testingHeaderBody(t *testing.T) (h *types.Header, b *types.RawBody) {
	t.Helper()

	txn := &types.DynamicFeeTransaction{TipCap: u256.N1, FeeCap: u256.N1, ChainID: u256.N1, CommonTx: types.CommonTx{Value: u256.N1, GasLimit: 1, Nonce: 1}}
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(t, err)
	rlpTxn := buf.Bytes()

	b = &types.RawBody{Transactions: [][]byte{rlpTxn, rlpTxn, rlpTxn}}
	h = &types.Header{}
	return h, b
}

func TestBodiesCanonical(t *testing.T) {
	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	m := mock.Mock(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require := require.New(t)
	require.NoError(err)
	defer tx.Rollback()
	m.HistoryV3 = true

	_, bw := m.BlocksIO()

	logEvery := time.NewTicker(time.Second)
	defer logEvery.Stop()

	h, b := testingHeaderBody(t)

	for i := uint64(1); i <= 10; i++ {
		if i == 3 {
			// if latest block is <=1, append delta check is disabled, so no sense to test it here.
			// INSTEAD we make first block canonical, write some blocks and then test append with gap
			err = bw.MakeBodiesCanonical(tx, 1)
			require.NoError(err)
		}
		h.Number = new(big.Int).SetUint64(i)
		hash := h.Hash()
		err = rawdb.WriteHeader(tx, h)
		require.NoError(err)
		err = rawdb.WriteCanonicalHash(tx, hash, i)
		require.NoError(err)
		_, err = rawdb.WriteRawBodyIfNotExists(tx, hash, i, b)
		require.NoError(err)
	}

	// test append with gap
	err = rawdb.AppendCanonicalTxNums(tx, 5)
	require.Error(err)
	var e1 rawdbv3.ErrTxNumsAppendWithGap
	require.ErrorAs(err, &e1)

	// this should see same error inside then retry from last block available, therefore return no error
	err = bw.MakeBodiesCanonical(tx, 5)
	require.NoError(err)
}

func TestBodiesUnwind(t *testing.T) {
	require := require.New(t)
	m := mock.Mock(t)
	db := m.DB
	tx, err := db.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	_, bw := m.BlocksIO()

	h, b := testingHeaderBody(t)

	logEvery := time.NewTicker(time.Second)
	defer logEvery.Stop()

	for i := uint64(1); i <= 10; i++ {
		h.Number = new(big.Int).SetUint64(i)
		hash := h.Hash()
		err = rawdb.WriteHeader(tx, h)
		require.NoError(err)
		err = rawdb.WriteCanonicalHash(tx, hash, i)
		require.NoError(err)
		_, err = rawdb.WriteRawBodyIfNotExists(tx, hash, i, b)
		require.NoError(err)
	}

	err = bw.MakeBodiesCanonical(tx, 1)
	require.NoError(err)

	{
		n, err := tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+10*(3+2), int(n)) // genesis 2 system txs + from 1, 10 block with 3 txn in each

		lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
		require.NoError(err)
		require.Equal(10, int(lastBlockNum))
		require.Equal(1+10*(3+2), int(lastTxNum))

		err = bw.MakeBodiesNonCanonical(tx, 5+1) // block 5 already canonical, start from next one
		require.NoError(err)

		n, err = tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+10*(3+2), int(n)) // genesis 2 system txs + from 1, 5 block with 3 txn in each

		lastBlockNum, lastTxNum, err = rawdbv3.TxNums.Last(tx)
		require.NoError(err)
		require.Equal(5, int(lastBlockNum))
		require.Equal(1+5*(3+2), int(lastTxNum))
	}
	{
		_, err = rawdb.WriteRawBodyIfNotExists(tx, common.Hash{11}, 11, b)
		require.NoError(err)
		err = rawdb.WriteCanonicalHash(tx, common.Hash{11}, 11)
		require.NoError(err)

		err = bw.MakeBodiesCanonical(tx, 5+1) // block 5 already canonical, start from next one
		require.NoError(err)
		n, err := tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n))

		n, err = tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n))

		lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
		require.NoError(err)
		require.Equal(11, int(lastBlockNum))
		require.Equal(1+11*(3+2), int(lastTxNum))
	}

	{
		// unwind to block 5, means mark blocks >= 6 as non-canonical
		err = bw.MakeBodiesNonCanonical(tx, 5+1)
		require.NoError(err)

		n, err := tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n)) // from 0, 5 block with 3 txn in each

		lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
		require.NoError(err)
		require.Equal(5, int(lastBlockNum))
		require.Equal(1+5*(3+2), int(lastTxNum))

		err = bw.MakeBodiesCanonical(tx, 5+1) // block 5 already canonical, start from next one
		require.NoError(err)
		n, err = tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n))

		lastBlockNum, lastTxNum, err = rawdbv3.TxNums.Last(tx)
		require.NoError(err)
		require.Equal(11, int(lastBlockNum))
		require.Equal(1+11*(3+2), int(lastTxNum))
	}
}
