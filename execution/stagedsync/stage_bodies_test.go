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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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

	m := execmoduletester.New(t)
	require := require.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
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
		h.Number.SetUint64(i)
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
	m := execmoduletester.New(t)
	db := m.DB
	tx, err := db.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	_, bw := m.BlocksIO()

	h, b := testingHeaderBody(t)

	logEvery := time.NewTicker(time.Second)
	defer logEvery.Stop()

	for i := uint64(1); i <= 10; i++ {
		h.Number.SetUint64(i)
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

func TestBlockHashStageKeepsBadHeaderUnindexed(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	bad := &types.Header{Extra: []byte("bad")}
	bad.Number.SetUint64(1)
	require.NoError(t, rawdb.WriteHeader(tx, bad))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, bad.Hash(), 1))
	require.NoError(t, rawdb.TruncateCanonicalHash(tx, 1, true))

	good := &types.Header{}
	good.Number.SetUint64(1)
	require.NoError(t, rawdb.WriteHeader(tx, good))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Headers, 1))

	s := &stagedsync.StageState{ID: stages.BlockHashes}
	require.NoError(t, stagedsync.SpawnBlockHashStage(s, tx))

	require.Nil(t, rawdb.ReadHeaderNumber(tx, bad.Hash()))
	progress, err := stages.GetStageProgress(tx, stages.BlockHashes)
	require.NoError(t, err)
	require.Equal(t, uint64(1), progress)
}

func writeCanonicalChain(t *testing.T, tx kv.RwTx, n uint64) []common.Hash {
	hashes := make([]common.Hash, n+1)
	for i := uint64(1); i <= n; i++ {
		h := &types.Header{ParentHash: hashes[i-1]}
		h.Number.SetUint64(i)
		hashes[i] = h.Hash()
		require.NoError(t, rawdb.WriteHeader(tx, h))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, hashes[i], i))
	}
	return hashes
}

func TestBadBlockUnwindKeepsValidAncestorsIndexed(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const n = 5
	hashes := writeCanonicalChain(t, tx, n)

	u := &stagedsync.UnwindState{ID: stages.Headers, UnwindPoint: n - 3, Reason: stagedsync.BadBlock(hashes[n], errors.New("bad"))}
	s := &stagedsync.StageState{ID: stages.Headers}
	require.NoError(t, stagedsync.HeadersUnwind(m.Ctx, u, s, tx, stagedsync.StageHeadersCfg(m.BlockReader)))

	for _, i := range []uint64{n - 2, n - 1} {
		num := rawdb.ReadHeaderNumber(tx, hashes[i])
		require.NotNil(t, num, "valid ancestor %d lost its hash index", i)
		require.Equal(t, i, *num)
		badNum, err := rawdb.ReadBadHeaderNumber(tx, hashes[i])
		require.NoError(t, err)
		require.Nil(t, badNum, "valid ancestor %d marked bad", i)
	}
	for _, i := range []uint64{n - 2, n - 1, n} {
		canonical, err := rawdb.ReadCanonicalHash(tx, i)
		require.NoError(t, err)
		require.Equal(t, common.Hash{}, canonical, "block %d still canonical", i)
	}
	canonical, err := rawdb.ReadCanonicalHash(tx, n-3)
	require.NoError(t, err)
	require.Equal(t, hashes[n-3], canonical)
	require.Nil(t, rawdb.ReadHeaderNumber(tx, hashes[n]))
	badNum, err := rawdb.ReadBadHeaderNumber(tx, hashes[n])
	require.NoError(t, err)
	require.Equal(t, uint64(n), *badNum)
}

func TestBadBlockUnwindWithoutHashIndexMarksWholeRangeBad(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const n = 5
	hashes := writeCanonicalChain(t, tx, n)
	require.NoError(t, tx.Delete(kv.HeaderNumber, hashes[n][:]))

	u := &stagedsync.UnwindState{ID: stages.Headers, UnwindPoint: n - 3, Reason: stagedsync.BadBlock(hashes[n], errors.New("bad"))}
	s := &stagedsync.StageState{ID: stages.Headers}
	require.NoError(t, stagedsync.HeadersUnwind(m.Ctx, u, s, tx, stagedsync.StageHeadersCfg(m.BlockReader)))

	for _, i := range []uint64{n - 2, n - 1, n} {
		require.Nil(t, rawdb.ReadHeaderNumber(tx, hashes[i]), "block %d kept its hash index", i)
		badNum, err := rawdb.ReadBadHeaderNumber(tx, hashes[i])
		require.NoError(t, err)
		require.Equal(t, i, *badNum, "block %d not marked bad", i)
	}
	require.Equal(t, uint64(n-3), *rawdb.ReadHeaderNumber(tx, hashes[n-3]))
}
