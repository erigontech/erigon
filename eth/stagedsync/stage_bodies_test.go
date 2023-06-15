package stagedsync_test

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
)

func TestBodiesUnwind(t *testing.T) {
	require := require.New(t)
	m := stages2.Mock(t)
	db := m.DB
	tx, err := db.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	_, bw := m.NewBlocksIO()

	txn := &types.DynamicFeeTransaction{Tip: u256.N1, FeeCap: u256.N1, ChainID: u256.N1, CommonTx: types.CommonTx{Value: u256.N1, Gas: 1, Nonce: 1}}
	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)
	rlpTxn := buf.Bytes()

	logEvery := time.NewTicker(time.Second)
	defer logEvery.Stop()

	b := &types.RawBody{Transactions: [][]byte{rlpTxn, rlpTxn, rlpTxn}}
	h := &types.Header{}
	for i := uint64(1); i <= 10; i++ {
		h.Number = big.NewInt(int64(i))
		hash := h.Hash()
		err = bw.WriteHeader(tx, h)
		require.NoError(err)
		err = rawdb.WriteCanonicalHash(tx, hash, i)
		require.NoError(err)
		_, err = bw.WriteRawBodyIfNotExists(tx, hash, i, b)
		require.NoError(err)
	}

	err = bw.MakeBodiesCanonical(tx, 1)
	require.NoError(err)

	{
		n, err := tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+10*(3+2), int(n)) // genesis 2 system txs + from 1, 10 block with 3 txn in each

		if m.HistoryV3 {
			lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
			require.NoError(err)
			require.Equal(10, int(lastBlockNum))
			require.Equal(1+10*(3+2), int(lastTxNum))
		}

		err = bw.MakeBodiesNonCanonical(tx, 5+1) // block 5 already canonical, start from next one
		require.NoError(err)

		n, err = tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+10*(3+2), int(n)) // genesis 2 system txs + from 1, 5 block with 3 txn in each

		if m.HistoryV3 {
			lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
			require.NoError(err)
			require.Equal(5, int(lastBlockNum))
			require.Equal(1+5*(3+2), int(lastTxNum))
		}
	}
	{
		_, err = bw.WriteRawBodyIfNotExists(tx, libcommon.Hash{11}, 11, b)
		require.NoError(err)
		err = rawdb.WriteCanonicalHash(tx, libcommon.Hash{11}, 11)
		require.NoError(err)

		err = bw.MakeBodiesCanonical(tx, 5+1) // block 5 already canonical, start from next one
		require.NoError(err)
		n, err := tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n))

		n, err = tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n))

		if m.HistoryV3 {
			lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
			require.NoError(err)
			require.Equal(11, int(lastBlockNum))
			require.Equal(1+11*(3+2), int(lastTxNum))
		}
	}

	{
		// unwind to block 5, means mark blocks >= 6 as non-canonical
		err = bw.MakeBodiesNonCanonical(tx, 5+1)
		require.NoError(err)

		n, err := tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n)) // from 0, 5 block with 3 txn in each

		if m.HistoryV3 {
			lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
			require.NoError(err)
			require.Equal(5, int(lastBlockNum))
			require.Equal(1+5*(3+2), int(lastTxNum))
		}

		err = bw.MakeBodiesCanonical(tx, 5+1) // block 5 already canonical, start from next one
		require.NoError(err)
		n, err = tx.ReadSequence(kv.EthTx)
		require.NoError(err)
		require.Equal(2+11*(3+2), int(n))

		if m.HistoryV3 {
			lastBlockNum, lastTxNum, err := rawdbv3.TxNums.Last(tx)
			require.NoError(err)
			require.Equal(11, int(lastBlockNum))
			require.Equal(1+11*(3+2), int(lastTxNum))
		}
	}
}
