package stagedsync

import (
	"bytes"
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/u256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/stretchr/testify/require"
)

func TestBodiesUnwind(t *testing.T) {
	require := require.New(t)
	_, tx := memdb.NewTestTx(t)
	txn := &types.DynamicFeeTransaction{ChainID: u256.N1, Tip: u256.N1, FeeCap: u256.N1, CommonTx: types.CommonTx{Value: u256.N1, Gas: 1, Nonce: 1}}
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(err)
	rlpTxn := buf.Bytes()

	b := &types.RawBody{Transactions: [][]byte{rlpTxn, rlpTxn, rlpTxn}}
	for i := uint64(1); i <= 10; i++ {
		err = rawdb.WriteRawBody(tx, common.Hash{}, i, b)
		require.NoError(err)
	}
	err = stages.SaveStageProgress(tx, stages.Bodies, 10)
	require.NoError(err)

	u := &UnwindState{ID: stages.Bodies, UnwindPoint: 5}
	err = UnwindBodiesStage(u, tx, BodiesCfg{}, context.Background())
	require.NoError(err)

	n, err := tx.ReadSequence(kv.EthTx)
	require.NoError(err)
	require.Equal(15, int(n))

	err = rawdb.WriteRawBody(tx, common.Hash{}, 6, b)
	require.NoError(err)

	n, err = tx.ReadSequence(kv.EthTx)
	require.NoError(err)
	require.Equal(18, int(n))
}
