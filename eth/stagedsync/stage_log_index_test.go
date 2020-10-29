package stagedsync

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"

	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/require"
)

func TestLogIndex(t *testing.T) {
	require := require.New(t)

	db := ethdb.NewMemDatabase()
	defer db.Close()
	tx, err := db.Begin(context.Background(), ethdb.RW)
	require.NoError(err)
	defer tx.Rollback()

	addr1, addr2 := common.HexToAddress("0x0"), common.HexToAddress("0x376c47978271565f56DEB45495afa69E59c16Ab2")
	topic1, topic2 := common.HexToHash("0x0"), common.HexToHash("0x1234")
	receipts1 := types.Receipts{{
		Logs: []*types.Log{
			{
				Address: addr1,
				Topics:  []common.Hash{topic1},
			},
			{
				Address: addr1,
				Topics:  []common.Hash{topic2},
			},
		},
	}}
	receipts2 := types.Receipts{{
		Logs: []*types.Log{
			{
				Address: addr2,
				Topics:  []common.Hash{topic2},
			},
		},
	}}
	err = rawdb.AppendReceipts(tx, 1, receipts1)
	require.NoError(err)

	err = rawdb.AppendReceipts(tx, 2, receipts2)
	require.NoError(err)

	err = promoteLogIndex("logPrefix", tx, 0, "", nil)
	require.NoError(err)

	// Check indices GetCardinality (in how many blocks they meet)
	m, err := bitmapdb.Get(tx, dbutils.LogAddressIndex, addr1[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(1, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogAddressIndex, addr2[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(1, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogTopicIndex, topic1[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(1, int(m.GetCardinality()), 0, 10_000_000)

	m, err = bitmapdb.Get(tx, dbutils.LogTopicIndex, topic2[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(2, int(m.GetCardinality()))

	// Unwind test
	err = unwindLogIndex("logPrefix", tx, 2, 1, nil)
	require.NoError(err)

	m, err = bitmapdb.Get(tx, dbutils.LogAddressIndex, addr1[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(1, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogAddressIndex, addr2[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(0, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogTopicIndex, topic1[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(1, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogTopicIndex, topic2[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(1, int(m.GetCardinality()))

	// Unwind test
	err = unwindLogIndex("logPrefix", tx, 1, 0, nil)
	require.NoError(err)

	m, err = bitmapdb.Get(tx, dbutils.LogAddressIndex, addr1[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(0, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogAddressIndex, addr2[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(0, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogTopicIndex, topic1[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(0, int(m.GetCardinality()))

	m, err = bitmapdb.Get(tx, dbutils.LogTopicIndex, topic2[:], 0, 10_000_000)
	require.NoError(err)
	require.Equal(0, int(m.GetCardinality()))
}
