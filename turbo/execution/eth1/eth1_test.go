package eth1_test

import (
	"context"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/stretchr/testify/require"
)

func TestInsertGetterHeader(t *testing.T) {
	bn := uint64(2)
	header := &types.Header{
		Difficulty: big.NewInt(0),
		Number:     big.NewInt(int64(bn)),
	}
	db := memdb.NewTestDB(t)
	tx, _ := db.BeginRw(context.TODO())
	rawdb.WriteTd(tx, libcommon.Hash{}, 1, libcommon.Big0)
	tx.Commit()
	e := eth1.NewEthereumExecutionModule(nil, db, nil, nil, nil, nil, nil, false)
	_, err := e.InsertHeaders(context.TODO(), &execution.InsertHeadersRequest{
		Headers: []*execution.Header{
			eth1_utils.HeaderToHeaderRPC(header),
		}})
	require.NoError(t, err)
	resp, err := e.GetHeader(context.TODO(), &execution.GetSegmentRequest{
		BlockHash:   gointerfaces.ConvertHashToH256(header.Hash()),
		BlockNumber: &bn,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Header.BlockNumber, bn)
}

func TestInsertGetterBody(t *testing.T) {
	bn := uint64(2)
	bhash := libcommon.Hash{1}
	txs := [][]byte{{1}}
	body := &types.RawBody{
		Transactions: txs,
	}
	e := eth1.NewEthereumExecutionModule(nil, memdb.NewTestDB(t), nil, nil, nil, nil, nil, false)
	_, err := e.InsertBodies(context.TODO(), &execution.InsertBodiesRequest{
		Bodies: []*execution.BlockBody{
			eth1_utils.ConvertRawBlockBodyToRpc(body, bn, bhash),
		}})
	require.NoError(t, err)
	resp, err := e.GetBody(context.TODO(), &execution.GetSegmentRequest{
		BlockHash:   gointerfaces.ConvertHashToH256(bhash),
		BlockNumber: &bn,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Body.BlockHash, gointerfaces.ConvertHashToH256(bhash))
}
