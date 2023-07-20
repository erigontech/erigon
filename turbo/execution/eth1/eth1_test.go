package eth1_test

import (
	"context"
	"math/big"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1"
	"github.com/stretchr/testify/require"
)

func TestInsertGetterHeader(t *testing.T) {
	bn := uint64(2)
	header := &types.Header{
		Difficulty: big.NewInt(0),
		Number:     big.NewInt(int64(bn)),
	}
	e := eth1.NewEthereumExecutionModule(nil, memdb.NewTestDB(t))
	_, err := e.InsertHeaders(context.TODO(), &execution.InsertHeadersRequest{
		Headers: []*execution.Header{
			eth1.HeaderToHeaderRPC(header),
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
	e := eth1.NewEthereumExecutionModule(nil, memdb.NewTestDB(t))
	_, err := e.InsertBodies(context.TODO(), &execution.InsertBodiesRequest{
		Bodies: []*execution.BlockBody{
			eth1.ConvertRawBlockBodyToRpc(body, bn, bhash),
		}})
	require.NoError(t, err)
	resp, err := e.GetBody(context.TODO(), &execution.GetSegmentRequest{
		BlockHash:   gointerfaces.ConvertHashToH256(bhash),
		BlockNumber: &bn,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Body.BlockHash, gointerfaces.ConvertHashToH256(bhash))
}
