package commands

import (
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/stretchr/testify/require"
)

func TestPendingBlock(t *testing.T) {
	ctx, conn := createTestGrpcConn(t)
	mining := txpool.NewMiningClient(conn)
	ff := filters.New(ctx, nil, nil, mining)
	api := NewEthAPI(NewBaseApi(ff), nil, nil, nil, mining, 5000000)
	expect := uint64(12345)
	b, err := rlp.EncodeToBytes(types.NewBlockWithHeader(&types.Header{Number: big.NewInt(int64(expect))}))
	require.NoError(t, err)
	ch := make(chan *types.Block, 1)
	defer close(ch)
	id := api.filters.SubscribePendingBlock(ch)
	defer api.filters.UnsubscribePendingBlock(id)

	api.filters.HandlePendingBlock(&txpool.OnPendingBlockReply{RplBlock: b})
	block := api.pendingBlock()

	require.Equal(t, block.Number().Uint64(), expect)
	select {
	case got := <-ch:
		require.Equal(t, expect, got.Number().Uint64())
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout waiting for  expected notification")
	}
}

func TestPendingLogs(t *testing.T) {
	ctx, conn := createTestGrpcConn(t)
	mining := txpool.NewMiningClient(conn)
	ff := filters.New(ctx, nil, nil, mining)
	api := NewEthAPI(NewBaseApi(ff), nil, nil, nil, mining, 5000000)
	expect := []byte{211}

	ch := make(chan types.Logs, 1)
	defer close(ch)
	id := api.filters.SubscribePendingLogs(ch)
	defer api.filters.UnsubscribePendingLogs(id)

	b, err := rlp.EncodeToBytes([]*types.Log{{Data: expect}})
	require.NoError(t, err)
	api.filters.HandlePendingLogs(&txpool.OnPendingLogsReply{RplLogs: b})
	select {
	case logs := <-ch:
		require.Equal(t, expect, logs[0].Data)
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout waiting for  expected notification")
	}
}
