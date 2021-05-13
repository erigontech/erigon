package commands

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/stretchr/testify/require"
)

func TestPendingBlock(t *testing.T) {
	conn := createTestGrpcConn()
	defer conn.Close()
	mining := txpool.NewMiningClient(conn)
	ff := filters.New(context.Background(), nil, nil, mining)
	api := NewEthAPI(NewBaseApi(ff), nil, nil, nil, mining, 5000000)
	expect := uint64(12345)
	b, err := rlp.EncodeToBytes(types.NewBlockWithHeader(&types.Header{Number: big.NewInt(int64(expect))}))
	require.NoError(t, err)
	txsCh := make(chan []types.Transaction, 1)
	defer close(txsCh)
	id := api.filters.SubscribePendingTxs(txsCh)
	defer api.filters.UnsubscribePendingTxs(id)

	api.filters.HandlePendingBlock(&txpool.OnPendingBlockReply{RplBlock: b})
	block := api.pendingBlock()

	require.Equal(t, block.Number().Uint64(), expect)
}

func TestPendingLogs(t *testing.T) {
	conn := createTestGrpcConn()
	defer conn.Close()
	mining := txpool.NewMiningClient(conn)
	ff := filters.New(context.Background(), nil, nil, mining)
	api := NewEthAPI(NewBaseApi(ff), nil, nil, nil, mining, 5000000)
	expect := []byte{211}

	ctx, cancel := context.WithCancel(context.Background())
	api.filters.SubscribePendingLogs(ctx, func(logs types.Logs) {
		require.Equal(t, expect, logs[0].Data)
		cancel()
	})
	b, err := rlp.EncodeToBytes([]*types.Log{{Data: expect}})
	require.NoError(t, err)
	api.filters.HandlePendingLogs(&txpool.OnPendingLogsReply{RplLogs: b})
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout waiting for  expected notification")
	}
}
