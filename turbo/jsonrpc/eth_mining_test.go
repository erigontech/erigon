package jsonrpc

import (
	"math/big"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon/consensus/ethash"
	"github.com/erigontech/erigon/rpc/rpccfg"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/gointerfaces/txpool"

	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestPendingBlock(t *testing.T) {
	m := mock.Mock(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, mock.Mock(t))
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	engine := ethash.NewFaker()
	api := NewEthAPI(NewBaseApi(ff, stateCache, m.BlockReader, nil, false, rpccfg.DefaultEvmCallTimeout, engine,
		m.Dirs), nil, nil, nil, mining, 5000000, 1e18, 100_000, false, 100_000, 128, log.New())
	expect := uint64(12345)
	b, err := rlp.EncodeToBytes(types.NewBlockWithHeader(&types.Header{Number: big.NewInt(int64(expect))}))
	require.NoError(t, err)
	ch, id := ff.SubscribePendingBlock(1)
	defer ff.UnsubscribePendingBlock(id)

	ff.HandlePendingBlock(&txpool.OnPendingBlockReply{RplBlock: b})
	block := api.pendingBlock()

	require.Equal(t, block.NumberU64(), expect)
	select {
	case got := <-ch:
		require.Equal(t, expect, got.NumberU64())
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout waiting for  expected notification")
	}
}

func TestPendingLogs(t *testing.T) {
	m := mock.Mock(t)
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, mining, func() {}, m.Log)
	expect := []byte{211}

	ch, id := ff.SubscribePendingLogs(1)
	defer ff.UnsubscribePendingLogs(id)

	b, err := rlp.EncodeToBytes([]*types.Log{{Data: expect}})
	require.NoError(t, err)
	ff.HandlePendingLogs(&txpool.OnPendingLogsReply{RplLogs: b})
	select {
	case logs := <-ch:
		require.Equal(t, expect, logs[0].Data)
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout waiting for  expected notification")
	}
}
