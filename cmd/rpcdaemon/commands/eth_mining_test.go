package commands

import (
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

func TestPendingBlock(t *testing.T) {
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, stages.Mock(t))
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, nil, nil, mining, func() {})
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	engine := ethash.NewFaker()
	api := NewEthAPI(NewBaseApi(ff, stateCache, snapshotsync.NewBlockReader(), nil, false, rpccfg.DefaultEvmCallTimeout, engine), nil, nil, nil, mining, 5000000, 100_000)
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
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, stages.Mock(t))
	mining := txpool.NewMiningClient(conn)
	ff := rpchelper.New(ctx, nil, nil, mining, func() {})
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
