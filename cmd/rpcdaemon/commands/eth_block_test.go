package commands

import (
	"context"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/assert"
)

// Gets the latest block number with the latest tag
func TestGetBlockByNumberWithLatestTag(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	b, err := api.GetBlockByNumber(context.Background(), rpc.LatestBlockNumber, false)
	expected := common.HexToHash("0x6804117de2f3e6ee32953e78ced1db7b20214e0d8c745a03b8fecf7cc8ee76ef")
	if err != nil {
		t.Errorf("error getting block number with latest tag: %s", err)
	}
	assert.Equal(t, expected, b["hash"])
}

func TestGetBlockByNumberWithPendingTag(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	m := stages.MockWithTxPool(t)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, nil, txPool, txpool.NewMiningClient(conn), func() {})

	expected := 1
	header := &types.Header{
		Number: big.NewInt(int64(expected)),
	}

	rlpBlock, err := rlp.EncodeToBytes(types.NewBlockWithHeader(header))
	if err != nil {
		t.Errorf("failed encoding the block: %s", err)
	}
	ff.HandlePendingBlock(&txpool.OnPendingBlockReply{
		RplBlock: rlpBlock,
	})

	api := NewEthAPI(NewBaseApi(ff, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	b, err := api.GetBlockByNumber(context.Background(), rpc.PendingBlockNumber, false)
	if err != nil {
		t.Errorf("error getting block number with pending tag: %s", err)
	}
	assert.Equal(t, (*hexutil.Big)(big.NewInt(int64(expected))), b["number"])
}

func TestGetBlockByNumber_WithFinalizedTag_NoFinalizedBlockInDb(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	ctx := context.Background()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	if _, err := api.GetBlockByNumber(ctx, rpc.FinalizedBlockNumber, false); err != nil {
		assert.ErrorIs(t, rpchelper.UnknownBlockError, err)
	}
}

func TestGetBlockByNumber_WithSafeTag_NoSafeBlockInDb(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	ctx := context.Background()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false), db, nil, nil, nil, 5000000)
	if _, err := api.GetBlockByNumber(ctx, rpc.SafeBlockNumber, false); err != nil {
		assert.ErrorIs(t, rpchelper.UnknownBlockError, err)
	}
}
