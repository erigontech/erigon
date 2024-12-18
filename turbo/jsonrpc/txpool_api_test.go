package jsonrpc

import (
	"bytes"
	"fmt"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces/txpool"
	txPoolProto "github.com/erigontech/erigon-lib/gointerfaces/txpool"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func TestTxPoolContent(t *testing.T) {
	m, require := mock.MockWithTxPool(t), require.New(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	require.NoError(err)
	err = m.InsertChain(chain)
	require.NoError(err)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, txPool, txpool.NewMiningClient(conn), func() {}, m.Log)
	agg := m.HistoryV3Components()
	api := NewTxPoolAPI(NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), m.BlockReader, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, txPool)

	expectValue := uint64(1234)
	txn, err := types.SignTx(types.NewTransaction(0, libcommon.Address{1}, uint256.NewInt(expectValue), params.TxGas, uint256.NewInt(10*params.GWei), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(err)

	buf := bytes.NewBuffer(nil)
	err = txn.MarshalBinary(buf)
	require.NoError(err)

	reply, err := txPool.Add(ctx, &txpool.AddRequest{RlpTxs: [][]byte{buf.Bytes()}})
	require.NoError(err)
	for _, res := range reply.Imported {
		require.Equal(res, txPoolProto.ImportResult_SUCCESS, fmt.Sprintf("%s", reply.Errors))
	}

	content, err := api.Content(ctx)
	require.NoError(err)

	sender := m.Address.String()
	require.Equal(1, len(content["pending"][sender]))
	require.Equal(expectValue, content["pending"][sender]["0"].Value.ToInt().Uint64())

	status, err := api.Status(ctx)
	require.NoError(err)
	require.Len(status, 3)
	require.Equal(status["pending"], hexutil.Uint(1))
	require.Equal(status["queued"], hexutil.Uint(0))
}
