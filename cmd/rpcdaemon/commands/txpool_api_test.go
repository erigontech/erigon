package commands

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	txPoolProto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

func TestTxPoolContent(t *testing.T) {
	m, require := stages.MockWithTxPool(t), require.New(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	}, false /* intermediateHashes */)
	require.NoError(err)
	err = m.InsertChain(chain)
	require.NoError(err)

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	txPool := txpool.NewTxpoolClient(conn)
	ff := rpchelper.New(ctx, nil, txPool, txpool.NewMiningClient(conn), func() {}, m.Log)
	agg := m.HistoryV3Components()
	br, _ := m.NewBlocksIO()
	api := NewTxPoolAPI(NewBaseApi(ff, kvcache.New(kvcache.DefaultCoherentConfig), br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs), m.DB, txPool)

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
