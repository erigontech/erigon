// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"

	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/log/v3"

	txpool "github.com/ledgerwatch/erigon-lib/gointerfaces/txpoolproto"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
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
	b, err := rlp.EncodeToBytes(types.NewBlockWithHeader(&types.Header{Number: new(big.Int).SetUint64(expect)}))
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
