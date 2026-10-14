// Copyright 2026 The Erigon Authors
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
package systemtx_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// TestTxpoolRefuses is the txpool part of 1.11: the pool's own parser,
// used for RPC submissions and gossip, does not know type 0x76.
func TestTxpoolRefuses(t *testing.T) {
	enc := common.FromHex(minimalSystemTx)
	wrapped, err := rlp.EncodeToBytes(enc)
	require.NoError(t, err)

	parser := txpool.NewTxnParseContext(*mustNew(t, protocol.SystemTx{ChainID: 1, To: protocol.EEZL2Address}).GetChainID())
	for _, tc := range []struct {
		payload     []byte
		hasEnvelope bool
	}{{enc, false}, {wrapped, true}} {
		_, err := parser.ParseTransaction(tc.payload, 0, &txpool.TxnSlot{}, make([]byte, 20), tc.hasEnvelope, false, nil)
		require.ErrorContains(t, err, "unknown transaction type")
	}
}

func TestSendRawTransactionRefuses(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithTxPool())
	chain, err := m.GenerateChain(1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chain))

	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	api := jsonrpc.NewEthAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, txpoolproto.NewTxpoolClient(conn), nil,
		&rpccfg.EthApiConfig{GasCap: 5_000_000, FeeCap: ethconfig.Defaults.RPCTxFeeCap, RpcTxSyncDefaultTimeout: time.Second, RpcTxSyncMaxTimeout: time.Second},
		log.New())

	tx := mustNew(t, protocol.SystemTx{ChainID: m.ChainConfig.ChainID.Uint64(), To: protocol.EEZL2Address, Input: []byte{1}})
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))
	_, err = api.SendRawTransaction(ctx, buf.Bytes())
	require.ErrorContains(t, err, "unknown transaction type")

	// eth_sendTransaction is not implemented, so it cannot build one.
	_, err = api.SendTransaction(ctx, map[string]any{"type": "0x76"})
	require.ErrorContains(t, err, "eth_sendTransaction")
}
