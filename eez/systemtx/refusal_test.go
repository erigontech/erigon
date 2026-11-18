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
	"encoding/json"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/txnprovider/txpool"
)

// specInbound is an App. F.3 version-0 inbound transaction, type 0x45 under
// the spec's production rules: 0x45 ‖ rlp([version, chainId, sourceHash,
// gasLimit, to, value, input]).
func specInbound(t *testing.T, chainID uint64) []byte {
	t.Helper()
	body, err := rlp.EncodeToBytes([]any{
		uint64(0),
		chainID,
		common.Hash{1},
		uint64(16_777_216),
		common.HexToAddress("0xee50000000000000000000000000000000000000"),
		uint64(0),
		[]byte{1, 2, 3, 4},
	})
	require.NoError(t, err)
	return append([]byte{0x45}, body...)
}

// TestTxpoolRefuses is the txpool part of 1.11 and of S4.3 (App. F.3.1): the
// pool's own parser, used for RPC submissions and gossip, knows none of the
// system types.
func TestTxpoolRefuses(t *testing.T) {
	for _, tc := range []struct {
		chainID uint64
		enc     []byte
	}{
		{1, common.FromHex(minimalSystemTx)},
		{901, nativeVector(t, "native-source-base").ExpectedBytes},
		{901, nativeVector(t, "native-history-advance").ExpectedBytes},
		{1, specInbound(t, 1)},
	} {
		wrapped, err := rlp.EncodeToBytes(tc.enc)
		require.NoError(t, err)
		parser := txpool.NewTxnParseContext(*uint256.NewInt(tc.chainID))
		for _, p := range []struct {
			payload     []byte
			hasEnvelope bool
		}{{tc.enc, false}, {wrapped, true}} {
			_, err := parser.ParseTransaction(p.payload, 0, &txpool.TxnSlot{}, make([]byte, 20), p.hasEnvelope, false, nil)
			require.ErrorContains(t, err, "unknown transaction type")
		}
		// A Transactions or PooledTransactions packet.
		packet, err := rlp.EncodeToBytes([][]byte{tc.enc})
		require.NoError(t, err)
		_, err = txpool.ParseTransactions(packet, 0, parser, &txpool.TxnSlots{}, nil)
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

// TestInboundRefused is S4.3 (App. F.3.1): type 0x45, unsigned, is refused
// over the raw-transaction RPC, by the signing APIs, and by the transactions
// the RPC builds from call arguments. TestTxpoolRefuses covers the pool and
// gossip.
func TestInboundRefused(t *testing.T) {
	m := execmoduletester.New(t, execmoduletester.WithTxPool())
	chain, err := m.GenerateChain(1, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chain))
	ctx, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	api := jsonrpc.NewEthAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, txpoolproto.NewTxpoolClient(conn), nil,
		// Unprotected transactions allowed, so that only the type refuses them.
		&rpccfg.EthApiConfig{GasCap: 5_000_000, FeeCap: ethconfig.Defaults.RPCTxFeeCap, AllowUnprotectedTxs: true, RpcTxSyncDefaultTimeout: time.Second, RpcTxSyncMaxTimeout: time.Second},
		log.New())
	chainID := m.ChainConfig.ChainID.Uint64()

	// The spec's envelope, which no decoder takes yet, and a decodable 0x45,
	// which the pool refuses.
	native, err := (&protocol.NativeTx{ChainID: chainID, GasLimit: 1, To: protocol.RollupXEEZL2Address}).MarshalBinary()
	require.NoError(t, err)
	for _, tc := range []struct {
		enc    []byte
		reason string
	}{
		{specInbound(t, chainID), "invalid native transaction"},
		{native, "unknown transaction type"},
	} {
		_, err := api.SendRawTransaction(ctx, tc.enc)
		require.ErrorContains(t, err, tc.reason)
		_, err = api.SendRawTransactionSync(ctx, tc.enc, nil)
		require.ErrorContains(t, err, tc.reason)
	}

	_, err = api.Sign(ctx, common.Address{}, hexutil.Bytes{0x45})
	require.ErrorContains(t, err, "eth_sign")
	_, err = api.SignTransaction(ctx, map[string]any{"type": "0x45"})
	require.ErrorContains(t, err, "eth_signTransaction")
	_, err = api.SendTransaction(ctx, map[string]any{"type": "0x45"})
	require.ErrorContains(t, err, "eth_sendTransaction")

	// eth_fillTransaction and eth_call pick the type from the fields set.
	var args ethapi.CallArgs
	require.NoError(t, json.Unmarshal([]byte(`{"type":"0x45","to":"0xee50000000000000000000000000000000000000","input":"0x01020304"}`), &args))
	built, err := args.ToTransaction(5_000_000, uint256.NewInt(1))
	require.NoError(t, err)
	require.NotEqual(t, byte(0x45), built.Type())
}
