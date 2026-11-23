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
package l1_test

import (
	"context"
	"crypto/tls"
	"errors"
	"math/big"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// erigonRPC returns the rpc reader of m serving JSON-RPC.
func erigonRPC(t *testing.T, m *execmoduletester.ExecModuleTester) *l1.RPC {
	t.Helper()
	logger := log.New()
	api := jsonrpc.NewEthAPI(jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, nil, nil, &rpccfg.EthApiConfig{
			GasCap:                  5_000_000,
			FeeCap:                  ethconfig.Defaults.RPCTxFeeCap,
			ReturnDataLimit:         100_000,
			RpcTxSyncDefaultTimeout: time.Second,
			RpcTxSyncMaxTimeout:     time.Second,
		}, logger)
	return serveRPC(t, api)
}

// serveRPC returns the rpc reader of a JSON-RPC server of the eth service.
func serveRPC(t *testing.T, service any) *l1.RPC {
	t.Helper()
	logger := log.New()
	server := rpc.NewServer(50, false, false, true, logger, 100)
	require.NoError(t, server.RegisterName("eth", service))
	t.Cleanup(server.Stop)
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	r, err := l1.DialRPC(context.Background(), httpServer.URL, logger)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	return r
}

// TestRPCConformance is T-L1-1 against an Erigon L1 serving JSON-RPC.
func TestRPCConformance(t *testing.T) {
	m, model := l1test.ErigonL1(t)
	l1test.Conformance(t, erigonRPC(t, m), model, l1test.SampleFilters)
}

// reverting answers every call with an execution revert whose reason reads
// like a missing block.
type reverting struct{}

type revertError struct{}

func (revertError) Error() string  { return "execution reverted: rollup not found" }
func (revertError) ErrorCode() int { return 3 }
func (revertError) ErrorData() any { return hexutil.Encode(l1test.RevertReason) }

func (reverting) Call(context.Context, map[string]any, map[string]any) (hexutil.Bytes, error) {
	return nil, revertError{}
}

// TestRPCRevert: a revert is ErrReverted whatever its reason says.
func TestRPCRevert(t *testing.T) {
	_, err := serveRPC(t, reverting{}).CallAt(t.Context(), l1test.RevertingCall, common.Hash{1})
	require.ErrorIs(t, err, l1.ErrReverted)
	require.NotErrorIs(t, err, l1.ErrNotFound)
	require.ErrorContains(t, err, "rollup not found")
}

// TestServerConformance is T-L1-1 for the rpc reader over l1test.NewServer,
// which serves a reader the way an L1 node does.
func TestServerConformance(t *testing.T) {
	_, model := l1test.ErigonL1(t)
	server, err := l1test.NewServer(model, log.New())
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)

	r, err := l1.DialRPC(context.Background(), httpServer.URL, log.New())
	require.NoError(t, err)
	t.Cleanup(r.Close)
	l1test.Conformance(t, r, model, l1test.SampleFilters)
}

func TestRPCErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		msg  string
		want error
	}{
		{"query returned more than 10000 results", l1.ErrRangeTooWide},
		{"Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range", l1.ErrRangeTooWide},
		{"query block range exceeds server limit, narrow your filter: 1000", l1.ErrRangeTooWide},
		{"query returns too many logs, narrow your filter: 20000", l1.ErrRangeTooWide},
		{"finalized block not found", l1.ErrNotFound},
		{"unknown block", l1.ErrNotFound},
	} {
		require.ErrorIs(t, l1.MapRPCError(errors.New(tc.msg), "call"), tc.want, tc.msg)
	}
	err := l1.MapRPCError(errors.New("connection refused"), "call")
	require.False(t, errors.Is(err, l1.ErrRangeTooWide) || errors.Is(err, l1.ErrNotFound))

	for _, msg := range []string{`hash "0x1f" is not currently canonical`, "0x1f block is not canonical"} {
		err = l1.MapRPCError(errors.New(msg), "call")
		require.ErrorIs(t, err, l1.ErrNotCanonical, msg)
		require.ErrorIs(t, err, l1.ErrNotFound, msg)
	}
	for _, msg := range []string{"block not found: 0x1f", "block 11 is not executed (last executed: 10)"} {
		err = l1.MapRPCError(errors.New(msg), "call")
		require.ErrorIs(t, err, l1.ErrNotFound, msg)
		require.NotErrorIs(t, err, l1.ErrNotCanonical, msg)
	}

	// EIP-4444 history expiry: the source will never serve the block.
	for _, e := range []error{errors.New("pruned history unavailable"), prunedError{}} {
		err = l1.MapRPCError(e, "call")
		require.ErrorIs(t, err, l1.ErrNotFound, e.Error())
		require.ErrorIs(t, err, l1.ErrBeforeHistory, e.Error())
	}
}

// prunedError is the JSON-RPC error of a node that expired a block's
// history.
type prunedError struct{}

func (prunedError) Error() string  { return "history expired" }
func (prunedError) ErrorCode() int { return 4444 }

// TestRPCTransportError checks that a failed connection prints the call, the
// socket operation, the remote address and the system error only.
func TestRPCTransportError(t *testing.T) {
	reset := &url.Error{Op: "Post", URL: "http://127.0.0.1:18545", Err: &net.OpError{
		Op:     "read",
		Net:    "tcp",
		Source: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 57349},
		Addr:   &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 18545},
		Err:    &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
	}}
	err := l1.MapRPCError(reset, "eth_getBlockByNumber(latest)")
	require.EqualError(t, err, "eth_getBlockByNumber(latest): read tcp 127.0.0.1:18545: read: connection reset by peer")
	require.ErrorIs(t, err, syscall.ECONNRESET)
	var ue *url.Error
	require.ErrorAs(t, err, &ue)

	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	r, err := l1.DialRPC(t.Context(), "http://"+addr, log.New())
	require.NoError(t, err)
	defer r.Close()
	require.NotErrorIs(t, r.SendRawTransaction(t.Context(), []byte{0x02}), l1.ErrRefused, "no answer refuses it")
	_, err = r.ChainID(t.Context())
	// The system error's text and errno are the platform's.
	require.True(t, strings.HasPrefix(err.Error(), "eth_chainId: dial tcp "+addr+": "), err.Error())
	require.NotContains(t, err.Error(), "http://")
	var op *net.OpError
	require.ErrorAs(t, err, &op)
	require.Equal(t, "dial", op.Op)
}

// TestRPCURLError checks that a failure with no connection error, as an
// untrusted certificate, prints the endpoint without its path and query,
// where providers put API keys, and keeps the cause.
// TestRedactURL: an endpoint prints its scheme and host only.
func TestRedactURL(t *testing.T) {
	for raw, want := range map[string]string{
		"wss://user:secret@l1.example/v3/KEY?key=KEY": "wss://l1.example",
		"https://l1.example:8545":                     "https://l1.example:8545",
		"builder:8545":                                "builder:",
		"http://user:secret@[l1/KEY":                  "endpoint",
	} {
		require.Equal(t, want, l1.RedactURL(raw), raw)
	}
}

func TestRPCURLError(t *testing.T) {
	srv := httptest.NewTLSServer(nil)
	defer srv.Close()
	r, err := l1.DialRPC(t.Context(), srv.URL+"/v2/secret-key?apikey=secret-query", log.New())
	require.NoError(t, err)
	defer r.Close()
	_, err = r.ChainID(t.Context())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
	require.Contains(t, err.Error(), srv.URL)
	require.ErrorContains(t, err, "x509")
	var cert *tls.CertificateVerificationError
	require.ErrorAs(t, err, &cert)
	var ue *url.Error
	require.ErrorAs(t, err, &ue)

	err = l1.MapRPCError(&url.Error{Op: "Post", URL: "https://user:secret@host/v2/secret?apikey=secret", Err: errors.New("cause")}, "call")
	require.EqualError(t, err, "call: Post https://host: cause")

	host := strings.TrimPrefix(srv.URL, "https://")
	for name, endpoint := range map[string]string{
		"user info":       "https://user:secret@" + host + "/v2/secret-key",
		"unparsable":      srv.URL + "/v2/secret-key\n",
		"websocket dial":  "wss://" + host + "/v2/secret-key",
		"websocket query": "wss://" + host + "/v2?apikey=secret",
	} {
		t.Run(name, func(t *testing.T) {
			r, err := l1.DialRPC(t.Context(), endpoint, log.New())
			if err == nil {
				defer r.Close()
				_, err = r.ChainID(t.Context())
			}
			require.Error(t, err)
			require.NotContains(t, err.Error(), "secret")
			if strings.HasPrefix(endpoint, "wss://") {
				require.ErrorContains(t, err, "WebSocket dial")
			}
		})
	}
}

// posting serves what a poster reads from and sends to L1.
type posting struct {
	baseFee *hexutil.Big
	sent    []hexutil.Bytes
}

var poster = common.HexToAddress("0x5")

func (p *posting) GetTransactionCount(_ context.Context, addr common.Address, block string) (hexutil.Uint64, error) {
	if addr != poster || block != "pending" {
		return 0, errors.New("want the poster's pending nonce")
	}
	return 7, nil
}

func (p *posting) GetBlockByNumber(_ context.Context, block string, full bool) (map[string]any, error) {
	if block != "latest" || full {
		return nil, errors.New("want the latest header")
	}
	h := map[string]any{"number": "0x10", "hash": common.Hash{1}, "parentHash": common.Hash{2}, "timestamp": "0x20"}
	if p.baseFee != nil {
		h["baseFeePerGas"] = p.baseFee
	}
	return h, nil
}

func (p *posting) GetBalance(_ context.Context, addr common.Address, block string) (*hexutil.Big, error) {
	if addr != poster || block != "latest" {
		return nil, errors.New("want the poster's latest balance")
	}
	return (*hexutil.Big)(new(big.Int).Lsh(big.NewInt(1), 70)), nil
}

func (p *posting) GetTransactionReceipt(_ context.Context, hash common.Hash) (map[string]any, error) {
	if hash != (common.Hash{4}) {
		return nil, nil
	}
	return map[string]any{
		"status": "0x1", "blockNumber": "0x11", "transactionIndex": "0x2", "transactionHash": hash,
		"logs": []map[string]any{{
			"address": poster, "topics": []common.Hash{{5}}, "data": "0x0102", "blockNumber": "0x11",
			"transactionHash": hash, "transactionIndex": "0x2", "blockHash": common.Hash{6}, "logIndex": "0x7", "removed": false,
		}},
	}, nil
}

// GetTransactionByHash knows transaction 4 in a block and 9 in the pool.
func (p *posting) GetTransactionByHash(_ context.Context, hash common.Hash) (map[string]any, error) {
	switch hash {
	case common.Hash{4}:
		return map[string]any{"hash": hash, "blockHash": common.Hash{6}, "blockNumber": "0x11", "transactionIndex": "0x2"}, nil
	case common.Hash{9}:
		return map[string]any{"hash": hash, "blockHash": nil, "blockNumber": nil, "transactionIndex": nil}, nil
	}
	return nil, nil
}

func (p *posting) SendRawTransaction(_ context.Context, raw hexutil.Bytes) (common.Hash, error) {
	switch {
	case len(raw) == 0:
		return common.Hash{}, errors.New("nonce too low")
	case raw[0] == 0x01:
		return common.Hash{}, errors.New("already known")
	case raw[0] == 0x03:
		return common.Hash{}, errors.New("existing txn with same hash")
	}
	p.sent = append(p.sent, raw)
	return common.Hash{3}, nil
}

func TestRPCPosting(t *testing.T) {
	p := &posting{baseFee: (*hexutil.Big)(big.NewInt(1_000_000_007))}
	r := serveRPC(t, p)
	ctx := t.Context()
	nonce, err := r.PendingNonce(ctx, poster)
	require.NoError(t, err)
	require.Equal(t, uint64(7), nonce)
	fee, err := r.BaseFee(ctx)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(1_000_000_007), fee)
	balance, err := r.Balance(ctx, poster)
	require.NoError(t, err)
	require.Equal(t, new(uint256.Int).Lsh(uint256.NewInt(1), 70), balance)
	require.NoError(t, r.SendRawTransaction(ctx, []byte{0x02, 0xc0}))
	require.Equal(t, []hexutil.Bytes{{0x02, 0xc0}}, p.sent)
	err = r.SendRawTransaction(ctx, nil)
	require.ErrorContains(t, err, "nonce too low")
	require.ErrorIs(t, err, l1.ErrRefused, "the node answered")
	require.NoError(t, r.SendRawTransaction(ctx, []byte{0x01}), "the pool holds it")
	require.NoError(t, r.SendRawTransaction(ctx, []byte{0x03}), "the pool holds it")

	receipt, err := r.Receipt(ctx, common.Hash{4})
	require.NoError(t, err)
	require.Equal(t, &l1.Receipt{Status: 1, BlockNumber: 0x11, TxIndex: 2, Logs: []*types.Log{{
		Address: poster, Topics: []common.Hash{{5}}, Data: []byte{1, 2}, BlockNumber: 0x11,
		TxHash: common.Hash{4}, TxIndex: 2, BlockHash: common.Hash{6}, Index: 7,
	}}}, receipt)
	receipt, err = r.Receipt(ctx, common.Hash{8})
	require.NoError(t, err)
	require.Nil(t, receipt)
	for hash, want := range map[common.Hash]bool{{4}: true, {9}: true, {8}: false} {
		has, err := r.HasTransaction(ctx, hash)
		require.NoError(t, err)
		require.Equal(t, want, has, "transaction %x", hash)
	}

	p.baseFee = nil
	fee, err = r.BaseFee(ctx)
	require.NoError(t, err)
	require.True(t, fee.IsZero())
}
