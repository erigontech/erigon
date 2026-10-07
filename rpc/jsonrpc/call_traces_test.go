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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func blockNumbersFromTraces(t *testing.T, b []byte) []int {
	t.Helper()
	var err error
	var p fastjson.Parser
	response := b
	var v *fastjson.Value
	if v, err = p.ParseBytes(response); err != nil {
		t.Fatalf("parsing response: %v", err)
	}
	var elems []*fastjson.Value
	if elems, err = v.Array(); err != nil {
		t.Fatalf("expected array in the response: %v", err)
	}
	numbers := make([]int, 0, len(elems))
	for _, elem := range elems {
		bn := elem.GetInt("blockNumber")
		numbers = append(numbers, bn)
	}
	return numbers
}

func TestCallTraceOneByOne(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(10, func(i int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	api := newTraceApiForTest(m)
	// Insert blocks 1 by 1 to trigger possible "off by one" errors
	for i := 0; i < chain.Length(); i++ {
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("inserting chain: %v", err)
		}
	}
	stream := jsonstream.New(nil)
	fromBlock := rpc.BlockNumber(1)
	toBlock := rpc.BlockNumber(10)
	toAddress1 := common.Address{1}
	traceReq1 := TraceFilterRequest{
		FromBlock: &fromBlock,
		ToBlock:   &toBlock,
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq1, new(bool), nil, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, stream.Buffer()))
}

func TestCallTraceUnwind(t *testing.T) {
	m := execmoduletester.New(t)
	var chainA, chainB *blockgen.ChainPack
	var err error
	chainA, err = m.GenerateChain(10, func(i int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate chainA: %v", err)
	}
	chainB, err = m.GenerateChain(20, func(i int, gen *blockgen.BlockGen) {
		if i < 5 || i >= 10 {
			gen.SetCoinbase(common.Address{1})
		} else {
			gen.SetCoinbase(common.Address{2})
		}
	})
	if err != nil {
		t.Fatalf("generate chainB: %v", err)
	}

	api := newTraceApiForTest(m)

	if err = m.InsertChain(chainA); err != nil {
		t.Fatalf("inserting chainA: %v", err)
	}
	stream := jsonstream.New(nil)
	fromBlock := rpc.BlockNumber(1)
	toBlock := rpc.BlockNumber(10)
	toAddress1 := common.Address{1}
	traceReq1 := TraceFilterRequest{
		FromBlock: &fromBlock,
		ToBlock:   &toBlock,
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq1, new(bool), nil, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, stream.Buffer()))

	if err = m.InsertChain(chainB.Slice(0, 12)); err != nil {
		t.Fatalf("inserting chainB: %v", err)
	}
	stream.Reset(nil)
	toBlock = 12
	traceReq2 := TraceFilterRequest{
		FromBlock: &fromBlock,
		ToBlock:   &toBlock,
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq2, new(bool), nil, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 11, 12}, blockNumbersFromTraces(t, stream.Buffer()))

	if err = m.InsertChain(chainB.Slice(12, 20)); err != nil {
		t.Fatalf("inserting chainB: %v", err)
	}
	stream.Reset(nil)
	fromBlock = 12
	toBlock = 20
	traceReq3 := TraceFilterRequest{
		FromBlock: &fromBlock,
		ToBlock:   &toBlock,
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq3, new(bool), nil, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{12, 13, 14, 15, 16, 17, 18, 19, 20}, blockNumbersFromTraces(t, stream.Buffer()))
}

func TestFilterNoAddresses(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(10, func(i int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	api := newTraceApiForTest(m)
	// Insert blocks 1 by 1 to trigger possible "off by one" errors
	for i := 0; i < chain.Length(); i++ {
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("inserting chain: %v", err)
		}
	}
	stream := jsonstream.New(nil)
	fromBlock := rpc.BlockNumber(1)
	toBlock := rpc.BlockNumber(10)
	traceReq1 := TraceFilterRequest{
		FromBlock: &fromBlock,
		ToBlock:   &toBlock,
	}
	for _, mode := range []TraceFilterMode{"", TraceFilterModeIntersection, TraceFilterModeUnion} {
		t.Run(string(mode), func(t *testing.T) {
			traceReq1.Mode = mode
			stream.Reset(nil)
			require.NoError(t, api.Filter(t.Context(), traceReq1, nil, nil, stream))
			require.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, stream.Buffer()))
		})
	}
}

// TestFilterGenesisHasNoReward checks that trace_filter reports no block reward for the genesis
// block, which is not mined, in agreement with trace_block.
func TestFilterGenesisHasNoReward(t *testing.T) {
	m := execmoduletester.New(t)
	miner := common.Address{1}
	chain, err := m.GenerateChain(3, func(i int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(miner)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chain))
	api := newTraceApiForTest(m)

	genesisTraces, err := api.Block(context.Background(), 0, new(bool), nil)
	require.NoError(t, err)
	require.Empty(t, genesisTraces)

	genesisAuthor := m.Genesis.Coinbase()
	filter := func(t *testing.T, from, to rpc.BlockNumber, toAddress []*common.Address, after, count *uint64) []int {
		t.Helper()
		stream := jsonstream.New(nil)
		req := TraceFilterRequest{
			FromBlock: &from,
			ToBlock:   &to,
			ToAddress: toAddress,
			After:     after,
			Count:     count,
		}
		require.NoError(t, api.Filter(context.Background(), req, new(bool), nil, stream))
		return blockNumbersFromTraces(t, stream.Buffer())
	}
	one := uint64(1)

	t.Run("genesis", func(t *testing.T) {
		require.Empty(t, filter(t, 0, 0, nil, nil, nil))
	})
	t.Run("genesis author", func(t *testing.T) {
		require.Empty(t, filter(t, 0, 0, []*common.Address{&genesisAuthor}, nil, nil))
	})
	t.Run("from genesis", func(t *testing.T) {
		require.Equal(t, []int{1, 2, 3}, filter(t, 0, 3, nil, nil, nil))
	})
	t.Run("from genesis by author", func(t *testing.T) {
		require.Equal(t, []int{1, 2, 3}, filter(t, 0, 3, []*common.Address{&miner, &genesisAuthor}, nil, nil))
	})
	t.Run("from genesis paginated", func(t *testing.T) {
		zero := uint64(0)
		require.Equal(t, []int{1}, filter(t, 0, 3, nil, &zero, &one))
		require.Equal(t, []int{2}, filter(t, 0, 3, nil, &one, &one))
	})
}

func TestFilterAddressIntersection(t *testing.T) {
	m := execmoduletester.New(t)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	toAddress1, toAddress2, other := common.Address{1}, common.Address{2}, common.Address{3}

	once := new(sync.Once)
	chain, err := m.GenerateChain(15, func(i int, block *blockgen.BlockGen) {
		once.Do(func() { block.SetCoinbase(common.Address{4}) })

		var rcv common.Address
		switch {
		case i < 5:
			rcv = toAddress1
		case i < 10:
			rcv = toAddress2
		default:
			rcv = other
		}

		signer := types.LatestSigner(m.ChainConfig)
		txn, err := types.SignTx(types.NewTransaction(block.TxNonce(m.Address), rcv, new(uint256.Int), 21000, new(uint256.Int), nil), *signer, m.Key)
		if err != nil {
			t.Fatal(err)
		}
		block.AddTx(txn)
	})
	require.NoError(t, err, "generate chain")

	err = m.InsertChain(chain)
	require.NoError(t, err, "inserting chain")

	fromBlock := rpc.BlockNumber(1)
	toBlock := rpc.BlockNumber(15)
	first := []int{1, 2, 3, 4, 5}
	second := []int{6, 7, 8, 9, 10}
	all := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	for _, tc := range []struct {
		name     string
		from, to []*common.Address
		mode     TraceFilterMode
		want     []int
	}{
		{"second", []*common.Address{&m.Address, &other}, []*common.Address{&m.Address, &toAddress2}, TraceFilterModeIntersection, second},
		{"first", []*common.Address{&m.Address, &other}, []*common.Address{&toAddress1, &m.Address}, TraceFilterModeIntersection, first},
		{"empty", []*common.Address{&toAddress2, &toAddress1, &other}, []*common.Address{&other}, TraceFilterModeIntersection, []int{}},
		{"default", []*common.Address{&m.Address, &other}, []*common.Address{&toAddress2}, "", second},
		{"self activity default", []*common.Address{&m.Address}, []*common.Address{&m.Address}, "", []int{}},
		{"self activity union", []*common.Address{&m.Address}, []*common.Address{&m.Address}, TraceFilterModeUnion, all},
		{"default from only", []*common.Address{&m.Address}, nil, "", all},
		{"default to only", nil, []*common.Address{&toAddress2}, "", second},
		{"union", []*common.Address{&m.Address}, []*common.Address{&toAddress2}, TraceFilterModeUnion, all},
		{"from only", []*common.Address{&m.Address}, nil, TraceFilterModeIntersection, all},
		{"to only", nil, []*common.Address{&toAddress2}, TraceFilterModeIntersection, second},
		{"from with empty to", []*common.Address{&m.Address}, []*common.Address{}, TraceFilterModeIntersection, all},
		{"to with empty from", []*common.Address{}, []*common.Address{&toAddress2}, TraceFilterModeIntersection, second},
		{"union from only", []*common.Address{&m.Address}, nil, TraceFilterModeUnion, all},
		{"union to only", nil, []*common.Address{&toAddress2}, TraceFilterModeUnion, second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := TraceFilterRequest{
				FromBlock:   &fromBlock,
				ToBlock:     &toBlock,
				FromAddress: tc.from, ToAddress: tc.to, Mode: tc.mode,
			}
			var result json.RawMessage
			require.NoError(t, client.CallContext(t.Context(), &result, "trace_filter", req))
			require.Equal(t, tc.want, blockNumbersFromTraces(t, result))
			after, count := uint64(2), uint64(2)
			req.After, req.Count = &after, &count
			require.NoError(t, client.CallContext(t.Context(), &result, "trace_filter", req))
			require.Equal(t, tc.want[min(2, len(tc.want)):min(4, len(tc.want))], blockNumbersFromTraces(t, result))
		})
	}
}

// TestFilterRangeDefaults checks that omitted bounds default to the latest
// executed block, as in eth_getLogs, and that a reversed range, including one
// whose start is that implicit latest block, is invalid params.
func TestFilterRangeDefaults(t *testing.T) {
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(5, func(i int, gen *blockgen.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chain))

	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	for _, tc := range []struct {
		name string
		req  map[string]any
		want []int // nil means -32602
	}{
		{"no bounds", map[string]any{}, []int{5}},
		{"null bounds", map[string]any{"fromBlock": nil, "toBlock": nil}, []int{5}},
		{"from only", map[string]any{"fromBlock": "0x3"}, []int{3, 4, 5}},
		{"from earliest", map[string]any{"fromBlock": "earliest"}, []int{1, 2, 3, 4, 5}},
		{"to latest", map[string]any{"toBlock": "latest"}, []int{5}},
		{"to head", map[string]any{"toBlock": "0x5"}, []int{5}},
		{"to before head", map[string]any{"toBlock": "0x2"}, nil},
		{"explicit", map[string]any{"fromBlock": "0x1", "toBlock": "0x2"}, []int{1, 2}},
		{"explicit reversed", map[string]any{"fromBlock": "0x3", "toBlock": "0x2"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, "trace_filter", tc.req)
			if tc.want == nil {
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
				require.ErrorContains(t, err, errInvalidBlockRange)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, blockNumbersFromTraces(t, result))
		})
	}
}

// A failed CREATE deploys no contract, so trace_filter does not match it by the address it would have
// occupied, at the top level or nested.
func TestFilterFailedCreateHasNoRecipient(t *testing.T) {
	m := execmoduletester.New(t)
	// PUSH4 0xdeadbeef, PUSH1 0, MSTORE, PUSH1 4, PUSH1 28, REVERT
	revert := common.FromHex("0x63deadbeef6000526004601cfd")
	// Runs revert through CREATE and succeeds: PUSH13 revert, PUSH1 0, MSTORE, PUSH1 13, PUSH1 19, PUSH1 0, CREATE, POP, STOP
	factory := append(append([]byte{0x6c}, revert...), 0x60, 0x00, 0x52, 0x60, 13, 0x60, 19, 0x60, 0x00, 0xf0, 0x50, 0x00)
	chain, err := m.GenerateChain(1, func(i int, block *blockgen.BlockGen) {
		signer := types.LatestSigner(m.ChainConfig)
		for _, init := range [][]byte{revert, factory} {
			txn, err := types.SignTx(types.NewContractCreation(block.TxNonce(m.Address), new(uint256.Int), 100_000, new(uint256.Int), init), *signer, m.Key)
			require.NoError(t, err)
			block.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chain))
	api := newTraceApiForTest(m)

	failed := types.CreateAddress(m.Address, 0)
	deployed := types.CreateAddress(m.Address, 1)
	nested := types.CreateAddress(deployed, 1)
	block := rpc.BlockNumber(1)
	filter := func(from, to []*common.Address) []map[string]any {
		t.Helper()
		stream := jsonstream.New(nil)
		req := TraceFilterRequest{
			FromBlock:   &block,
			ToBlock:     &block,
			FromAddress: from, ToAddress: to,
		}
		require.NoError(t, api.Filter(context.Background(), req, new(bool), nil, stream))
		var traces []map[string]any
		require.NoError(t, json.Unmarshal(stream.Buffer(), &traces))
		return traces
	}

	assert.Empty(t, filter(nil, []*common.Address{&failed}), "top-level failed create")
	assert.Empty(t, filter(nil, []*common.Address{&nested}), "nested failed create")
	assert.Empty(t, filter([]*common.Address{&m.Address}, []*common.Address{&failed}), "intersection with the sender")

	created := filter(nil, []*common.Address{&deployed})
	require.Len(t, created, 1, "a successful create matches its address")
	require.Nil(t, created[0]["error"])
	byFactory := filter([]*common.Address{&deployed}, nil)
	require.Len(t, byFactory, 1, "a nested failed create matches its sender")
	require.Equal(t, "Reverted", byFactory[0]["error"])
}

func TestFilterModeValidation(t *testing.T) {
	m := execmoduletester.New(t)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })
	for _, mode := range []any{"garbage", "INTERSECTION", "", 1} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, "trace_filter", map[string]any{"mode": mode, "count": 0})
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr)
			require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
		})
	}
}

// TestFilterBoundPastHead checks that a bound past the executed head returns
// -32602, as eth_getLogs does, instead of an empty result, and that the head
// itself is still a valid bound.
func TestFilterBoundPastHead(t *testing.T) {
	m := execmoduletester.New(t)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	for name, req := range map[string]map[string]any{
		"fromBlock next":          {"fromBlock": "0x1"},
		"toBlock next":            {"fromBlock": "0x0", "toBlock": "0x1"},
		"toBlock far":             {"fromBlock": "0x0", "toBlock": "0xfffffffff"},
		"fromBlock far, reversed": {"fromBlock": "0xfffffffff", "toBlock": "0x0"},
	} {
		t.Run(name, func(t *testing.T) {
			var result json.RawMessage
			err := client.CallContext(t.Context(), &result, "trace_filter", req)
			var rpcErr rpc.Error
			require.ErrorAs(t, err, &rpcErr)
			require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
			require.EqualError(t, err, ErrBlockRangeIntoFuture)
		})
	}

	t.Run("head", func(t *testing.T) {
		var result json.RawMessage
		require.NoError(t, client.CallContext(t.Context(), &result, "trace_filter", map[string]any{"fromBlock": "0x0", "toBlock": "0x0"}))
		require.JSONEq(t, "[]", string(result))
	})
}

// An explicit null for an optional trace_filter member is the same as omitting it.
func TestFilterNullMembers(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	relay, sink, other := common.Address{1}, common.Address{2}, common.Address{3}
	// The relay calls the sink: GAS is the gas, and value, arguments and return data are zero.
	relayCode := append([]byte{0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x73}, sink[:]...)
	relayCode = append(relayCode, 0x5a, 0xf1, 0x00)
	m := execmoduletester.New(t, execmoduletester.WithKey(key), execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(common.Ether)},
			relay:  {Code: relayCode},
		},
	}))
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	// Every block holds a call to the relay, which calls the sink, and a transfer to other.
	signer := types.LatestSigner(m.ChainConfig)
	blocks, err := m.GenerateChain(3, func(i int, block *blockgen.BlockGen) {
		for _, rcv := range []common.Address{relay, other} {
			txn, err := types.SignTx(types.NewTransaction(block.TxNonce(sender), rcv, new(uint256.Int), 100_000, new(uint256.Int), nil), *signer, key)
			require.NoError(t, err)
			block.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(blocks))

	traces := func(t *testing.T, req map[string]any) string {
		t.Helper()
		var result json.RawMessage
		require.NoError(t, client.CallContext(t.Context(), &result, "trace_filter", req))
		return string(result)
	}
	filter := func(t *testing.T, req map[string]any) []int {
		t.Helper()
		return blockNumbersFromTraces(t, []byte(traces(t, req)))
	}

	t.Run("null mode is intersection", func(t *testing.T) {
		for _, tc := range []struct {
			name                string
			from, to            []common.Address
			intersection, union []int
		}{
			// The address indexes share only the relay call, so it is the only transaction traced.
			// Of its two traces, only relay -> sink is from a listed sender to a listed recipient;
			// sender -> relay matches the from list alone.
			{"per-trace match", []common.Address{sender, relay}, []common.Address{sink}, []int{1, 2, 3}, []int{1, 1, 1, 2, 2, 2, 3, 3, 3}},
			// The address indexes share no transaction, so the intersection traces nothing.
			{"address index", []common.Address{sink}, []common.Address{other}, []int{}, []int{1, 2, 3}},
			// Block rewards skip the per-trace match, so only the index intersection keeps out the
			// rewards of the miner, the zero address.
			{"reward", []common.Address{sender}, []common.Address{{}}, []int{}, []int{1, 1, 1, 2, 2, 2, 3, 3, 3}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				req := func(mode ...any) map[string]any {
					r := map[string]any{"fromBlock": "0x1", "toBlock": "0x3", "fromAddress": tc.from, "toAddress": tc.to}
					if len(mode) > 0 {
						r["mode"] = mode[0]
					}
					return r
				}
				require.Equal(t, tc.union, filter(t, req(TraceFilterModeUnion)))
				require.Equal(t, tc.intersection, filter(t, req(TraceFilterModeIntersection)))
				require.Equal(t, tc.intersection, filter(t, req()))
				require.Equal(t, tc.intersection, filter(t, req(nil)))
			})
		}
	})

	// toBlock is the head, so the range stays valid when an omitted fromBlock
	// defaults to the latest block.
	full := map[string]any{
		"fromBlock": "0x1", "toBlock": "0x3",
		"fromAddress": []common.Address{sender, relay}, "toAddress": []common.Address{sink},
		"mode": TraceFilterModeUnion, "after": 1, "count": 2,
	}
	for member := range full {
		t.Run(member, func(t *testing.T) {
			omitted := maps.Clone(full)
			delete(omitted, member)
			null := maps.Clone(omitted)
			null[member] = nil
			require.JSONEq(t, traces(t, omitted), traces(t, null))
		})
	}
	t.Run("all", func(t *testing.T) {
		null := map[string]any{}
		for member := range full {
			null[member] = nil
		}
		require.JSONEq(t, traces(t, map[string]any{}), traces(t, null))
	})
}

// TestFilterBlockHash checks that blockHash selects exactly the canonical block
// it names, as a single-block range does, and that a hash is not a range bound.
func TestFilterBlockHash(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	relay, sink, other := common.Address{1}, common.Address{2}, common.Address{3}
	// The relay calls the sink: GAS is the gas, and value, arguments and return data are zero.
	relayCode := append([]byte{0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x60, 0x00, 0x73}, sink[:]...)
	relayCode = append(relayCode, 0x5a, 0xf1, 0x00)
	m := execmoduletester.New(t, execmoduletester.WithKey(key), execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(common.Ether)},
			relay:  {Code: relayCode},
		},
	}))
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	// Every block of branch a holds a call to the relay, which calls the sink, and a
	// transfer to other. Branch b shares block 1, then replaces blocks 2 and 3 with
	// empty blocks rewarding other, and grows past a.
	signer := types.LatestSigner(m.ChainConfig)
	addCalls := func(block *blockgen.BlockGen) {
		for _, rcv := range []common.Address{relay, other} {
			txn, err := types.SignTx(types.NewTransaction(block.TxNonce(sender), rcv, new(uint256.Int), 100_000, new(uint256.Int), nil), *signer, key)
			require.NoError(t, err)
			block.AddTx(txn)
		}
	}
	a, err := m.GenerateChain(3, func(i int, block *blockgen.BlockGen) { addCalls(block) })
	require.NoError(t, err)
	b, err := m.GenerateChain(4, func(i int, block *blockgen.BlockGen) {
		if i == 0 {
			addCalls(block)
			return
		}
		block.SetCoinbase(other)
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(a))

	call := func(req map[string]any) (json.RawMessage, error) {
		var result json.RawMessage
		err := client.CallContext(t.Context(), &result, "trace_filter", req)
		return result, err
	}
	traces := func(t *testing.T, req map[string]any) string {
		t.Helper()
		result, err := call(req)
		require.NoError(t, err)
		return string(result)
	}
	with := func(req, members map[string]any) map[string]any {
		req = maps.Clone(req)
		maps.Copy(req, members)
		return req
	}
	hash := a.Blocks[1].Hash()
	byHash := map[string]any{"blockHash": hash}
	block2 := map[string]any{"fromBlock": "0x2", "toBlock": "0x2"}

	t.Run("selects the block", func(t *testing.T) {
		var records []struct {
			BlockHash   common.Hash `json:"blockHash"`
			BlockNumber uint64      `json:"blockNumber"`
		}
		require.NoError(t, json.Unmarshal([]byte(traces(t, byHash)), &records))
		// Two relay traces, the transfer and the block reward. Block 2 is not the
		// head, so an ignored blockHash would answer for block 3.
		require.Len(t, records, 4)
		for _, record := range records {
			require.Equal(t, hash, record.BlockHash)
			require.Equal(t, uint64(2), record.BlockNumber)
		}
	})

	t.Run("as a single-block range", func(t *testing.T) {
		for name, members := range map[string]map[string]any{
			"all":          {},
			"fromAddress":  {"fromAddress": []common.Address{relay}},
			"toAddress":    {"toAddress": []common.Address{sink}},
			"reward":       {"toAddress": []common.Address{{}}},
			"union":        {"fromAddress": []common.Address{relay}, "toAddress": []common.Address{other}, "mode": TraceFilterModeUnion},
			"page":         {"after": 1, "count": 2},
			"past the end": {"after": 4},
			"count zero":   {"count": 0},
		} {
			t.Run(name, func(t *testing.T) {
				require.JSONEq(t, traces(t, with(block2, members)), traces(t, with(byHash, members)))
			})
		}
	})

	t.Run("null members are omitted", func(t *testing.T) {
		require.JSONEq(t, traces(t, block2), traces(t, with(byHash, map[string]any{"fromBlock": nil, "toBlock": nil})))
		require.JSONEq(t, traces(t, block2), traces(t, with(block2, map[string]any{"blockHash": nil})))
	})

	t.Run("genesis", func(t *testing.T) {
		require.JSONEq(t, "[]", traces(t, map[string]any{"blockHash": a.Blocks[0].ParentHash()}))
	})

	t.Run("invalid params", func(t *testing.T) {
		for name, req := range map[string]map[string]any{
			"blockHash with fromBlock":       {"blockHash": hash, "fromBlock": "0x2"},
			"blockHash with toBlock":         {"blockHash": hash, "toBlock": "latest"},
			"blockHash with range and count": {"blockHash": hash, "fromBlock": "0x2", "toBlock": "0x2", "count": 0},
			"short blockHash":                {"blockHash": "0x02"},
			"object blockHash":               {"blockHash": map[string]any{"blockHash": hash}},
			"hash fromBlock":                 {"fromBlock": hash, "toBlock": "0x2"},
			"hash toBlock":                   {"fromBlock": "0x1", "toBlock": hash},
			"object fromBlock":               {"fromBlock": map[string]any{"blockHash": hash}, "toBlock": "0x2"},
			"object toBlock":                 {"fromBlock": "0x1", "toBlock": map[string]any{"blockHash": hash, "requireCanonical": true}},
			"number object bound":            {"fromBlock": map[string]any{"blockNumber": "0x2"}, "toBlock": "0x2"},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := call(req)
				var rpcErr rpc.Error
				require.ErrorAs(t, err, &rpcErr)
				require.Equal(t, rpc.ErrCodeInvalidParams, rpcErr.ErrorCode())
			})
		}
	})

	t.Run("unknown", func(t *testing.T) {
		for _, req := range []map[string]any{{"blockHash": common.Hash{0xff}}, {"blockHash": common.Hash{0xff}, "count": 0}} {
			_, err := call(req)
			requireResourceNotFound(t, err, "block not found")
		}
	})

	t.Run("noncanonical", func(t *testing.T) {
		require.NoError(t, m.InsertChain(b))
		replacement := b.Blocks[1].Hash()
		require.NotEqual(t, hash, replacement)
		for _, req := range []map[string]any{byHash, with(byHash, map[string]any{"count": 0})} {
			_, err := call(req)
			requireResourceNotFound(t, err, "block not found")
		}
		require.Equal(t, []int{2}, blockNumbersFromTraces(t, []byte(traces(t, map[string]any{"blockHash": replacement}))))
		require.JSONEq(t, traces(t, block2), traces(t, map[string]any{"blockHash": replacement}))
	})
}

// TestFilterBlockHashAheadOfExecution pins that a blockHash naming
// a canonical block that is not executed yet is not found (-32001), even with
// count 0, rather than an empty result for a block the node cannot trace.
func TestFilterBlockHashAheadOfExecution(t *testing.T) {
	t.Parallel()
	m, aheadHash := newBlockAheadOfExecutionTester(t)
	server := rpc.NewServer(50, false, false, true, log.New(), 100)
	require.NoError(t, server.RegisterName("trace", newTraceApiForTest(m)))
	client := rpc.DialInProc(server, log.New())
	t.Cleanup(func() { client.Close(); server.Stop() })

	for _, req := range []map[string]any{{"blockHash": aheadHash}, {"blockHash": aheadHash, "count": 0}} {
		var result json.RawMessage
		err := client.CallContext(t.Context(), &result, "trace_filter", req)
		requireResourceNotFound(t, err, "is not executed")
	}
}

func TestFilterBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	const tipCap = 2
	c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
	contractAddr, _, blockNumber, overrideBaseFee := c.setupBaseFeeOverrideCall(t, opGasprice, tipCap)
	api := c.traceAPI()

	n := rpc.BlockNumber(blockNumber)
	traceReq := TraceFilterRequest{
		FromBlock: &n,
		ToBlock:   &n,
		ToAddress: []*common.Address{&contractAddr},
	}

	stream := jsonstream.New(nil)
	err := api.Filter(context.Background(), traceReq, new(bool), traceConfigWithBaseFeeOverride(overrideBaseFee), stream)
	require.NoError(t, err)

	expectedGasPrice := new(uint256.Int).AddUint64(overrideBaseFee, tipCap)
	expectedOutput := hexutil.Bytes(expectedGasPrice.PaddedBytes(32)).String()
	require.Contains(t, string(stream.Buffer()), expectedOutput)
}

// TestFilterBlockOverridesOtherFieldsAffectOpcodes checks that filterV3's
// per-transaction BlockContext picks up BlockOverrides fields other than
// BaseFeePerGas too.
func TestFilterBlockOverridesOtherFieldsAffectOpcodes(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	for _, tc := range blockOverrideOpcodeCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
			contractAddr := c.deployOpcodeContract(t, tc.opcode)
			_, blockNumber, _ := c.callWithDynamicFee(t, contractAddr, 2, 1)
			api := c.traceAPI()

			n := rpc.BlockNumber(blockNumber)
			traceReq := TraceFilterRequest{
				FromBlock: &n,
				ToBlock:   &n,
				ToAddress: []*common.Address{&contractAddr},
			}

			stream := jsonstream.New(nil)
			err := api.Filter(context.Background(), traceReq, new(bool), &config.TraceConfig{
				BlockOverrides: tc.override,
			}, stream)
			require.NoError(t, err)
			require.Contains(t, string(stream.Buffer()), hexutil.Bytes(tc.expected).String())
		})
	}
}

// TestFilterRejectedBlockOverrideReturnsError checks that trace_filter
// reports a rejected BlockOverrides field (here BeaconRoot, which Override
// always rejects) as a normal RPC error instead of leaving lastRules unset
// for the block's remaining transactions.
func TestFilterRejectedBlockOverrideReturnsError(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
	api := c.traceAPI()

	n := rpc.BlockNumber(0)
	traceReq := TraceFilterRequest{
		FromBlock: &n,
		ToBlock:   &n,
	}

	beaconRoot := common.HexToHash("0x01")
	stream := jsonstream.New(nil)
	err := api.Filter(context.Background(), traceReq, new(bool), &config.TraceConfig{
		BlockOverrides: &ethapi.BlockOverrides{BeaconRoot: &beaconRoot},
	}, stream)
	require.Error(t, err)
}

// TestFilterSignerReflectsBlockOverridesNumber is filterV3's analogue of
// TestReplayTransactionSignerReflectsBlockOverridesNumber: filterV3 derives
// fork rules (lastRules) from the overridden BlockContext but must also
// recompute lastSigner from it, not from the block's real number. A
// transaction that cannot be traced must fail the whole request instead of
// mixing an error object into the result array.
func TestFilterSignerReflectsBlockOverridesNumber(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, delayedSpuriousDragonConfig())
	c.mineProtectedTxAtBlock3(t)
	api := c.traceAPI()

	n := rpc.BlockNumber(3)
	traceReq := TraceFilterRequest{
		FromBlock: &n,
		ToBlock:   &n,
		ToAddress: []*common.Address{&c.bankAddress},
	}

	stream := jsonstream.New(nil)
	err := api.Filter(context.Background(), traceReq, new(bool), &config.TraceConfig{
		BlockOverrides: &ethapi.BlockOverrides{Number: (*hexutil.U256)(uint256.NewInt(1))},
	}, stream)
	require.ErrorContains(t, err, "protected txn is not supported by signer")
	require.Empty(t, string(stream.Buffer()))
}

// TestFilterErrorAfterExportedTracesKeepsValidJSON covers the other half of
// filterV3's error contract: when a transaction fails after earlier traces were
// already streamed, the request still fails and the result array holds only
// TraceEntry items. The envelope is assembled the way runMethod does it, since
// sealing the half-written array is the handler's job, not filterV3's.
func TestFilterErrorAfterExportedTracesKeepsValidJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, delayedSpuriousDragonConfig())
	c.mineProtectedTxAtBlock3(t)
	api := c.traceAPI()

	from, to := rpc.BlockNumber(1), rpc.BlockNumber(3)
	traceReq := TraceFilterRequest{
		FromBlock: &from,
		ToBlock:   &to,
	}

	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	stream.WriteObjectStart()
	stream.Field("jsonrpc")
	stream.WriteString("2.0")
	stream.Field("id")
	stream.Int(1)
	err := rpc.WriteFieldOrError(stream, "result", func(*jsonstream.Stream) error {
		return api.Filter(context.Background(), traceReq, new(bool), &config.TraceConfig{
			BlockOverrides: &ethapi.BlockOverrides{Number: (*hexutil.U256)(uint256.NewInt(1))},
		}, stream)
	})
	require.ErrorContains(t, err, "protected txn is not supported by signer")
	stream.WriteObjectEnd()
	require.NoError(t, stream.Flush())

	var envelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(buf.Bytes(), &envelope), "envelope is not valid JSON: %s", buf.String())
	require.Contains(t, envelope, "error")

	var traces []json.RawMessage
	require.NoError(t, json.Unmarshal(envelope["result"], &traces), "result array was left unsealed: %s", envelope["result"])
	require.NotEmpty(t, traces)
	for i, trace := range traces {
		var entry map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(trace, &entry), "item %d is not a JSON object", i)
		require.Contains(t, entry, "type", "item %d is not a TraceEntry", i)
		if reason, ok := entry["error"]; ok {
			var failure string
			require.NoError(t, json.Unmarshal(reason, &failure),
				"item %d: error must be a TraceEntry failure reason, not an RPC error object", i)
		}
	}
}

// TestFilterCountSatisfiedIgnoresLaterErrors checks that once count traces
// were exported the scan stops: a failure in a transaction the client never
// asked to see must not turn a complete page into an RPC error.
func TestFilterCountSatisfiedIgnoresLaterErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, delayedSpuriousDragonConfig())
	c.mineProtectedTxAtBlock3(t)
	api := c.traceAPI()

	from, to := rpc.BlockNumber(1), rpc.BlockNumber(3)
	count := uint64(1)
	traceReq := TraceFilterRequest{
		FromBlock: &from,
		ToBlock:   &to,
		Count:     &count,
	}

	stream := jsonstream.New(nil)
	err := api.Filter(context.Background(), traceReq, new(bool), &config.TraceConfig{
		BlockOverrides: &ethapi.BlockOverrides{Number: (*hexutil.U256)(uint256.NewInt(1))},
	}, stream)
	require.NoError(t, err)

	var traces []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(stream.Buffer(), &traces))
	require.Len(t, traces, 1)
	require.JSONEq(t, `"reward"`, string(traces[0]["type"]))
}

// TestFilterAfterSkipsTracesBeforeExporting pins the after/count pagination:
// after skips the first matches without exporting them, count then bounds the
// exported page.
func TestFilterAfterSkipsTracesBeforeExporting(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, delayedSpuriousDragonConfig())
	c.mineProtectedTxAtBlock3(t)
	api := c.traceAPI()

	from, to := rpc.BlockNumber(1), rpc.BlockNumber(2)
	after, count := uint64(1), uint64(1)
	traceReq := TraceFilterRequest{
		FromBlock: &from,
		ToBlock:   &to,
		After:     &after,
		Count:     &count,
	}

	stream := jsonstream.New(nil)
	err := api.Filter(context.Background(), traceReq, new(bool), nil, stream)
	require.NoError(t, err)

	var traces []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(stream.Buffer(), &traces))
	require.Len(t, traces, 1)
	require.JSONEq(t, `"reward"`, string(traces[0]["type"]))
	require.JSONEq(t, `2`, string(traces[0]["blockNumber"]))
}

// TestFilterZeroCountReturnsEmptyArray checks that count=0 yields [] without
// tracing anything, so it cannot fail on transactions it will never export.
func TestFilterZeroCountReturnsEmptyArray(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, delayedSpuriousDragonConfig())
	c.mineProtectedTxAtBlock3(t)
	api := c.traceAPI()

	from, to := rpc.BlockNumber(1), rpc.BlockNumber(3)
	count := uint64(0)
	traceReq := TraceFilterRequest{
		FromBlock: &from,
		ToBlock:   &to,
		Count:     &count,
	}

	stream := jsonstream.New(nil)
	err := api.Filter(context.Background(), traceReq, new(bool), &config.TraceConfig{
		BlockOverrides: &ethapi.BlockOverrides{Number: (*hexutil.U256)(uint256.NewInt(1))},
	}, stream)
	require.NoError(t, err)
	require.Equal(t, "[]", string(stream.Buffer()))
}
