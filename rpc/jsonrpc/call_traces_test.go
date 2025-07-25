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
	"context"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/jsonstream"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/execution/stages/mock"
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
	m := mock.Mock(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})
	// Insert blocks 1 by 1 to trigger possible "off by one" errors
	for i := 0; i < chain.Length(); i++ {
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("inserting chain: %v", err)
		}
	}
	s := jsoniter.ConfigDefault.BorrowStream(nil)
	defer jsoniter.ConfigDefault.ReturnStream(s)
	stream := jsonstream.Wrap(s)
	var fromBlock, toBlock uint64
	fromBlock = 1
	toBlock = 10
	toAddress1 := common.Address{1}
	traceReq1 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq1, new(bool), nil, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, stream.Buffer()))
}

func TestCallTraceUnwind(t *testing.T) {
	m := mock.Mock(t)
	var chainA, chainB *core.ChainPack
	var err error
	chainA, err = core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate chainA: %v", err)
	}
	chainB, err = core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 20, func(i int, gen *core.BlockGen) {
		if i < 5 || i >= 10 {
			gen.SetCoinbase(common.Address{1})
		} else {
			gen.SetCoinbase(common.Address{2})
		}
	})
	if err != nil {
		t.Fatalf("generate chainB: %v", err)
	}

	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})

	if err = m.InsertChain(chainA); err != nil {
		t.Fatalf("inserting chainA: %v", err)
	}
	s := jsoniter.ConfigDefault.BorrowStream(nil)
	defer jsoniter.ConfigDefault.ReturnStream(s)
	stream := jsonstream.Wrap(s)
	var fromBlock, toBlock uint64
	fromBlock = 1
	toBlock = 10
	toAddress1 := common.Address{1}
	traceReq1 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
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
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
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
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq3, new(bool), nil, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{12, 13, 14, 15, 16, 17, 18, 19, 20}, blockNumbersFromTraces(t, stream.Buffer()))
}

func TestFilterNoAddresses(t *testing.T) {
	m := mock.Mock(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})
	// Insert blocks 1 by 1 to trigger possible "off by one" errors
	for i := 0; i < chain.Length(); i++ {
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("inserting chain: %v", err)
		}
	}
	s := jsoniter.ConfigDefault.BorrowStream(nil)
	defer jsoniter.ConfigDefault.ReturnStream(s)
	stream := jsonstream.Wrap(s)
	var fromBlock, toBlock uint64
	fromBlock = 1
	toBlock = 10
	traceReq1 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
	}
	if err = api.Filter(context.Background(), traceReq1, new(bool), nil, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, stream.Buffer()))
}

func TestFilterAddressIntersection(t *testing.T) {
	m := mock.Mock(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})

	toAddress1, toAddress2, other := common.Address{1}, common.Address{2}, common.Address{3}

	once := new(sync.Once)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 15, func(i int, block *core.BlockGen) {
		once.Do(func() { block.SetCoinbase(common.Address{4}) })

		var rcv common.Address
		if i < 5 {
			rcv = toAddress1
		} else if i < 10 {
			rcv = toAddress2
		} else {
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

	fromBlock, toBlock := uint64(1), uint64(15)
	t.Run("second", func(t *testing.T) {
		s := jsoniter.ConfigDefault.BorrowStream(nil)
		defer jsoniter.ConfigDefault.ReturnStream(s)
		stream := jsonstream.Wrap(s)

		traceReq1 := TraceFilterRequest{
			FromBlock:   (*hexutil.Uint64)(&fromBlock),
			ToBlock:     (*hexutil.Uint64)(&toBlock),
			FromAddress: []*common.Address{&m.Address, &other},
			ToAddress:   []*common.Address{&m.Address, &toAddress2},
			Mode:        TraceFilterModeIntersection,
		}
		if err = api.Filter(context.Background(), traceReq1, new(bool), nil, stream); err != nil {
			t.Fatalf("trace_filter failed: %v", err)
		}
		assert.Equal(t, []int{6, 7, 8, 9, 10}, blockNumbersFromTraces(t, stream.Buffer()))
	})
	t.Run("first", func(t *testing.T) {
		s := jsoniter.ConfigDefault.BorrowStream(nil)
		defer jsoniter.ConfigDefault.ReturnStream(s)
		stream := jsonstream.Wrap(s)

		traceReq1 := TraceFilterRequest{
			FromBlock:   (*hexutil.Uint64)(&fromBlock),
			ToBlock:     (*hexutil.Uint64)(&toBlock),
			FromAddress: []*common.Address{&m.Address, &other},
			ToAddress:   []*common.Address{&toAddress1, &m.Address},
			Mode:        TraceFilterModeIntersection,
		}
		if err = api.Filter(context.Background(), traceReq1, new(bool), nil, stream); err != nil {
			t.Fatalf("trace_filter failed: %v", err)
		}
		assert.Equal(t, []int{1, 2, 3, 4, 5}, blockNumbersFromTraces(t, stream.Buffer()))
	})
	t.Run("empty", func(t *testing.T) {
		s := jsoniter.ConfigDefault.BorrowStream(nil)
		defer jsoniter.ConfigDefault.ReturnStream(s)
		stream := jsonstream.Wrap(s)

		traceReq1 := TraceFilterRequest{
			FromBlock:   (*hexutil.Uint64)(&fromBlock),
			ToBlock:     (*hexutil.Uint64)(&toBlock),
			ToAddress:   []*common.Address{&other},
			FromAddress: []*common.Address{&toAddress2, &toAddress1, &other},
			Mode:        TraceFilterModeIntersection,
		}
		if err = api.Filter(context.Background(), traceReq1, new(bool), nil, stream); err != nil {
			t.Fatalf("trace_filter failed: %v", err)
		}
		require.Empty(t, blockNumbersFromTraces(t, stream.Buffer()))
	})
}
