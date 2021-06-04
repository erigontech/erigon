package commands

import (
	"bytes"
	"context"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fastjson"
)

func blockNumbersFromTraces(t *testing.T, b []byte) []int {
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
	var numbers []int
	for _, elem := range elems {
		bn := elem.GetInt("blockNumber")
		numbers = append(numbers, bn)
	}
	return numbers
}

func TestCallTraceOneByOne(t *testing.T) {
	m := stages.Mock(t)
	defer m.DB.Close()
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	api := NewTraceAPI(NewBaseApi(nil), m.DB, &cli.Flags{})
	// Insert blocks 1 by 1, to tirgget possible "off by one" errors
	for i := 0; i < chain.Length; i++ {
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("inserting chain: %v", err)
		}
	}
	var buf bytes.Buffer
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
	var fromBlock, toBlock uint64
	fromBlock = 1
	toBlock = 10
	toAddress1 := common.Address{1}
	traceReq1 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq1, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, buf.Bytes()))
}

func TestCallTraceUnwind(t *testing.T) {
	m := stages.Mock(t)
	defer m.DB.Close()
	var chainA, chainB *core.ChainPack
	var err error
	chainA, err = core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chainA: %v", err)
	}
	chainB, err = core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 20, func(i int, gen *core.BlockGen) {
		if i < 5 || i >= 10 {
			gen.SetCoinbase(common.Address{1})
		} else {
			gen.SetCoinbase(common.Address{2})
		}
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chainB: %v", err)
	}
	api := NewTraceAPI(NewBaseApi(nil), m.DB, &cli.Flags{})
	if err = m.InsertChain(chainA); err != nil {
		t.Fatalf("inserting chainA: %v", err)
	}
	var buf bytes.Buffer
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
	var fromBlock, toBlock uint64
	fromBlock = 1
	toBlock = 10
	toAddress1 := common.Address{1}
	traceReq1 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq1, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}

	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, buf.Bytes()))
	if err = m.InsertChain(chainB.Slice(0, 12)); err != nil {
		t.Fatalf("inserting chainB: %v", err)
	}
	buf.Reset()
	toBlock = 12
	traceReq2 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq2, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 11, 12}, blockNumbersFromTraces(t, buf.Bytes()))
	if err = m.InsertChain(chainB.Slice(12, 20)); err != nil {
		t.Fatalf("inserting chainB: %v", err)
	}
	buf.Reset()
	fromBlock = 12
	toBlock = 20
	traceReq3 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq3, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{12, 13, 14, 15, 16, 17, 18, 19, 20}, blockNumbersFromTraces(t, buf.Bytes()))
}

func TestFilterNoAddresses(t *testing.T) {
	m := stages.Mock(t)
	defer m.DB.Close()
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	api := NewTraceAPI(NewBaseApi(nil), m.DB, &cli.Flags{})
	// Insert blocks 1 by 1, to tirgget possible "off by one" errors
	for i := 0; i < chain.Length; i++ {
		if err = m.InsertChain(chain.Slice(i, i+1)); err != nil {
			t.Fatalf("inserting chain: %v", err)
		}
	}
	var buf bytes.Buffer
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
	var fromBlock, toBlock uint64
	fromBlock = 1
	toBlock = 10
	traceReq1 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
	}
	if err = api.Filter(context.Background(), traceReq1, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, blockNumbersFromTraces(t, buf.Bytes()))
}
