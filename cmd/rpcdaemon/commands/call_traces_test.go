package commands

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/turbo/stages"
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

func TestCallTraceUnwind(t *testing.T) {
	m := stages.Mock(t)
	defer m.DB.Close()
	var chainA, chainB *core.ChainPack
	var err error
	chainA, err = core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 100, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate chainA: %v", err)
	}
	chainB, err = core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 200, func(i int, gen *core.BlockGen) {
		if i < 50 || i >= 100 {
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
	toBlock = 100
	toAddress1 := common.Address{1}
	traceReq1 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq1, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}

	fmt.Printf("result1: %d\n", blockNumbersFromTraces(t, buf.Bytes()))
	if err = m.InsertChain(chainB.Slice(0, 120)); err != nil {
		t.Fatalf("inserting chainB: %v", err)
	}
	buf.Reset()
	toBlock = 120
	traceReq2 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq2, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	fmt.Printf("result2: %d\n", blockNumbersFromTraces(t, buf.Bytes()))
	if err = m.InsertChain(chainB.Slice(120, chainB.Length)); err != nil {
		t.Fatalf("inserting chainB: %v", err)
	}
	buf.Reset()
	fromBlock = 120
	toBlock = 200
	traceReq3 := TraceFilterRequest{
		FromBlock: (*hexutil.Uint64)(&fromBlock),
		ToBlock:   (*hexutil.Uint64)(&toBlock),
		ToAddress: []*common.Address{&toAddress1},
	}
	if err = api.Filter(context.Background(), traceReq3, stream); err != nil {
		t.Fatalf("trace_filter failed: %v", err)
	}
	fmt.Printf("result3: %d\n", blockNumbersFromTraces(t, buf.Bytes()))
}
