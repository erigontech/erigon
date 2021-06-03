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
)

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
	chainB, err = core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 12, func(i int, gen *core.BlockGen) {
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
	fmt.Printf("Result: %s\n", buf.Bytes())
	if err = m.InsertChain(chainB); err != nil {
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
	fmt.Printf("Result2: %s\n", buf.Bytes())
}
