package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

/*
Testing tracing RPC API by generating patters of contracts invoking one another based on the input
*/

func TestGeneratedTraces(t *testing.T) {
	m := rpcdaemontest.CreateTestSentryForTraces(t)
	agg := m.HistoryV3Components()
	br := snapshotsync.NewBlockReaderWithSnapshots(m.BlockSnapshots)
	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	baseApi := NewBaseApi(nil, stateCache, br, agg, false, rpccfg.DefaultEvmCallTimeout, m.Engine)
	api := NewPrivateDebugAPI(baseApi, m.DB, 0)
	var buf bytes.Buffer
	stream := jsoniter.NewStream(jsoniter.ConfigDefault, &buf, 4096)
	err := api.TraceBlockByNumber(context.Background(), rpc.BlockNumber(0), &tracers.TraceConfig{}, stream)
	if err != nil {
		t.Errorf("traceBlock %d: %v", 0, err)
	}
	if err = stream.Flush(); err != nil {
		t.Fatalf("error flusing: %v", err)
	}
	var er []ethapi.ExecutionResult
	if err = json.Unmarshal(buf.Bytes(), &er); err != nil {
		t.Fatalf("parsing result: %v", err)
	}
}
