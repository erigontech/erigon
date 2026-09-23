package engineapi_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

func dataDir(t *testing.T) string {
	if d := os.Getenv("EMPTYCOST_DATADIR"); d != "" {
		return d
	}
	return t.TempDir()
}

func TestZZZEmptyPayloadCost(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlCrit)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:        logger,
		DataDir:       dataDir(t),
		Genesis:       genesis,
		CoinbaseKey:   coinbaseKey,
		DisableSentry: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	var rpcCalls atomic.Int64
	rpcStop := make(chan struct{})
	var rpcWg sync.WaitGroup
	if clients, _ := strconv.Atoi(os.Getenv("EMPTYCOST_RPC")); clients > 0 {
		body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["latest",false]}`)
		for range clients {
			rpcWg.Add(1)
			go func() {
				defer rpcWg.Done()
				cl := &http.Client{}
				for {
					select {
					case <-rpcStop:
						return
					default:
					}
					req, err := http.NewRequest("POST", eat.JsonRpcUrl, bytes.NewReader(body))
					if err != nil {
						return
					}
					req.Header.Set("Content-Type", "application/json")
					resp, err := cl.Do(req)
					if err != nil {
						continue
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					rpcCalls.Add(1)
				}
			}()
		}
	}

	n := 300
	if v := os.Getenv("EMPTYCOST_N"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	build := make([]float64, 0, n)
	np := make([]float64, 0, n)
	fcu := make([]float64, 0, n)

	start := time.Now()
	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		for range n {

			t0 := time.Now()
			p, err := eat.MockCl.BuildNewPayload(ctx)
			require.NoError(t, err)
			t1 := time.Now()
			_, err = eat.MockCl.InsertNewPayload(ctx, p)
			require.NoError(t, err)
			t2 := time.Now()
			require.NoError(t, eat.MockCl.UpdateForkChoice(ctx, p))
			t3 := time.Now()
			if dir := os.Getenv("EMPTYCOST_DATADIR"); dir != "" {
				_ = os.WriteFile(filepath.Join(dir, "head.txt"), []byte(strconv.FormatUint(uint64(p.ExecutionPayload.BlockNumber), 10)), 0o644)
			}

			build = append(build, t1.Sub(t0).Seconds()*1000)
			np = append(np, t2.Sub(t1).Seconds()*1000)
			fcu = append(fcu, t3.Sub(t2).Seconds()*1000)
		}
	})
	total := time.Since(start)
	close(rpcStop)
	rpcWg.Wait()
	if c := rpcCalls.Load(); c > 0 {
		t.Logf("%-11s n=%d rps=%.0f", "rpc", c, float64(c)/total.Seconds())
	}
	for name, v := range map[string][]float64{"build": build, "newPayload": np, "fcu": fcu} {
		sort.Float64s(v)
		sum := 0.0
		for _, x := range v {
			sum += x
		}
		t.Logf("%-11s n=%d mean=%.2fms p50=%.2f p90=%.2f p99=%.2f max=%.2f", name, len(v), sum/float64(len(v)), v[len(v)/2], v[len(v)*9/10], v[len(v)*99/100], v[len(v)-1])
	}
}
