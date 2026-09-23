package engineapi_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"

	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

func TestZZZEmptyPayloadCost(t *testing.T) {
	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlCrit)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:        logger,
		DataDir:       t.TempDir(),
		Genesis:       genesis,
		CoinbaseKey:   coinbaseKey,
		DisableSentry: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	n := 300
	build := make([]float64, 0, n)
	np := make([]float64, 0, n)
	fcu := make([]float64, 0, n)

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

			build = append(build, t1.Sub(t0).Seconds()*1000)
			np = append(np, t2.Sub(t1).Seconds()*1000)
			fcu = append(fcu, t3.Sub(t2).Seconds()*1000)
		}
	})
	for name, v := range map[string][]float64{"build": build, "newPayload": np, "fcu": fcu} {
		sort.Float64s(v)
		sum := 0.0
		for _, x := range v {
			sum += x
		}
		t.Logf("%-11s n=%d mean=%.2fms p50=%.2f p90=%.2f p99=%.2f max=%.2f", name, len(v), sum/float64(len(v)), v[len(v)/2], v[len(v)*9/10], v[len(v)*99/100], v[len(v)-1])
	}
}
