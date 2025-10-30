package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/erigontech/erigon/cmd/txnbench/internal/rpcclient"
	"math"
	"os"
	"time"

	"github.com/pelletier/go-toml/v2"
)

const repeats = 10

func RunBenchmark(ctx context.Context, c *rpcclient.Client, name string) (BenchOutput, error) {
	const benchFile = "benchdata.toml"
	content, err := os.ReadFile(benchFile)
	if err != nil {
		return BenchOutput{}, fmt.Errorf("read %s: %w", benchFile, err)
	}
	var data BenchData
	if err := toml.Unmarshal(content, &data); err != nil {
		return BenchOutput{}, fmt.Errorf("parse %s: %w", benchFile, err)
	}

	chainID, err := c.EthChainID(ctx)
	if err != nil {
		return BenchOutput{}, fmt.Errorf("eth_chainId: %w", err)
	}

	rpcURL := os.Getenv("BENCH_RPC_URL")
	if rpcURL == "" {
		rpcURL = "http://127.0.0.0:8545"
	}

	out := BenchOutput{
		Meta: OutputMeta{
			Name:       name,
			Timestamp:  time.Now().UTC(),
			RPCURL:     rpcURL,
			ChainID:    chainID,
			BenchFile:  benchFile,
			LatestHint: data.Meta.LatestBlock,
		},
	}

	for _, bi := range data.Blocks {
		for _, tx := range bi.Txs {
			// first call (cold-ish)
			t0 := time.Now()
			if _, err := c.EthGetTransactionByHash(ctx, tx); err != nil {
				return BenchOutput{}, fmt.Errorf("eth_getTransactionByHash(%s): %w", tx, err)
			}
			firstMs := float64(time.Since(t0).Microseconds()) / 1000.0

			// 10 reqs — warm avg + stddev
			durs := make([]float64, 0, repeats)
			for i := 0; i < repeats; i++ {
				t1 := time.Now()
				if _, err := c.EthGetTransactionByHash(ctx, tx); err != nil {
					return BenchOutput{}, fmt.Errorf("repeat %d eth_getTransactionByHash(%s): %w", i+1, tx, err)
				}
				ms := float64(time.Since(t1).Microseconds()) / 1000.0
				durs = append(durs, ms)
			}
			avg, std := meanStd(durs)

			out.Results = append(out.Results, TxBenchEntry{
				BlockNumber:     bi.Number,
				TxHash:          tx,
				FirstLatencyMs:  firstMs,
				AvgLatencyMs:    avg,
				StddevLatencyMs: std,
				Repeats:         repeats,
			})
		}
	}

	return out, nil
}

func meanStd(xs []float64) (mean, std float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range xs {
		sum += v
	}
	mean = sum / float64(len(xs))
	var s2 float64
	for _, v := range xs {
		d := v - mean
		s2 += d * d
	}
	if len(xs) > 1 {
		std = math.Sqrt(s2 / float64(len(xs)-1))
	}
	return
}

func isNonNullJSON(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}
