package bench

import "time"

// benchdata.toml

type BenchData struct {
	Meta   BenchMeta   `toml:"meta"`
	Blocks []BenchItem `toml:"blocks"`
}

type BenchMeta struct {
	ChainID     uint64 `toml:"chain_id"`
	LatestBlock uint64 `toml:"latest_block"`
}

type BenchItem struct {
	Number uint64   `toml:"number"`
	Txs    []string `toml:"txs"`
}

// results <name>.json

type BenchOutput struct {
	Meta    OutputMeta     `json:"meta"`
	Results []TxBenchEntry `json:"results"`
}

type OutputMeta struct {
	Name       string    `json:"name"`
	Timestamp  time.Time `json:"timestamp"`
	RPCURL     string    `json:"rpc_url"`
	ChainID    uint64    `json:"chain_id"`
	BenchFile  string    `json:"bench_file"`
	LatestHint uint64    `json:"latest_block_hint"`
}

type TxBenchEntry struct {
	BlockNumber     uint64  `json:"block"`
	TxHash          string  `json:"tx_hash"`
	FirstLatencyMs  float64 `json:"first_latency_ms"`
	AvgLatencyMs    float64 `json:"avg_latency_ms"`
	StddevLatencyMs float64 `json:"stddev_latency_ms"`
	Repeats         int     `json:"repeats"`
}
