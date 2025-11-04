package bench

import (
	"context"
	"fmt"
	"math"

	"github.com/erigontech/erigon/cmd/txnbench/internal/rpcclient"
)

const (
	searchUpLimit = 10
)

func GenerateBenchData(ctx context.Context, c *rpcclient.Client) (BenchData, error) {
	latest, err := c.EthBlockNumber(ctx)
	if err != nil {
		return BenchData{}, fmt.Errorf("eth_blockNumber: %w", err)
	}
	chainID, err := c.EthChainID(ctx)
	if err != nil {
		return BenchData{}, fmt.Errorf("eth_chainId: %w", err)
	}

	targets := []uint64{
		uint64(math.Floor(float64(latest) * 0.25)),
		uint64(math.Floor(float64(latest) * 0.50)),
		uint64(math.Floor(float64(latest) * 0.75)),
	}

	blocks := make([]BenchItem, 0, len(targets))
	for _, t := range targets {
		num, hashes, err := findBlockWithTxs(ctx, c, t)
		if err != nil {
			return BenchData{}, err
		}

		take := min(len(hashes), 2)
		item := BenchItem{
			Number: num,
			Txs:    append([]string{}, hashes[:take]...),
		}
		blocks = append(blocks, item)
	}

	return BenchData{
		Meta: BenchMeta{
			ChainID:     chainID,
			LatestBlock: latest,
		},
		Blocks: blocks,
	}, nil
}

func findBlockWithTxs(ctx context.Context, c *rpcclient.Client, start uint64) (uint64, []string, error) {
	for off := uint64(0); off <= searchUpLimit; off++ {
		h := start + off
		b, err := c.EthGetBlockByNumber(ctx, rpcclient.ToHex(h), false)
		if err != nil {
			return 0, nil, fmt.Errorf("get block %d: %w", h, err)
		}
		if len(b.Transactions) > 0 {

			return h, b.Transactions, nil
		}
	}
	return 0, nil, fmt.Errorf("no blocks with txs in [%d..%d]", start, start+searchUpLimit)
}
