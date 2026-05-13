// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package gasprice

import (
	"context"
	"slices"
	"sync/atomic"

	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/gasprice/gaspricecfg"
)

const sampleNumber = 3 // Number of transactions sampled in a block

// OracleBackend includes all necessary background APIs for oracle.
type OracleBackend interface {
	HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error)
	ChainConfig() *chain.Config
	GetLatestBlockNumber() (uint64, error)

	GetReceiptsGasUsed(ctx context.Context, block *types.Block) (types.Receipts, error)
	PendingBlockAndReceipts() (*types.Block, types.Receipts)

	// Fork opens a new TemporalTx and returns a goroutine-local backend together
	// with a cleanup function (call via defer cleanup()).
	// If the backend does not support forking, it returns (nil, nil, nil) and
	// the caller should fall back to using the main backend sequentially.
	Fork(ctx context.Context) (OracleBackend, func(), error)
}

type Cache interface {
	GetLatest() (common.Hash, *uint256.Int)
	SetLatest(hash common.Hash, price *uint256.Int)
}

// Oracle recommends gas prices based on the content of recent
// blocks. Suitable for both light and full clients.
type Oracle struct {
	backend      OracleBackend
	maxPrice     *uint256.Int
	ignorePrice  *uint256.Int
	cache        Cache
	historyCache *FeeHistoryCache

	checkBlocks                       int
	percentile                        int
	maxHeaderHistory, maxBlockHistory int

	log log.Logger
}

// NewOracle returns a new gasprice oracle which can recommend suitable
// gasprice for newly created transaction.
func NewOracle(backend OracleBackend, params gaspricecfg.Config, cache Cache, historyCache *FeeHistoryCache, log log.Logger) *Oracle {
	blocks := params.Blocks
	if blocks < 1 {
		blocks = 1
		log.Warn("Sanitizing invalid gasprice oracle sample blocks", "provided", params.Blocks, "updated", blocks)
	}
	percent := params.Percentile
	if percent < 0 {
		percent = 0
		log.Warn("Sanitizing invalid gasprice oracle sample percentile", "provided", params.Percentile, "updated", percent)
	}
	if percent > 100 {
		percent = 100
		log.Warn("Sanitizing invalid gasprice oracle sample percentile", "provided", params.Percentile, "updated", percent)
	}
	maxPrice := params.MaxPrice
	if maxPrice == nil || maxPrice.IsZero() {
		maxPrice = gaspricecfg.DefaultMaxPrice
		log.Warn("Sanitizing invalid gasprice oracle price cap", "provided", params.MaxPrice, "updated", maxPrice)
	}
	ignorePrice := params.IgnorePrice
	if ignorePrice == nil {
		ignorePrice = gaspricecfg.DefaultIgnorePrice
		log.Warn("Sanitizing invalid gasprice oracle ignore price", "provided", params.IgnorePrice, "updated", ignorePrice)
	}

	setBorDefaultGpoIgnorePrice(backend.ChainConfig(), params, log)

	return &Oracle{
		backend:          backend,
		maxPrice:         maxPrice,
		ignorePrice:      ignorePrice,
		checkBlocks:      blocks,
		percentile:       percent,
		cache:            cache,
		historyCache:     historyCache,
		maxHeaderHistory: params.MaxHeaderHistory,
		maxBlockHistory:  params.MaxBlockHistory,
		log:              log,
	}
}

// SuggestTipCap returns a TipCap so that newly created transaction can
// have a very high chance to be included in the following blocks.
// NODE: if caller wants legacy txn SuggestedPrice, we need to add
// baseFee to the returned bigInt
func (oracle *Oracle) SuggestTipCap(ctx context.Context) (*uint256.Int, error) {
	latestHead, latestPrice := oracle.cache.GetLatest()
	head, err := oracle.backend.HeaderByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return latestPrice, err
	}
	if head == nil {
		return latestPrice, nil
	}

	headHash := head.Hash()
	if latestHead == headHash {
		return latestPrice, nil
	}

	// check again, the last request could have populated the cache
	latestHead, latestPrice = oracle.cache.GetLatest()
	if latestHead == headHash {
		return latestPrice, nil
	}

	number := head.Number.Uint64()
	txPrices := make([]*uint256.Int, 0, 2*sampleNumber*oracle.checkBlocks)

	// Phase 1: fetch checkBlocks blocks in parallel (or sequentially when Fork
	// is not supported by the backend).
	count1 := oracle.checkBlocks
	if uint64(count1) > number {
		count1 = int(number)
	}
	phase1, err := oracle.fetchBlockPricesParallel(ctx, number, count1)
	if err != nil {
		return latestPrice, err
	}

	// Post-process phase 1: apply the empty-block fallback (issue #17617) and
	// count sparse blocks to decide whether a phase-2 extension is needed.
	sparseCount := 0
	for _, prices := range phase1 {
		added := len(prices)
		// Empty block (or all transactions from coinbase): fall back to the
		// last known price so the percentile estimate stays stable.
		if added == 0 && latestPrice != nil {
			txPrices = append(txPrices, new(uint256.Int).Set(latestPrice))
			added = 1
		} else {
			txPrices = append(txPrices, prices...)
		}
		if added <= 1 {
			sparseCount++
		}
	}

	// Phase 2: extend the window by up to sparseCount extra blocks (capped at
	// checkBlocks), mirroring geth's adaptive extension up to 2*checkBlocks.
	if sparseCount > 0 && number > uint64(count1) {
		count2 := min(sparseCount, oracle.checkBlocks)
		startBlock2 := number - uint64(count1)
		if uint64(count2) > startBlock2 {
			count2 = int(startBlock2)
		}
		if count2 > 0 {
			phase2, err := oracle.fetchBlockPricesParallel(ctx, startBlock2, count2)
			if err != nil {
				return latestPrice, err
			}
			for _, prices := range phase2 {
				if len(prices) == 0 && latestPrice != nil {
					txPrices = append(txPrices, new(uint256.Int).Set(latestPrice))
				} else {
					txPrices = append(txPrices, prices...)
				}
			}
		}
	}

	price := latestPrice
	if len(txPrices) > 0 {
		slices.SortFunc(txPrices, func(a, b *uint256.Int) int { return a.Cmp(b) })
		index := (len(txPrices) - 1) * oracle.percentile / 100
		price = txPrices[index]
	}

	if price != nil && price.Cmp(oracle.maxPrice) > 0 {
		price = new(uint256.Int).Set(oracle.maxPrice)
	}

	oracle.cache.SetLatest(headHash, price)

	return price, nil
}

// fetchBlockPricesParallel fetches prices for count consecutive blocks, starting
// at head and going backwards. Work is distributed across up to maxBlockFetchers
// goroutines; each goroutine opens its own read transaction via Fork so MDBX
// transactions are never shared across goroutines.
//
// When Fork is not supported (returns nil backend), a single goroutine falls back
// to using the main backend sequentially; all other goroutines exit immediately to
// avoid concurrent access on the shared transaction.
//
// The returned slice has length count: entry i corresponds to block head-i.
func (oracle *Oracle) fetchBlockPricesParallel(ctx context.Context, head uint64, count int) ([][]*uint256.Int, error) {
	results := make([][]*uint256.Int, count)
	var (
		nextIdx uint64
		seqOnce int32 // CAS flag: 0 = available, 1 = sequential mode claimed
	)
	g, fetchCtx := errgroup.WithContext(ctx)
	for range min(maxBlockFetchers, count) {
		g.Go(func() error {
			localBackend, cleanup, forkErr := oracle.backend.Fork(fetchCtx)
			if forkErr != nil {
				return forkErr
			}
			if localBackend == nil {
				// Fork not supported: allow exactly one goroutine to proceed
				// sequentially on the shared backend; the others exit.
				if !atomic.CompareAndSwapInt32(&seqOnce, 0, 1) {
					return nil
				}
				localBackend = oracle.backend
			} else if cleanup != nil {
				defer cleanup()
			}
			for {
				if err := fetchCtx.Err(); err != nil {
					return err
				}
				idx := int(atomic.AddUint64(&nextIdx, 1)) - 1
				if idx >= count {
					return nil
				}
				blockNum := head - uint64(idx)
				prices := make([]*uint256.Int, 0, sampleNumber)
				if err := oracle.getBlockPricesFromBackend(fetchCtx, localBackend, blockNum, sampleNumber, oracle.ignorePrice, &prices); err != nil {
					return err
				}
				results[idx] = prices
			}
		})
	}
	return results, g.Wait()
}

// getBlockPricesFromBackend calculates the lowest transaction gas prices in a
// given block using the supplied backend (which may be a Fork'd per-goroutine
// backend). Empty blocks or blocks where all transactions come from the coinbase
// contribute nothing to out; the caller applies its own fallback.
//
// Effective tips are pre-computed once per transaction to avoid repeated
// allocations that occurred when a heap's Less method called GetEffectiveGasTip
// on every comparison (O(n log n) allocations). Now we allocate exactly once
// per transaction (O(n)) and sort with slices.SortFunc (pdqsort).
func (oracle *Oracle) getBlockPricesFromBackend(ctx context.Context, backend OracleBackend, blockNum uint64, limit int,
	ignoreUnder *uint256.Int, out *[]*uint256.Int) error {
	block, err := backend.BlockByNumber(ctx, rpc.BlockNumber(blockNum))
	if err != nil {
		oracle.log.Error("getBlockPrices", "err", err)
		return err
	}
	if block == nil {
		return nil
	}

	baseFee := block.BaseFee()
	coinbase := block.Coinbase()

	// Pre-compute effective tip for every transaction exactly once.
	type txWithTip struct {
		tx  types.Transaction
		tip *uint256.Int
	}
	items := make([]txWithTip, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		items[i] = txWithTip{tx: tx, tip: tx.GetEffectiveGasTip(baseFee)}
	}

	// Sort ascending by effective tip; slices.SortFunc uses pdqsort (no reflection).
	slices.SortFunc(items, func(a, b txWithTip) int { return a.tip.Cmp(b.tip) })

	// Since items are sorted ascending, all tips below ignoreUnder form a
	// contiguous prefix that we can skip with a single pass.
	start := 0
	if ignoreUnder != nil {
		for start < len(items) && items[start].tip.Lt(ignoreUnder) {
			start++
		}
	}

	count := 0
	for _, item := range items[start:] {
		if count >= limit {
			break
		}
		sender, _ := item.tx.GetSender()
		if sender.Value() != coinbase {
			*out = append(*out, item.tip)
			count++
		}
	}
	return nil
}

// setBorDefaultGpoIgnorePrice enforces gpo IgnorePrice to be equal to BorDefaultGpoIgnorePrice (25gwei by default)
func setBorDefaultGpoIgnorePrice(chainConfig *chain.Config, gasPriceConfig gaspricecfg.Config, log log.Logger) {
	if chainConfig.Bor != nil && gasPriceConfig.IgnorePrice != gaspricecfg.BorDefaultGpoIgnorePrice {
		log.Warn("Sanitizing invalid bor gasprice oracle ignore price", "provided", gasPriceConfig.IgnorePrice, "updated", gaspricecfg.BorDefaultGpoIgnorePrice)
		gasPriceConfig.IgnorePrice = gaspricecfg.BorDefaultGpoIgnorePrice
	}
}
