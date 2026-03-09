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
	"container/heap"
	"context"
	"sort"

	"github.com/holiman/uint256"

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
	txPrices := make([]*uint256.Int, 0, sampleNumber*oracle.checkBlocks)
	for len(txPrices) < sampleNumber*oracle.checkBlocks && number > 0 {
		if err := oracle.getBlockPrices(ctx, number, sampleNumber, oracle.ignorePrice, &txPrices); err != nil {
			return latestPrice, err
		}
		number--
	}
	price := latestPrice
	if len(txPrices) > 0 {
		sort.Slice(txPrices, func(i, j int) bool { return txPrices[i].Lt(txPrices[j]) })
		index := (len(txPrices) - 1) * oracle.percentile / 100
		price = txPrices[index]
	}

	if price.Cmp(oracle.maxPrice) > 0 {
		price = new(uint256.Int).Set(oracle.maxPrice)
	}

	oracle.cache.SetLatest(headHash, price)

	return price, nil
}

type transactionsByGasPrice struct {
	txs     []types.Transaction
	baseFee *uint256.Int
	log     log.Logger
}

func newTransactionsByGasPrice(txs []types.Transaction,
	baseFee *uint256.Int, log log.Logger) transactionsByGasPrice {
	return transactionsByGasPrice{
		txs:     txs,
		baseFee: baseFee,
		log:     log,
	}
}

func (t transactionsByGasPrice) Len() int      { return len(t.txs) }
func (t transactionsByGasPrice) Swap(i, j int) { t.txs[i], t.txs[j] = t.txs[j], t.txs[i] }
func (t transactionsByGasPrice) Less(i, j int) bool {
	tip1 := t.txs[i].GetEffectiveGasTip(t.baseFee)
	tip2 := t.txs[j].GetEffectiveGasTip(t.baseFee)
	return tip1.Lt(tip2)
}

// Push (part of heap.Interface) places a new link onto the end of queue
func (t *transactionsByGasPrice) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	l, ok := x.(types.Transaction)
	if !ok {
		t.log.Error("Type assertion failure", "err", "cannot get types.Transaction from interface")
	}
	t.txs = append(t.txs, l)
}

// Pop (part of heap.Interface) removes the first link from the queue
func (t *transactionsByGasPrice) Pop() any {
	old := t.txs
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	t.txs = old[0 : n-1]
	return x
}

// getBlockPrices calculates the lowest transaction gas price in a given block.
// the block is empty or all transactions are sent by the miner
// itself(it doesn't make any sense to include this kind of transaction prices for sampling),
// nil gasprice is returned.
func (oracle *Oracle) getBlockPrices(ctx context.Context, blockNum uint64, limit int,
	ignoreUnder *uint256.Int, out *[]*uint256.Int) error {
	block, err := oracle.backend.BlockByNumber(ctx, rpc.BlockNumber(blockNum))
	if err != nil {
		oracle.log.Error("getBlockPrices", "err", err)
		return err
	}

	if block == nil {
		return nil
	}

	blockTxs := block.Transactions()
	plainTxs := make([]types.Transaction, len(blockTxs))
	copy(plainTxs, blockTxs)
	baseFee := block.BaseFee()
	txs := newTransactionsByGasPrice(plainTxs, baseFee, oracle.log)
	heap.Init(&txs)

	count := 0
	for count < limit && txs.Len() > 0 {
		tx := heap.Pop(&txs).(types.Transaction)
		tip := tx.GetEffectiveGasTip(baseFee)
		if ignoreUnder != nil && tip.Lt(ignoreUnder) {
			continue
		}
		sender, _ := tx.GetSender()
		if sender.Value() != block.Coinbase() {
			*out = append(*out, tip)
			count = count + 1
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
