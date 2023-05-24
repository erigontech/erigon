// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package gasprice

import (
	"container/heap"
	"context"
	"errors"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/eth/gasprice/gaspricecfg"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
)

const sampleNumber = 3 // Number of transactions sampled in a block

// OracleBackend includes all necessary background APIs for oracle.
type OracleBackend interface {
	HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error)
	BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error)
	ChainConfig() *chain.Config

	GetReceipts(ctx context.Context, hash libcommon.Hash) (types.Receipts, error)
	PendingBlockAndReceipts() (*types.Block, types.Receipts)
}

type Cache interface {
	GetLatest() (libcommon.Hash, *big.Int)
	SetLatest(hash libcommon.Hash, price *big.Int)
}

// Oracle recommends gas prices based on the content of recent
// blocks. Suitable for both light and full clients.
type Oracle struct {
	backend     OracleBackend
	lastHead    libcommon.Hash
	lastPrice   *big.Int
	maxPrice    *big.Int
	ignorePrice *big.Int
	cache       Cache

	checkBlocks                       int
	percentile                        int
	maxHeaderHistory, maxBlockHistory int
}

// NewOracle returns a new gasprice oracle which can recommend suitable
// gasprice for newly created transaction.
func NewOracle(backend OracleBackend, params gaspricecfg.Config, cache Cache) *Oracle {
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
	if maxPrice == nil || maxPrice.Int64() <= 0 {
		maxPrice = gaspricecfg.DefaultMaxPrice
		log.Warn("Sanitizing invalid gasprice oracle price cap", "provided", params.MaxPrice, "updated", maxPrice)
	}
	ignorePrice := params.IgnorePrice
	if ignorePrice == nil || ignorePrice.Int64() < 0 {
		ignorePrice = gaspricecfg.DefaultIgnorePrice
		log.Warn("Sanitizing invalid gasprice oracle ignore price", "provided", params.IgnorePrice, "updated", ignorePrice)
	}
	return &Oracle{
		backend:          backend,
		lastPrice:        params.Default,
		maxPrice:         maxPrice,
		ignorePrice:      ignorePrice,
		checkBlocks:      blocks,
		percentile:       percent,
		cache:            cache,
		maxHeaderHistory: params.MaxHeaderHistory,
		maxBlockHistory:  params.MaxBlockHistory,
	}
}

// SuggestTipCap returns a TipCap so that newly created transaction can
// have a very high chance to be included in the following blocks.
// NODE: if caller wants legacy tx SuggestedPrice, we need to add
// baseFee to the returned bigInt
func (oracle *Oracle) SuggestTipCap(ctx context.Context) (*big.Int, error) {
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
	txPrices := make(sortingHeap, 0, sampleNumber*oracle.checkBlocks)
	for txPrices.Len() < sampleNumber*oracle.checkBlocks && number > 0 {
		err := oracle.getBlockPrices(ctx, number, sampleNumber, oracle.ignorePrice, &txPrices)
		if err != nil {
			return latestPrice, err
		}
		number--
	}
	price := latestPrice
	if txPrices.Len() > 0 {
		// Item with this position needs to be extracted from the sorting heap
		// so we pop all the items before it
		percentilePosition := (txPrices.Len() - 1) * oracle.percentile / 100
		for i := 0; i < percentilePosition; i++ {
			heap.Pop(&txPrices)
		}
	}
	if txPrices.Len() > 0 {
		// Don't need to pop it, just take from the top of the heap
		price = txPrices[0].ToBig()
	}
	if price.Cmp(oracle.maxPrice) > 0 {
		price = new(big.Int).Set(oracle.maxPrice)
	}

	oracle.cache.SetLatest(headHash, price)

	return price, nil
}

type transactionsByGasPrice struct {
	txs     []types.Transaction
	baseFee *uint256.Int
}

func newTransactionsByGasPrice(txs []types.Transaction,
	baseFee *uint256.Int) transactionsByGasPrice {
	return transactionsByGasPrice{
		txs:     txs,
		baseFee: baseFee,
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
func (t *transactionsByGasPrice) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	l, ok := x.(types.Transaction)
	if !ok {
		log.Error("Type assertion failure", "err", "cannot get types.Transaction from interface")
	}
	t.txs = append(t.txs, l)
}

// Pop (part of heap.Interface) removes the first link from the queue
func (t *transactionsByGasPrice) Pop() interface{} {
	old := t.txs
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	t.txs = old[0 : n-1]
	return x
}

// getBlockPrices calculates the lowest transaction gas price in a given block.
// the block is empty or all transactions are sent by the miner
// itself(it doesn't make any sense to include this kind of transaction prices for sampling),
// nil gasprice is returned.
func (oracle *Oracle) getBlockPrices(ctx context.Context, blockNum uint64, limit int,
	ingoreUnderBig *big.Int, s *sortingHeap) error {
	ignoreUnder, overflow := uint256.FromBig(ingoreUnderBig)
	if overflow {
		err := errors.New("overflow in getBlockPrices, gasprice.go: ignoreUnder too large")
		log.Error("gasprice.go: getBlockPrices", "err", err)
		return err
	}
	block, err := oracle.backend.BlockByNumber(ctx, rpc.BlockNumber(blockNum))
	if err != nil {
		log.Error("gasprice.go: getBlockPrices", "err", err)
		return err
	}

	if block == nil {
		return nil
	}

	blockTxs := block.Transactions()
	plainTxs := make([]types.Transaction, len(blockTxs))
	copy(plainTxs, blockTxs)
	var baseFee *uint256.Int
	if block.BaseFee() == nil {
		baseFee = nil
	} else {
		baseFee, overflow = uint256.FromBig(block.BaseFee())
		if overflow {
			err := errors.New("overflow in getBlockPrices, gasprice.go: baseFee > 2^256-1")
			log.Error("gasprice.go: getBlockPrices", "err", err)
			return err
		}
	}
	txs := newTransactionsByGasPrice(plainTxs, baseFee)
	heap.Init(&txs)

	count := 0
	for count < limit && txs.Len() > 0 {
		tx := heap.Pop(&txs).(types.Transaction)
		tip := tx.GetEffectiveGasTip(baseFee)
		if ignoreUnder != nil && tip.Lt(ignoreUnder) {
			continue
		}
		sender, _ := tx.GetSender()
		if err == nil && sender != block.Coinbase() {
			heap.Push(s, tip)
			count = count + 1
		}
	}
	return nil
}

type sortingHeap []*uint256.Int

func (s sortingHeap) Len() int           { return len(s) }
func (s sortingHeap) Less(i, j int) bool { return s[i].Lt(s[j]) }
func (s sortingHeap) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Push (part of heap.Interface) places a new link onto the end of queue
func (s *sortingHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	l := x.(*uint256.Int)
	*s = append(*s, l)
}

// Pop (part of heap.Interface) removes the first link from the queue
func (s *sortingHeap) Pop() interface{} {
	old := *s
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*s = old[0 : n-1]
	return x
}
