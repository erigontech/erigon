package receipts

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/transactions"
	lru "github.com/hashicorp/golang-lru/v2"
)

type Generator struct {
	receiptsCache      *lru.Cache[common.Hash, types.Receipts]
	receiptCache       *lru.Cache[common.Hash, *types.Receipt]
	receiptsCacheTrace bool
	receiptCacheTrace  bool

	blockReader services.FullBlockReader
	engine      consensus.EngineReader
}

type ReceiptEnv struct {
	ibs         *state.IntraBlockState
	usedGas     *uint64
	usedBlobGas *uint64
	gp          *core.GasPool
	noopWriter  *state.NoopWriter
	getHeader   func(hash common.Hash, number uint64) *types.Header
	header      *types.Header
}

var (
	receiptsCacheLimit = dbg.EnvInt("R_LRU", 1024) //ethmainnet: 1K receipts is ~200mb RAM
	receiptsCacheTrace = dbg.EnvBool("R_LRU_TRACE", false)
)

func NewGenerator(blockReader services.FullBlockReader, engine consensus.EngineReader) *Generator {
	receiptsCache, err := lru.New[common.Hash, types.Receipts](receiptsCacheLimit) //TODO: is handling both of them a good idea though...?
	if err != nil {
		panic(err)
	}

	receiptCache, err := lru.New[common.Hash, *types.Receipt](receiptsCacheLimit * 1000) // think they should be connected in some of that way
	if err != nil {
		panic(err)
	}

	return &Generator{
		receiptsCache:      receiptsCache,
		blockReader:        blockReader,
		engine:             engine,
		receiptsCacheTrace: receiptsCacheTrace,
		receiptCacheTrace:  receiptsCacheTrace,
		receiptCache:       receiptCache,
	}
}

func (g *Generator) LogStats() {
	if g == nil || !g.receiptsCacheTrace {
		return
	}
	//m := g.receiptsCache.Metrics()
	//log.Warn("[dbg] ReceiptsCache", "hit", m.Hits, "total", m.Hits+m.Misses, "Collisions", m.Collisions, "Evictions", m.Evictions, "Inserts", m.Inserts, "limit", receiptsCacheLimit, "ratio", fmt.Sprintf("%.2f", float64(m.Hits)/float64(m.Hits+m.Misses)))
}

func (g *Generator) GetCachedReceipts(ctx context.Context, blockHash common.Hash) (types.Receipts, bool) {
	return g.receiptsCache.Get(blockHash)
}

func (g *Generator) GetCachedReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, bool) {
	return g.receiptCache.Get(hash)
}

func (g *Generator) PrepareEnv(ctx context.Context, header *types.Header, cfg *chain.Config, tx kv.TemporalTx, txIndex int) (*ReceiptEnv, error) {
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, g.blockReader))
	ibs, _, _, _, _, err := transactions.ComputeBlockContext(ctx, g.engine, header, cfg, g.blockReader, txNumsReader, tx, txIndex)
	if err != nil {
		return nil, fmt.Errorf("ReceiptsGen: PrepareEnv: bn=%d, %w", header.Number.Uint64(), err)
	}

	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(core.GasPool).AddGas(header.GasLimit).AddBlobGas(cfg.GetMaxBlobGasPerBlock(header.Time))

	noopWriter := state.NewNoopWriter()

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := g.blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	return &ReceiptEnv{
		ibs:         ibs,
		usedGas:     usedGas,
		usedBlobGas: usedBlobGas,
		gp:          gp,
		noopWriter:  noopWriter,
		getHeader:   getHeader,
		header:      header,
	}, nil
}

func (g *Generator) addToCacheReceipts(header *types.Header, receipts types.Receipts) {
	g.receiptsCache.Add(header.Hash(), receipts.Copy()) // .Copy() helps pprof to attribute memory to cache - instead of evm (where it was allocated).
}

func (g *Generator) addToCacheReceipt(hash common.Hash, receipt *types.Receipt) {
	g.receiptCache.Add(hash, receipt.Copy()) // .Copy() helps pprof to attribute memory to cache - instead of evm (where it was allocated).
}

func (g *Generator) GetReceipt(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, header *types.Header, txn types.Transaction, index int, txNum uint64) (*types.Receipt, error) {
	if receipts, ok := g.receiptsCache.Get(header.Hash()); ok && len(receipts) > index {
		return receipts[index], nil
	}
	if receipt, ok := g.receiptCache.Get(txn.Hash()); ok {
		return receipt, nil
	}

	var receipt *types.Receipt

	genEnv, err := g.PrepareEnv(ctx, header, cfg, tx, index)
	if err != nil {
		return nil, err
	}

	cumGasUsed, _, firstLogIndex, err := rawtemporaldb.ReceiptAsOf(tx, txNum)
	if err != nil {
		return nil, err
	}

	receipt, _, err = core.ApplyTransaction(cfg, core.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, nil, genEnv.gp, genEnv.ibs, genEnv.noopWriter, genEnv.header, txn, genEnv.usedGas, genEnv.usedBlobGas, vm.Config{})
	if err != nil {
		return nil, fmt.Errorf("ReceiptGen.GetReceipt: bn=%d, txnIdx=%d, %w", header.Number.Uint64(), index, err)
	}

	receipt.BlockHash = header.Hash()

	receipt.CumulativeGasUsed = cumGasUsed
	receipt.TransactionIndex = uint(index)

	for i := range receipt.Logs {
		receipt.Logs[i].TxIndex = uint(index)
		receipt.Logs[i].Index = uint(firstLogIndex + uint32(i))
	}

	g.addToCacheReceipt(receipt.TxHash, receipt)
	return receipt, nil
}

func (g *Generator) GetReceipts(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, block *types.Block) (types.Receipts, error) {
	if receipts, ok := g.receiptsCache.Get(block.Hash()); ok {
		return receipts, nil
	}

	receipts := make(types.Receipts, len(block.Transactions()))

	genEnv, err := g.PrepareEnv(ctx, block.HeaderNoCopy(), cfg, tx, 0)
	if err != nil {
		return nil, err
	}

	for i, txn := range block.Transactions() {
		genEnv.ibs.SetTxContext(i)
		receipt, _, err := core.ApplyTransaction(cfg, core.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, nil, genEnv.gp, genEnv.ibs, genEnv.noopWriter, genEnv.header, txn, genEnv.usedGas, genEnv.usedBlobGas, vm.Config{})
		if err != nil {
			return nil, fmt.Errorf("ReceiptGen.GetReceipts: bn=%d, txnIdx=%d, %w", block.NumberU64(), i, err)
		}
		receipt.BlockHash = block.Hash()
		receipts[i] = receipt
	}

	g.addToCacheReceipts(block.HeaderNoCopy(), receipts)
	return receipts, nil
}

func (g *Generator) GetReceiptsGasUsed(tx kv.TemporalTx, block *types.Block, txNumsReader rawdbv3.TxNumsReader) (types.Receipts, error) {
	if receipts, ok := g.receiptsCache.Get(block.Hash()); ok {
		return receipts, nil
	}

	startTxNum, err := txNumsReader.Min(tx, block.NumberU64())
	if err != nil {
		return nil, fmt.Errorf("get min tx num: %w", err)
	}

	receipts := make(types.Receipts, len(block.Transactions()))

	var prevCumGasUsed uint64
	currentTxNum := startTxNum + 1
	for i := range block.Transactions() {
		receipt := &types.Receipt{}
		cumGasUsed, _, _, err := rawtemporaldb.ReceiptAsOf(tx, currentTxNum+1)
		if err != nil {
			return nil, fmt.Errorf("get receipt at tx %d (block %d, index %d): %w",
				currentTxNum, block.NumberU64(), i, err)
		}

		receipt.GasUsed = cumGasUsed - prevCumGasUsed
		receipts[i] = receipt
		log.Info("eth_feeHistory", "blockNum", block.NumberU64(), "txIndex", i, "receipt.gasused", receipts[i].GasUsed, "txNum", currentTxNum, "cumGasUsed", cumGasUsed, "prevCumGasUsed", prevCumGasUsed)

		prevCumGasUsed = cumGasUsed
		currentTxNum++
	}

	return receipts, nil
}
