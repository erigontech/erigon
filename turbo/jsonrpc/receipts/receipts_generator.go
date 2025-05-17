package receipts

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/go-cmp/cmp"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/transactions"
)

type Generator struct {
	receiptsCache *lru.Cache[common.Hash, types.Receipts]
	receiptCache  *lru.Cache[common.Hash, *types.Receipt]

	// blockExecMutex ensuring that only 1 block with given hash
	// executed at a time - all parallel requests for same hash will wait for results
	// "Requesting near-chain-tip block receipts" - is very common RPC request, means we facing many similar parallel requrest
	blockExecMutex *loaderMutex[common.Hash] // only
	txnExecMutex   *loaderMutex[common.Hash] // only 1 txn with current hash executed at a time - same parallel requests are waiting for results

	receiptsCacheTrace bool
	receiptCacheTrace  bool

	blockReader services.FullBlockReader
	txNumReader rawdbv3.TxNumsReader
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

	txNumReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(context.Background(), blockReader))

	return &Generator{
		receiptsCache:      receiptsCache,
		blockReader:        blockReader,
		txNumReader:        txNumReader,
		engine:             engine,
		receiptsCacheTrace: receiptsCacheTrace,
		receiptCacheTrace:  receiptsCacheTrace,
		receiptCache:       receiptCache,

		blockExecMutex: &loaderMutex[common.Hash]{},
		txnExecMutex:   &loaderMutex[common.Hash]{},
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
	blockHash := header.Hash()
	blockNum := header.Number.Uint64()
	txnHash := txn.Hash()

	//if can find in DB - then don't need store in `receiptsCache` - because DB it's already kind-of cache (small, mmaped, hot file)
	receiptFromDB, ok, err := rawdb.ReadReceiptCache(tx, blockNum, blockHash, uint32(index), txnHash)
	if err != nil {
		return nil, err
	}
	if ok && receiptFromDB != nil && !dbg.AssertEnabled {
		return receiptFromDB, nil
	}

	//if can find in DB - then don't need store in `receiptsCache` - because DB it's already kind-of cache (small, mmaped, hot file)
	receiptFromDB, ok, err = rawdb.ReadReceiptCacheV2(tx, blockNum, blockHash, txNum, txnHash)
	if err != nil {
		return nil, err
	}
	if ok && receiptFromDB != nil && !dbg.AssertEnabled {
		return receiptFromDB, nil
	}

	if receipts, ok := g.receiptsCache.Get(blockHash); ok && len(receipts) > index {
		return receipts[index], nil
	}

	mu := g.txnExecMutex.lock(txnHash)
	defer g.txnExecMutex.unlock(mu, txnHash)
	if receipt, ok := g.receiptCache.Get(txnHash); ok {
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
		return nil, fmt.Errorf("ReceiptGen.GetReceipt: bn=%d, txnIdx=%d, %w", blockNum, index, err)
	}

	receipt.BlockHash = blockHash
	receipt.CumulativeGasUsed = cumGasUsed
	receipt.TransactionIndex = uint(index)
	receipt.FirstLogIndexWithinBlock = firstLogIndex

	for i := range receipt.Logs {
		receipt.Logs[i].TxIndex = uint(index)
		receipt.Logs[i].Index = uint(firstLogIndex + uint32(i))
	}

	g.addToCacheReceipt(receipt.TxHash, receipt)

	if dbg.AssertEnabled && receiptFromDB != nil {
		g.assertEqualReceipts(receipt, receiptFromDB)
	}
	return receipt, nil
}

func (g *Generator) GetReceipts(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, block *types.Block) (types.Receipts, error) {
	blockHash := block.Hash()

	//if can find in DB - then don't need store in `receiptsCache` - because DB it's already kind-of cache (small, mmaped, hot file)
	receiptsFromDB, err := rawdb.ReadReceiptsCache(tx, block)
	if err != nil {
		return nil, err
	}
	if len(receiptsFromDB) > 0 && !dbg.AssertEnabled {
		return receiptsFromDB, nil
	}

	//if can find in DB - then don't need store in `receiptsCache` - because DB it's already kind-of cache (small, mmaped, hot file)
	receiptsFromDB, err = rawdb.ReadReceiptsCacheV2(tx, block, g.txNumReader)
	if err != nil {
		return nil, err
	}
	if len(receiptsFromDB) > 0 && !dbg.AssertEnabled {
		return receiptsFromDB, nil
	}

	mu := g.blockExecMutex.lock(blockHash) // parallel requests of same blockNum will executed only once
	defer g.blockExecMutex.unlock(mu, blockHash)
	if receipts, ok := g.receiptsCache.Get(blockHash); ok {
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
		receipt.BlockHash = blockHash
		if len(receipt.Logs) > 0 {
			receipt.FirstLogIndexWithinBlock = uint32(receipt.Logs[0].Index)
		}
		receipts[i] = receipt

		if dbg.AssertEnabled && receiptsFromDB != nil && len(receipts) > 0 {
			g.assertEqualReceipts(receipt, receiptsFromDB[i])
		}
	}

	g.addToCacheReceipts(block.HeaderNoCopy(), receipts)
	return receipts, nil
}

func (g *Generator) assertEqualReceipts(fromExecution, fromDB *types.Receipt) {
	toJson := func(a interface{}) string {
		aa, err := json.Marshal(a)
		if err != nil {
			panic(err)
		}
		return string(aa)
	}

	generated := fromExecution.Copy()
	if generated.TransactionIndex != fromDB.TransactionIndex {
		panic(fmt.Sprintf("assert: %d, %d", generated.TransactionIndex, fromDB.TransactionIndex))
	}
	if generated.FirstLogIndexWithinBlock != fromDB.FirstLogIndexWithinBlock {
		panic(fmt.Sprintf("assert: %d, %d", generated.FirstLogIndexWithinBlock, fromDB.FirstLogIndexWithinBlock))
	}

	for i := range generated.Logs {
		a := toJson(generated.Logs[i])
		b := toJson(fromDB.Logs[i])
		if a != b {
			panic(fmt.Sprintf("assert: %v, bn=%d, txnIdx=%d", cmp.Diff(a, b), generated.BlockNumber.Uint64(), generated.TransactionIndex))
		}
	}
	fromDB.Logs, generated.Logs = nil, nil
	fromDB.Bloom, generated.Bloom = types.Bloom{}, types.Bloom{}
	a := toJson(generated)
	b := toJson(fromDB)
	if a != b {
		panic(fmt.Sprintf("assert: %v, bn=%d, txnIdx=%d", cmp.Diff(a, b), generated.BlockNumber.Uint64(), generated.TransactionIndex))
	}
}

func (g *Generator) GetReceiptsGasUsed(tx kv.TemporalTx, block *types.Block, txNumsReader rawdbv3.TxNumsReader) (types.Receipts, error) {
	if receipts, ok := g.receiptsCache.Get(block.Hash()); ok {
		return receipts, nil
	}

	startTxNum, err := txNumsReader.Min(tx, block.NumberU64())
	if err != nil {
		return nil, err
	}

	receipts := make(types.Receipts, len(block.Transactions()))

	var prevCumGasUsed uint64
	currentTxNum := startTxNum + 1
	for i := range block.Transactions() {
		receipt := &types.Receipt{}
		cumGasUsed, _, _, err := rawtemporaldb.ReceiptAsOf(tx, currentTxNum+1)
		if err != nil {
			return nil, fmt.Errorf("ReceiptGen.GetReceiptsGasUsed: at tx %d (block %d, index %d): %w",
				currentTxNum, block.NumberU64(), i, err)
		}

		receipt.GasUsed = cumGasUsed - prevCumGasUsed
		receipts[i] = receipt

		prevCumGasUsed = cumGasUsed
		currentTxNum++
	}

	return receipts, nil
}

type loaderMutex[K comparable] struct {
	sync.Map
}

func (m *loaderMutex[K]) lock(key K) *sync.Mutex {
	value, _ := m.LoadOrStore(key, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu
}

func (m *loaderMutex[K]) unlock(mu *sync.Mutex, key K) {
	mu.Unlock()
	m.Delete(key)
}
