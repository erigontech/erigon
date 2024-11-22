package receipts

import (
	"context"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
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
)

type Generator struct {
	receiptsCache *lru.Cache[common.Hash, []*types.Receipt]
	blockReader   services.FullBlockReader
	engine        consensus.EngineReader
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

func NewGenerator(cacheSize int, blockReader services.FullBlockReader,
	engine consensus.EngineReader) *Generator {
	receiptsCache, err := lru.New[common.Hash, []*types.Receipt](cacheSize)
	if err != nil {
		panic(err)
	}

	return &Generator{
		receiptsCache: receiptsCache,
		blockReader:   blockReader,
		engine:        engine,
	}
}

func (g *Generator) GetCachedReceipts(ctx context.Context, blockHash common.Hash) (types.Receipts, bool) {
	return g.receiptsCache.Get(blockHash)
}

func (g *Generator) PrepareEnv(ctx context.Context, block *types.Block, cfg *chain.Config, tx kv.Tx, txIndex int) (*ReceiptEnv, error) {
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, g.blockReader))
	ibs, _, _, _, _, err := transactions.ComputeBlockContext(ctx, g.engine, block.HeaderNoCopy(), cfg, g.blockReader, txNumsReader, tx, txIndex)
	if err != nil {
		return nil, fmt.Errorf("ReceiptsGen: PrepareEnv: bn=%d, %w", block.NumberU64(), err)
	}

	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(cfg.GetMaxBlobGasPerBlock())

	noopWriter := state.NewNoopWriter()

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := g.blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	header := block.HeaderNoCopy()
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

func (g *Generator) GetReceipt(ctx context.Context, cfg *chain.Config, tx kv.TemporalTx, block *types.Block, index int, txNum uint64, optimize bool) (*types.Receipt, error) {
	var receipt *types.Receipt
	genEnv, err := g.PrepareEnv(ctx, block, cfg, tx, index)
	if err != nil {
		return nil, err
	}
	receipt, _, err = core.ApplyTransaction(cfg, core.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, nil, genEnv.gp, genEnv.ibs, genEnv.noopWriter, genEnv.header, block.Transactions()[index], genEnv.usedGas, genEnv.usedBlobGas, vm.Config{})
	if err != nil {
		return nil, fmt.Errorf("ReceiptGen.GetReceipt: bn=%d, txnIdx=%d, %w", block.NumberU64(), index, err)
	}
	receipt.BlockHash = block.Hash()
	cumGasUsed, _, firstLogIndex, err := rawtemporaldb.ReceiptAsOf(tx, txNum)
	if err != nil {
		return nil, err
	}
	println("cum gas used", cumGasUsed, "first log index", firstLogIndex, "txNum", txNum)

	receipt.CumulativeGasUsed = cumGasUsed
	receipt.TransactionIndex = uint(index)

	for i := range receipt.Logs {
		receipt.Logs[i].TxIndex = uint(index)
		receipt.Logs[i].Index = uint(firstLogIndex)
		firstLogIndex++
	}

	return receipt, nil
}

func (g *Generator) GetReceipts(ctx context.Context, cfg *chain.Config, tx kv.Tx, block *types.Block) (types.Receipts, error) {
	if receipts, ok := g.receiptsCache.Get(block.Hash()); ok {
		return receipts, nil
	}

	receipts := make(types.Receipts, len(block.Transactions()))

	genEnv, err := g.PrepareEnv(ctx, block, cfg, tx, 0)
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

	g.receiptsCache.Add(block.Hash(), receipts)
	return receipts, nil
}
