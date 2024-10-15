package receipts

import (
	"context"
	lru "github.com/hashicorp/golang-lru/v2"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
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
	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, g.engine, block, cfg, g.blockReader, txNumsReader, tx, txIndex)
	if err != nil {
		return nil, err
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

func (g *Generator) GetReceipt(ctx context.Context, cfg *chain.Config, tx kv.Tx, block *types.Block, index int, optimize bool) (*types.Receipt, error) {
	var receipt *types.Receipt
	if optimize {
		genEnv, err := g.PrepareEnv(ctx, block, cfg, tx, index)
		if err != nil {
			return nil, err
		}
		receipt, _, err = core.ApplyTransaction(cfg, core.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, nil, genEnv.gp, genEnv.ibs, genEnv.noopWriter, genEnv.header, block.Transactions()[index], genEnv.usedGas, genEnv.usedBlobGas, vm.Config{})
		if err != nil {
			return nil, err
		}
		receipt.BlockHash = block.Hash()
	} else {
		timeEnv := time.Now()
		genEnv, err := g.PrepareEnv(ctx, block, cfg, tx, 0)
		println("for prepare env", time.Now().Sub(timeEnv).Milliseconds(), "ms")
		if err != nil {
			return nil, err
		}
		for i, txn := range block.Transactions() {
			genEnv.ibs.SetTxContext(i)
			timeTx := time.Now()
			receipt, _, err = core.ApplyTransaction(cfg, core.GetHashFn(genEnv.header, genEnv.getHeader), g.engine, nil, genEnv.gp, genEnv.ibs, genEnv.noopWriter, genEnv.header, txn, genEnv.usedGas, genEnv.usedBlobGas, vm.Config{})
			if err != nil {
				return nil, err
			}
			receipt.BlockHash = block.Hash()
			println("for exec and get receipt", time.Now().Sub(timeTx).Milliseconds(), "ms")
			if i == index {
				break
			}
		}
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
			return nil, err
		}
		receipt.BlockHash = block.Hash()
		receipts[i] = receipt
	}

	g.receiptsCache.Add(block.Hash(), receipts)
	return receipts, nil
}
