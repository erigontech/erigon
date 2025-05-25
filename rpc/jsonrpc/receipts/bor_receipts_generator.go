package receipts

import (
	"context"
	"fmt"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/consensus"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/transactions"
)

type BorGenerator struct {
	receiptCache *lru.Cache[common.Hash, *types.Receipt]
	blockReader  services.FullBlockReader
	engine       consensus.EngineReader
}

func NewBorGenerator(blockReader services.FullBlockReader,
	engine consensus.EngineReader) *BorGenerator {
	receiptCache, err := lru.New[common.Hash, *types.Receipt](receiptsCacheLimit)
	if err != nil {
		panic(err)
	}

	return &BorGenerator{
		receiptCache: receiptCache,
		blockReader:  blockReader,
		engine:       engine,
	}
}

// GenerateBorReceipt generates the receipt for state sync transactions of a block
func (g *BorGenerator) GenerateBorReceipt(ctx context.Context, tx kv.TemporalTx, block *types.Block,
	msgs []*types.Message, chainConfig *chain.Config) (*types.Receipt, error) {
	if receipt, ok := g.receiptCache.Get(block.Hash()); ok {
		return receipt, nil
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, g.blockReader))
	ibs, blockContext, _, _, _, err := transactions.ComputeBlockContext(ctx, g.engine, block.HeaderNoCopy(), chainConfig, g.blockReader, txNumsReader, tx, len(block.Transactions())) // we want to get the state at the end of the block
	if err != nil {
		return nil, err
	}

	txNum, err := txNumsReader.Max(tx, block.NumberU64())
	if err != nil {
		return nil, err
	}

	cumGasUsedInLastBlock, _, firstLogIndex, err := rawtemporaldb.ReceiptAsOf(tx, txNum+1)
	if err != nil {
		return nil, err
	}

	gp := new(core.GasPool).AddGas(msgs[0].Gas() * uint64(len(msgs))).AddBlobGas(msgs[0].BlobGas() * uint64(len(msgs)))
	evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, chainConfig, vm.Config{})

	receipt, err := applyBorTransaction(msgs, evm, gp, ibs, block, cumGasUsedInLastBlock, uint(firstLogIndex))
	if err != nil {
		return nil, err
	}

	g.receiptCache.Add(block.Hash(), receipt.Copy())
	return receipt, nil
}

func (g *BorGenerator) GenerateBorLogs(ctx context.Context, msgs []*types.Message, txNumsReader rawdbv3.TxNumsReader, tx kv.TemporalTx, header *types.Header, chainConfig *chain.Config, txIndex int, txNum uint64) (types.Logs, error) {
	ibs, blockContext, _, _, _, err := transactions.ComputeBlockContext(ctx, g.engine, header, chainConfig, g.blockReader, txNumsReader, tx, txIndex)
	if err != nil {
		return nil, err
	}

	gp := new(core.GasPool).AddGas(msgs[0].Gas() * uint64(len(msgs))).AddBlobGas(msgs[0].BlobGas() * uint64(len(msgs)))
	evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, chainConfig, vm.Config{})

	_, _, firstLogIndex, err := rawtemporaldb.ReceiptAsOf(tx, txNum+1)
	if err != nil {
		return nil, err
	}
	fmt.Printf("firstLogIndex: %d\n", firstLogIndex)
	return getBorLogs(msgs, evm, gp, ibs, header.Number.Uint64(), header.Hash(), uint(txIndex), uint(firstLogIndex))
}

func getBorLogs(msgs []*types.Message, evm *vm.EVM, gp *core.GasPool, ibs *state.IntraBlockState, blockNum uint64, blockHash common.Hash, txIndex, logIndex uint) (types.Logs, error) {
	for _, msg := range msgs {
		txContext := core.NewEVMTxContext(msg)
		evm.Reset(txContext, ibs)

		_, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, nil /* engine */)
		if err != nil {
			return nil, err
		}
	}

	receiptLogs := ibs.GetLogs(0, bortypes.ComputeBorTxHash(blockNum, blockHash), blockNum, blockHash)

	// set fields
	for i, l := range receiptLogs {
		l.TxIndex = txIndex
		l.Index = logIndex + uint(i)
	}
	return receiptLogs, nil
}

func applyBorTransaction(msgs []*types.Message, evm *vm.EVM, gp *core.GasPool, ibs *state.IntraBlockState, block *types.Block, cumulativeGasUsed uint64, logIndex uint) (*types.Receipt, error) {
	receiptLogs, err := getBorLogs(msgs, evm, gp, ibs, block.Number().Uint64(), block.Hash(), uint(len(block.Transactions())), logIndex)
	if err != nil {
		return nil, err
	}

	numReceipts := len(block.Transactions())
	receipt := types.Receipt{
		Type:              0,
		CumulativeGasUsed: cumulativeGasUsed,
		TxHash:            bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash()),
		GasUsed:           0,
		BlockHash:         block.Hash(),
		BlockNumber:       block.Number(),
		TransactionIndex:  uint(numReceipts),
		Logs:              receiptLogs,
		Status:            types.ReceiptStatusSuccessful,
	}

	return &receipt, nil
}
