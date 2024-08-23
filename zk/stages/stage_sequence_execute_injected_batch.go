package stages

import (
	"math"

	"errors"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
	"github.com/ledgerwatch/erigon/zk/utils"
)

const (
	injectedBatchBlockNumber = 1
	injectedBatchBatchNumber = 1
)

func processInjectedInitialBatch(
	batchContext *BatchContext,
	batchState *BatchState,
) error {
	// set the block height for the fork we're running at to ensure contract interactions are correct
	if err := utils.RecoverySetBlockConfigForks(injectedBatchBlockNumber, batchState.forkId, batchContext.cfg.chainConfig, batchContext.s.LogPrefix()); err != nil {
		return err
	}

	header, parentBlock, err := prepareHeader(batchContext.sdb.tx, 0, math.MaxUint64, math.MaxUint64, batchState.forkId, batchContext.cfg.zk.AddressSequencer, batchContext.cfg.chainConfig, batchContext.cfg.miningConfig)
	if err != nil {
		return err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(batchContext.sdb.tx, hash, number)
	}
	getHashFn := core.GetHashFn(header, getHeader)
	blockContext := core.NewEVMBlockContext(header, getHashFn, batchContext.cfg.engine, &batchContext.cfg.zk.AddressSequencer)

	injected, err := batchContext.sdb.hermezDb.GetL1InjectedBatch(0)
	if err != nil {
		return err
	}

	fakeL1TreeUpdate := &zktypes.L1InfoTreeUpdate{
		GER:        injected.LastGlobalExitRoot,
		ParentHash: injected.L1ParentHash,
		Timestamp:  injected.Timestamp,
	}

	ibs := state.New(batchContext.sdb.stateReader)

	// the injected batch block timestamp should also match that of the injected batch
	header.Time = injected.Timestamp

	parentRoot := parentBlock.Root()
	if err = handleStateForNewBlockStarting(batchContext, ibs, injectedBatchBlockNumber, injectedBatchBatchNumber, injected.Timestamp, &parentRoot, fakeL1TreeUpdate, true); err != nil {
		return err
	}

	txn, receipt, execResult, effectiveGas, err := handleInjectedBatch(batchContext, ibs, &blockContext, injected, header, parentBlock, batchState.forkId)
	if err != nil {
		return err
	}

	batchState.blockState.builtBlockElements = BuiltBlockElements{
		transactions:     types.Transactions{*txn},
		receipts:         types.Receipts{receipt},
		executionResults: []*core.ExecutionResult{execResult},
		effectiveGases:   []uint8{effectiveGas},
	}
	batchCounters := vm.NewBatchCounterCollector(batchContext.sdb.smt.GetDepth(), uint16(batchState.forkId), batchContext.cfg.zk.VirtualCountersSmtReduction, batchContext.cfg.zk.ShouldCountersBeUnlimited(batchState.isL1Recovery()), nil)

	if _, err = doFinishBlockAndUpdateState(batchContext, ibs, header, parentBlock, batchState, injected.LastGlobalExitRoot, injected.L1ParentHash, 0, 0, batchCounters); err != nil {
		return err
	}

	return err
}

func handleInjectedBatch(
	batchContext *BatchContext,
	ibs *state.IntraBlockState,
	blockContext *evmtypes.BlockContext,
	injected *zktypes.L1InjectedBatch,
	header *types.Header,
	parentBlock *types.Block,
	forkId uint64,
) (*types.Transaction, *types.Receipt, *core.ExecutionResult, uint8, error) {
	decodedBlocks, err := zktx.DecodeBatchL2Blocks(injected.Transaction, forkId)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	if len(decodedBlocks) == 0 || len(decodedBlocks) > 1 {
		return nil, nil, nil, 0, errors.New("expected 1 block for the injected batch")
	}
	if len(decodedBlocks[0].Transactions) == 0 {
		return nil, nil, nil, 0, errors.New("expected 1 transaction in the injected batch")
	}

	batchCounters := vm.NewBatchCounterCollector(batchContext.sdb.smt.GetDepth(), uint16(forkId), batchContext.cfg.zk.VirtualCountersSmtReduction, batchContext.cfg.zk.ShouldCountersBeUnlimited(false), nil)

	// process the tx and we can ignore the counters as an overflow at this stage means no network anyway
	effectiveGas := DeriveEffectiveGasPrice(*batchContext.cfg, decodedBlocks[0].Transactions[0])
	receipt, execResult, _, err := attemptAddTransaction(*batchContext.cfg, batchContext.sdb, ibs, batchCounters, blockContext, header, decodedBlocks[0].Transactions[0], effectiveGas, false, forkId, 0 /* use 0 for l1InfoIndex in injected batch */, nil)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	return &decodedBlocks[0].Transactions[0], receipt, execResult, effectiveGas, nil
}
