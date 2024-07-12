package stages

import (
	"context"

	"errors"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	zktypes "github.com/ledgerwatch/erigon/zk/types"
)

const (
	injectedBatchNumber      = 1
	injectedBatchBlockNumber = 1
	injectedBatchBatchNumber = 1
)

func processInjectedInitialBatch(
	ctx context.Context,
	cfg SequenceBlockCfg,
	s *stagedsync.StageState,
	sdb *stageDb,
	forkId uint64,
	header *types.Header,
	parentBlock *types.Block,
	blockContext *evmtypes.BlockContext,
	l1Recovery bool,
) error {
	injected, err := sdb.hermezDb.GetL1InjectedBatch(0)
	if err != nil {
		return err
	}

	fakeL1TreeUpdate := &zktypes.L1InfoTreeUpdate{
		GER:        injected.LastGlobalExitRoot,
		ParentHash: injected.L1ParentHash,
		Timestamp:  injected.Timestamp,
	}

	ibs := state.New(sdb.stateReader)

	// the injected batch block timestamp should also match that of the injected batch
	header.Time = injected.Timestamp

	parentRoot := parentBlock.Root()
	if err = handleStateForNewBlockStarting(
		cfg.chainConfig,
		sdb.hermezDb,
		ibs,
		injectedBatchBlockNumber,
		injectedBatchBatchNumber,
		injected.Timestamp,
		&parentRoot,
		fakeL1TreeUpdate,
		true,
	); err != nil {
		return err
	}

	txn, receipt, execResult, effectiveGas, err := handleInjectedBatch(cfg, sdb, ibs, blockContext, injected, header, parentBlock, forkId)
	if err != nil {
		return err
	}

	txns := types.Transactions{*txn}
	receipts := types.Receipts{receipt}
	execResults := []*core.ExecutionResult{execResult}
	effectiveGases := []uint8{effectiveGas}

	_, err = doFinishBlockAndUpdateState(ctx, cfg, s, sdb, ibs, header, parentBlock, forkId, injectedBatchNumber, injected.LastGlobalExitRoot, injected.L1ParentHash, txns, receipts, execResults, effectiveGases, 0, l1Recovery)
	return err
}

func handleInjectedBatch(
	cfg SequenceBlockCfg,
	sdb *stageDb,
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

	batchCounters := vm.NewBatchCounterCollector(sdb.smt.GetDepth(), uint16(forkId), cfg.zk.VirtualCountersSmtReduction, cfg.zk.ShouldCountersBeUnlimited(false), nil)

	// process the tx and we can ignore the counters as an overflow at this stage means no network anyway
	effectiveGas := DeriveEffectiveGasPrice(cfg, decodedBlocks[0].Transactions[0])
	receipt, execResult, _, err := attemptAddTransaction(cfg, sdb, ibs, batchCounters, blockContext, header, decodedBlocks[0].Transactions[0], effectiveGas, false, forkId, 0 /* use 0 for l1InfoIndex in injected batch */, nil)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	return &decodedBlocks[0].Transactions[0], receipt, execResult, effectiveGas, nil
}
