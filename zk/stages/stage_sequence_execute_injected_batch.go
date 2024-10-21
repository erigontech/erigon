package stages

import (
	"encoding/json"
	"fmt"
	"math"
	"os"

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
	"github.com/ledgerwatch/log/v3"
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

	var (
		injectedBatch *zktypes.L1InjectedBatch
		err           error
	)

	if batchContext.cfg.zk.ShouldImportInitialBatch() {
		log.Debug(fmt.Sprintf("Initial batch is provided in the file '%s'", batchContext.cfg.zk.InitialBatchCfgFile))
		// import injected batch from file
		importResult, err := loadInjectedBatchDataFromFile(batchContext.cfg.zk.InitialBatchCfgFile)
		if err != nil {
			return err
		}

		// injected batch transactions are already baked in the genesis file
		// (no need to do anything at this point)
		if importResult.isPartOfGenesis {
			return nil
		}

		injectedBatch = importResult.injectedBatch
	} else {
		// retrieve injected batch from the database
		injectedBatch, err = batchContext.sdb.hermezDb.GetL1InjectedBatch(0)
		if err != nil {
			return err
		}
	}

	header, parentBlock, err := prepareHeader(batchContext.sdb.tx, 0, math.MaxUint64, math.MaxUint64,
		batchState.forkId, batchContext.cfg.zk.AddressSequencer, batchContext.cfg.chainConfig, batchContext.cfg.miningConfig)
	if err != nil {
		return err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(batchContext.sdb.tx, hash, number)
	}
	getHashFn := core.GetHashFn(header, getHeader)
	blockContext := core.NewEVMBlockContext(header, getHashFn, batchContext.cfg.engine, &batchContext.cfg.zk.AddressSequencer)

	fakeL1TreeUpdate := &zktypes.L1InfoTreeUpdate{
		GER:        injectedBatch.LastGlobalExitRoot,
		ParentHash: injectedBatch.L1ParentHash,
		Timestamp:  injectedBatch.Timestamp,
	}

	ibs := state.New(batchContext.sdb.stateReader)

	// the injected batch block timestamp should also match that of the injected batch
	header.Time = injectedBatch.Timestamp

	parentRoot := parentBlock.Root()
	if err = handleStateForNewBlockStarting(batchContext, ibs, injectedBatchBlockNumber,
		injectedBatchBatchNumber, injectedBatch.Timestamp, &parentRoot, fakeL1TreeUpdate, true); err != nil {
		return err
	}

	txn, receipt, execResult, effectiveGas, err := handleInjectedBatch(batchContext, ibs, &blockContext,
		injectedBatch, header, parentBlock, batchState.forkId)
	if err != nil {
		return err
	}

	batchState.blockState.builtBlockElements = BuiltBlockElements{
		transactions:     types.Transactions{*txn},
		receipts:         types.Receipts{receipt},
		executionResults: []*core.ExecutionResult{execResult},
		effectiveGases:   []uint8{effectiveGas},
	}
	batchCounters := vm.NewBatchCounterCollector(batchContext.sdb.smt.GetDepth(), uint16(batchState.forkId), batchContext.cfg.zk.VirtualCountersSmtReduction,
		batchContext.cfg.zk.ShouldCountersBeUnlimited(batchState.isL1Recovery()), nil)

	if _, err = doFinishBlockAndUpdateState(batchContext, ibs, header, parentBlock, batchState, injectedBatch.LastGlobalExitRoot, injectedBatch.L1ParentHash, 0, 0, batchCounters); err != nil {
		return err
	}

	return nil
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

type injectedBatchImportResult struct {
	// injectedBatch is unmarshaled injected batch definition from the provided JSON file
	injectedBatch *zktypes.L1InjectedBatch
	// isPartOfGenesis indicates that the injected batch is already part of the genesis spec
	isPartOfGenesis bool
}

// loadInjectedBatchDataFromFile loads data from a file, unmarshals it, and converts it to L1InjectedBatch
func loadInjectedBatchDataFromFile(fileName string) (*injectedBatchImportResult, error) {
	// Check if the file exists
	fileInfo, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("file %s does not exist", fileName)
	}

	if fileInfo.IsDir() {
		return nil, fmt.Errorf("%s is a directory, not a file", fileName)
	}

	rawBytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %v", fileName, err)
	}

	if len(rawBytes) == 0 {
		return &injectedBatchImportResult{isPartOfGenesis: true}, nil
	}

	// Unmarshal the JSON into L1InjectedBatch
	var injectedBatch zktypes.L1InjectedBatch
	err = json.Unmarshal(rawBytes, &injectedBatch)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON from file %s: %v", fileName, err)
	}

	log.Debug(fmt.Sprintf("Initializing with first batch data...\n%s", string(rawBytes)))

	return &injectedBatchImportResult{injectedBatch: &injectedBatch}, nil
}
