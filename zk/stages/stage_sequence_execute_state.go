package stages

import (
	"context"
	"fmt"
	"math"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/zk/l1_data"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/txpool"
)

type BatchContext struct {
	ctx        context.Context
	cfg        *SequenceBlockCfg
	historyCfg *stagedsync.HistoryCfg
	s          *stagedsync.StageState
	sdb        *stageDb
}

func newBatchContext(ctx context.Context, cfg *SequenceBlockCfg, historyCfg *stagedsync.HistoryCfg, s *stagedsync.StageState, sdb *stageDb) *BatchContext {
	return &BatchContext{
		ctx:        ctx,
		cfg:        cfg,
		historyCfg: historyCfg,
		s:          s,
		sdb:        sdb,
	}
}

// TYPE BATCH STATE
type BatchState struct {
	forkId                        uint64
	batchNumber                   uint64
	hasExecutorForThisBatch       bool
	hasAnyTransactionsInThisBatch bool
	builtBlocks                   []uint64
	yieldedTransactions           mapset.Set[[32]byte]
	blockState                    *BlockState
	batchL1RecoveryData           *BatchL1RecoveryData
	limboRecoveryData             *LimboRecoveryData
	resequenceBatchJob            *ResequenceBatchJob
}

func newBatchState(forkId, batchNumber, blockNumber uint64, hasExecutorForThisBatch, l1Recovery bool, txPool *txpool.TxPool, resequenceBatchJob *ResequenceBatchJob) *BatchState {
	batchState := &BatchState{
		forkId:                        forkId,
		batchNumber:                   batchNumber,
		hasExecutorForThisBatch:       hasExecutorForThisBatch,
		hasAnyTransactionsInThisBatch: false,
		builtBlocks:                   make([]uint64, 0, 128),
		yieldedTransactions:           mapset.NewSet[[32]byte](),
		blockState:                    newBlockState(),
		batchL1RecoveryData:           nil,
		limboRecoveryData:             nil,
		resequenceBatchJob:            resequenceBatchJob,
	}

	if batchNumber != injectedBatchBatchNumber { // process injected batch regularly, no matter if it is in any recovery
		if l1Recovery {
			batchState.batchL1RecoveryData = newBatchL1RecoveryData(batchState)
		}

		limboBlock, limboTxHash := txPool.GetLimboDetailsForRecovery(blockNumber)
		if limboTxHash != nil {
			// batchNumber == limboBlock.BatchNumber then we've unwound to the very beginning of the batch. 'limboBlock.BlockNumber' is the 1st block of 'batchNumber' batch. Everything is fine.

			// batchNumber - 1 == limboBlock.BatchNumber then we've unwound to the middle of a batch. We must set in 'batchState' that we're going to resume a batch build rather than starting a new one. Everything is fine.
			if batchNumber-1 == limboBlock.BatchNumber {
				batchState.batchNumber = limboBlock.BatchNumber
			} else if batchNumber != limboBlock.BatchNumber {
				// in any other configuration rather than (batchNumber or batchNumber - 1) == limboBlock.BatchNumber we can only panic
				panic(fmt.Errorf("requested batch %d while the network is already on %d", limboBlock.BatchNumber, batchNumber))
			}

			batchState.limboRecoveryData = newLimboRecoveryData(limboBlock.BlockTimestamp, limboTxHash)
		}

		if batchState.isL1Recovery() && batchState.isLimboRecovery() {
			panic("Both recoveries cannot be active simultaneously")
		}
	}

	return batchState
}

func (bs *BatchState) isL1Recovery() bool {
	return bs.batchL1RecoveryData != nil
}

func (bs *BatchState) isLimboRecovery() bool {
	return bs.limboRecoveryData != nil
}

func (bs *BatchState) isResequence() bool {
	return bs.resequenceBatchJob != nil
}

func (bs *BatchState) isAnyRecovery() bool {
	return bs.isL1Recovery() || bs.isLimboRecovery() || bs.isResequence()
}

func (bs *BatchState) isThereAnyTransactionsToRecover() bool {
	if !bs.isL1Recovery() {
		return false
	}

	return bs.blockState.hasAnyTransactionForInclusion() || bs.batchL1RecoveryData.recoveredBatchData.IsWorkRemaining
}

func (bs *BatchState) loadBlockL1RecoveryData(decodedBlocksIndex uint64) bool {
	decodedBatchL2Data, found := bs.batchL1RecoveryData.getDecodedL1RecoveredBatchDataByIndex(decodedBlocksIndex)
	bs.blockState.setBlockL1RecoveryData(decodedBatchL2Data)
	return found
}

// if not limbo set the limboHeaderTimestamp to the "default" value for "prepareHeader" function
func (bs *BatchState) getBlockHeaderForcedTimestamp() uint64 {
	if bs.isLimboRecovery() {
		return bs.limboRecoveryData.limboHeaderTimestamp
	}

	if bs.isResequence() {
		return uint64(bs.resequenceBatchJob.CurrentBlock().Timestamp)
	}

	return math.MaxUint64
}

func (bs *BatchState) getCoinbase(cfg *SequenceBlockCfg) common.Address {
	if bs.isL1Recovery() {
		return bs.batchL1RecoveryData.recoveredBatchData.Coinbase
	}

	return cfg.zk.AddressSequencer
}

func (bs *BatchState) onAddedTransaction(transaction types.Transaction, receipt *types.Receipt, execResult *core.ExecutionResult, effectiveGas uint8) {
	bs.blockState.builtBlockElements.onFinishAddingTransaction(transaction, receipt, execResult, effectiveGas)
	bs.hasAnyTransactionsInThisBatch = true
}

func (bs *BatchState) onBuiltBlock(blockNumber uint64) {
	bs.builtBlocks = append(bs.builtBlocks, blockNumber)
}

// TYPE BATCH L1 RECOVERY DATA
type BatchL1RecoveryData struct {
	recoveredBatchDataSize int
	recoveredBatchData     *l1_data.DecodedL1Data
	batchState             *BatchState
}

func newBatchL1RecoveryData(batchState *BatchState) *BatchL1RecoveryData {
	return &BatchL1RecoveryData{
		batchState: batchState,
	}
}

func (batchL1RecoveryData *BatchL1RecoveryData) loadBatchData(sdb *stageDb) (err error) {
	batchL1RecoveryData.recoveredBatchData, err = l1_data.BreakDownL1DataByBatch(batchL1RecoveryData.batchState.batchNumber, batchL1RecoveryData.batchState.forkId, sdb.hermezDb.HermezDbReader)
	if err != nil {
		return err
	}

	batchL1RecoveryData.recoveredBatchDataSize = len(batchL1RecoveryData.recoveredBatchData.DecodedData)
	return nil
}

func (batchL1RecoveryData *BatchL1RecoveryData) hasAnyDecodedBlocks() bool {
	return batchL1RecoveryData.recoveredBatchDataSize > 0
}

func (batchL1RecoveryData *BatchL1RecoveryData) getInfoTreeIndex(sdb *stageDb) (uint64, error) {
	var infoTreeIndex uint64

	if batchL1RecoveryData.recoveredBatchData.L1InfoRoot == SpecialZeroIndexHash {
		return uint64(0), nil
	}

	infoTreeIndex, found, err := sdb.hermezDb.GetL1InfoTreeIndexByRoot(batchL1RecoveryData.recoveredBatchData.L1InfoRoot)
	if err != nil {
		return uint64(0), err
	}
	if !found {
		return uint64(0), fmt.Errorf("could not find L1 info tree index for root %s", batchL1RecoveryData.recoveredBatchData.L1InfoRoot.String())
	}

	return infoTreeIndex, nil
}

func (batchL1RecoveryData *BatchL1RecoveryData) getDecodedL1RecoveredBatchDataByIndex(decodedBlocksIndex uint64) (*zktx.DecodedBatchL2Data, bool) {
	if decodedBlocksIndex == uint64(batchL1RecoveryData.recoveredBatchDataSize) {
		return nil, false
	}

	return &batchL1RecoveryData.recoveredBatchData.DecodedData[decodedBlocksIndex], true
}

// TYPE LIMBO RECOVERY DATA
type LimboRecoveryData struct {
	limboHeaderTimestamp uint64
	limboTxHash          *common.Hash
}

func newLimboRecoveryData(limboHeaderTimestamp uint64, limboTxHash *common.Hash) *LimboRecoveryData {
	return &LimboRecoveryData{
		limboHeaderTimestamp: limboHeaderTimestamp,
		limboTxHash:          limboTxHash,
	}
}

// TYPE BLOCK STATE
type BlockState struct {
	transactionsForInclusion []types.Transaction
	builtBlockElements       BuiltBlockElements
	blockL1RecoveryData      *zktx.DecodedBatchL2Data
}

func newBlockState() *BlockState {
	return &BlockState{}
}

func (bs *BlockState) hasAnyTransactionForInclusion() bool {
	return len(bs.transactionsForInclusion) > 0
}

func (bs *BlockState) setBlockL1RecoveryData(blockL1RecoveryData *zktx.DecodedBatchL2Data) {
	bs.blockL1RecoveryData = blockL1RecoveryData

	if bs.blockL1RecoveryData != nil {
		bs.transactionsForInclusion = bs.blockL1RecoveryData.Transactions
	} else {
		bs.transactionsForInclusion = []types.Transaction{}
	}
}

func (bs *BlockState) getDeltaTimestamp() uint64 {
	if bs.blockL1RecoveryData != nil {
		return uint64(bs.blockL1RecoveryData.DeltaTimestamp)
	}

	return math.MaxUint64
}

func (bs *BlockState) getL1EffectiveGases(cfg SequenceBlockCfg, i int) uint8 {
	if bs.blockL1RecoveryData != nil {
		return bs.blockL1RecoveryData.EffectiveGasPricePercentages[i]
	}

	return DeriveEffectiveGasPrice(cfg, bs.transactionsForInclusion[i])
}

// TYPE BLOCK ELEMENTS
type BuiltBlockElements struct {
	transactions     []types.Transaction
	receipts         types.Receipts
	effectiveGases   []uint8
	executionResults []*core.ExecutionResult
}

func (bbe *BuiltBlockElements) resetBlockBuildingArrays() {
	bbe.transactions = []types.Transaction{}
	bbe.receipts = types.Receipts{}
	bbe.effectiveGases = []uint8{}
	bbe.executionResults = []*core.ExecutionResult{}
}

func (bbe *BuiltBlockElements) onFinishAddingTransaction(transaction types.Transaction, receipt *types.Receipt, execResult *core.ExecutionResult, effectiveGas uint8) {
	bbe.transactions = append(bbe.transactions, transaction)
	bbe.receipts = append(bbe.receipts, receipt)
	bbe.executionResults = append(bbe.executionResults, execResult)
	bbe.effectiveGases = append(bbe.effectiveGases, effectiveGas)
}
