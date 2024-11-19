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
	dsTypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/l1_data"
	zktx "github.com/ledgerwatch/erigon/zk/tx"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/log/v3"
)

const maximumOverflowTransactionAttempts = 5

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
	overflowTransactions          int
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

		if batchState.isMoreThanSingleRecovery() {
			panic(fmt.Errorf("only single recovery could be active at a time, L1Recovery: %t, limboRecovery: %t, ResequenceRecovery: %t", batchState.isL1Recovery(), batchState.isLimboRecovery(), batchState.isResequence()))
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

func (bs *BatchState) isMoreThanSingleRecovery() bool {
	recoveryCounter := 0

	if bs.isL1Recovery() {
		recoveryCounter++
	}

	if bs.isLimboRecovery() {
		recoveryCounter++
	}

	if bs.isResequence() {
		recoveryCounter++
	}

	return recoveryCounter > 1
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
	if bs.batchNumber > 1 && bs.isL1Recovery() {
		return bs.batchL1RecoveryData.recoveredBatchData.Coinbase
	}

	return cfg.zk.AddressSequencer
}

func (bs *BatchState) onAddedTransaction(transaction types.Transaction, receipt *types.Receipt, execResult *core.ExecutionResult, effectiveGas uint8) {
	slotId, ok := bs.blockState.transactionHashesToSlots[transaction.Hash()]
	if !ok {
		log.Warn("[batchState] transaction hash not found in transaction hashes to slots map", "hash", transaction.Hash())
	}
	bs.blockState.builtBlockElements.onFinishAddingTransaction(transaction, receipt, execResult, effectiveGas, slotId)
	bs.hasAnyTransactionsInThisBatch = true
}

func (bs *BatchState) onBuiltBlock(blockNumber uint64) {
	bs.builtBlocks = append(bs.builtBlocks, blockNumber)
}

func (bs *BatchState) newOverflowTransaction() {
	bs.overflowTransactions++
}

func (bs *BatchState) reachedOverflowTransactionLimit() bool {
	return bs.overflowTransactions >= maximumOverflowTransactionAttempts
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
	transactionHashesToSlots map[common.Hash]common.Hash
	builtBlockElements       BuiltBlockElements
	blockL1RecoveryData      *zktx.DecodedBatchL2Data
}

func newBlockState() *BlockState {
	return &BlockState{
		transactionHashesToSlots: make(map[common.Hash]common.Hash),
	}
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
	txSlots          []common.Hash
}

func (bbe *BuiltBlockElements) resetBlockBuildingArrays() {
	bbe.transactions = []types.Transaction{}
	bbe.receipts = types.Receipts{}
	bbe.effectiveGases = []uint8{}
	bbe.executionResults = []*core.ExecutionResult{}
}

func (bbe *BuiltBlockElements) onFinishAddingTransaction(transaction types.Transaction, receipt *types.Receipt, execResult *core.ExecutionResult, effectiveGas uint8, slotId common.Hash) {
	bbe.transactions = append(bbe.transactions, transaction)
	bbe.receipts = append(bbe.receipts, receipt)
	bbe.executionResults = append(bbe.executionResults, execResult)
	bbe.effectiveGases = append(bbe.effectiveGases, effectiveGas)
	bbe.txSlots = append(bbe.txSlots, slotId)
}

type resequenceTxMetadata struct {
	blockNum int
	txIndex  int
}

type ResequenceBatchJob struct {
	batchToProcess  []*dsTypes.FullL2Block
	StartBlockIndex int
	StartTxIndex    int
	txIndexMap      map[common.Hash]resequenceTxMetadata
}

func NewResequenceBatchJob(batch []*dsTypes.FullL2Block) *ResequenceBatchJob {
	return &ResequenceBatchJob{
		batchToProcess:  batch,
		StartBlockIndex: 0,
		StartTxIndex:    0,
		txIndexMap:      make(map[common.Hash]resequenceTxMetadata),
	}
}

func (r *ResequenceBatchJob) HasMoreBlockToProcess() bool {
	return r.StartBlockIndex < len(r.batchToProcess)
}

func (r *ResequenceBatchJob) AtNewBlockBoundary() bool {
	return r.StartTxIndex == 0
}

func (r *ResequenceBatchJob) CurrentBlock() *dsTypes.FullL2Block {
	if r.HasMoreBlockToProcess() {
		return r.batchToProcess[r.StartBlockIndex]
	}
	return nil
}

func (r *ResequenceBatchJob) YieldNextBlockTransactions(decoder zktx.TxDecoder) ([]types.Transaction, error) {
	blockTransactions := make([]types.Transaction, 0)
	if r.HasMoreBlockToProcess() {
		block := r.CurrentBlock()
		r.txIndexMap[block.L2Blockhash] = resequenceTxMetadata{r.StartBlockIndex, 0}

		for i := r.StartTxIndex; i < len(block.L2Txs); i++ {
			transaction := block.L2Txs[i]
			tx, _, err := decoder(transaction.Encoded, transaction.EffectiveGasPricePercentage, block.ForkId)
			if err != nil {
				return nil, fmt.Errorf("decode tx error: %v", err)
			}
			r.txIndexMap[tx.Hash()] = resequenceTxMetadata{r.StartBlockIndex, i}
			blockTransactions = append(blockTransactions, tx)
		}
	}

	return blockTransactions, nil
}

func (r *ResequenceBatchJob) UpdateLastProcessedTx(h common.Hash) {
	if idx, ok := r.txIndexMap[h]; ok {
		block := r.batchToProcess[idx.blockNum]

		if idx.txIndex >= len(block.L2Txs)-1 {
			// we've processed all the transactions in this block
			// move to the next block
			r.StartBlockIndex = idx.blockNum + 1
			r.StartTxIndex = 0
		} else {
			// move to the next transaction in the block
			r.StartBlockIndex = idx.blockNum
			r.StartTxIndex = idx.txIndex + 1
		}
	} else {
		log.Warn("tx hash not found in tx index map", "hash", h)
	}
}
