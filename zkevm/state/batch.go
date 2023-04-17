package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/ledgerwatch/erigon/zkevm/state/metrics"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor/pb"
)

const (
	cTrue  = 1
	cFalse = 0
)

// Batch struct
type Batch struct {
	BatchNumber    uint64
	Coinbase       common.Address
	BatchL2Data    []byte
	StateRoot      common.Hash
	LocalExitRoot  common.Hash
	AccInputHash   common.Hash
	Timestamp      time.Time
	Transactions   []types.Transaction
	GlobalExitRoot common.Hash
	ForcedBatchNum *uint64
}

// ProcessingContext is the necessary data that a batch needs to provide to the runtime,
// without the historical state data (processing receipt from previous batch)
type ProcessingContext struct {
	L1BlockNumber  uint64
	BatchNumber    uint64
	Coinbase       common.Address
	Timestamp      time.Time
	GlobalExitRoot common.Hash
	ForcedBatchNum *uint64
}

// ProcessingReceipt indicates the outcome (StateRoot, AccInputHash) of processing a batch
type ProcessingReceipt struct {
	BatchNumber   uint64
	StateRoot     common.Hash
	LocalExitRoot common.Hash
	AccInputHash  common.Hash
	// Txs           []types.Transaction
	BatchL2Data []byte
}

// VerifiedBatch represents a VerifiedBatch
type VerifiedBatch struct {
	BlockNumber uint64
	BatchNumber uint64
	Aggregator  common.Address
	TxHash      common.Hash
	StateRoot   common.Hash
	IsTrusted   bool
}

// VirtualBatch represents a VirtualBatch
type VirtualBatch struct {
	BatchNumber   uint64
	TxHash        common.Hash
	Coinbase      common.Address
	SequencerAddr common.Address
	BlockNumber   uint64
}

// Sequence represents the sequence interval
type Sequence struct {
	FromBatchNumber uint64
	ToBatchNumber   uint64
}

func (b *Batch) ToJSON() (string, error) {
	data, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (b *Batch) UnmarshalJSON(data []byte) error {
	type batchAux Batch

	var helper struct {
		Transactions []json.RawMessage `json:"Transactions"`
		batchAux
	}

	if err := json.Unmarshal(data, &helper); err != nil {
		return err
	}

	for _, raw := range helper.Transactions {
		var tx types.LegacyTx
		if err := json.Unmarshal(raw, &tx); err != nil {
			return err
		}
		b.Transactions = append(b.Transactions, &tx)
	}

	// Assign the other fields
	b.BatchNumber = helper.batchAux.BatchNumber
	b.Coinbase = helper.batchAux.Coinbase
	b.BatchL2Data = helper.batchAux.BatchL2Data
	b.StateRoot = helper.batchAux.StateRoot
	b.LocalExitRoot = helper.batchAux.LocalExitRoot
	b.AccInputHash = helper.batchAux.AccInputHash
	b.Timestamp = helper.batchAux.Timestamp
	b.GlobalExitRoot = helper.batchAux.GlobalExitRoot
	b.ForcedBatchNum = helper.batchAux.ForcedBatchNum

	return nil
}

func (v *VerifiedBatch) ToJSON() (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (v *VerifiedBatch) FromJSON(b []byte) error {
	return json.Unmarshal(b, v)
}

// OpenBatch adds a new batch into the state, with the necessary data to start processing transactions within it.
// It's meant to be used by sequencers, since they don't necessarily know what transactions are going to be added
// in this batch yet. In other words it's the creation of a WIP batch.
// Note that this will add a batch with batch number N + 1, where N it's the greatest batch number on the state.
func (s *State) OpenBatch(ctx context.Context, processingContext ProcessingContext, dbTx pgx.Tx) error {
	if dbTx == nil {
		return ErrDBTxNil
	}
	// Check if the batch that is being opened has batch num + 1 compared to the latest batch
	lastBatchNum, err := s.PostgresStorage.GetLastBatchNumber(ctx, dbTx)
	if err != nil {
		return err
	}
	if lastBatchNum+1 != processingContext.BatchNumber {
		return fmt.Errorf("%w number %d, should be %d", ErrUnexpectedBatch, processingContext.BatchNumber, lastBatchNum+1)
	}
	// Check if last batch is closed
	isLastBatchClosed, err := s.PostgresStorage.IsBatchClosed(ctx, lastBatchNum, dbTx)
	if err != nil {
		return err
	}
	if !isLastBatchClosed {
		return ErrLastBatchShouldBeClosed
	}
	// Check that timestamp is equal or greater compared to previous batch
	prevTimestamp, err := s.GetLastBatchTime(ctx, dbTx)
	if err != nil {
		return err
	}
	if prevTimestamp.Unix() > processingContext.Timestamp.Unix() {
		return ErrTimestampGE
	}
	return s.PostgresStorage.openBatch(ctx, processingContext, dbTx)
}

// ProcessSequencerBatch is used by the sequencers to process transactions into an open batch
func (s *State) ProcessSequencerBatch(ctx context.Context, batchNumber uint64, batchL2Data []byte, caller metrics.CallerLabel, dbTx pgx.Tx) (*ProcessBatchResponse, error) {
	log.Debugf("*******************************************")
	log.Debugf("ProcessSequencerBatch start")

	processBatchResponse, err := s.processBatch(ctx, batchNumber, batchL2Data, caller, dbTx)
	if err != nil {
		return nil, err
	}

	txs, _, _, err := DecodeTxs(batchL2Data, 5)
	if err != nil && !errors.Is(err, ErrInvalidData) {
		return nil, err
	}
	result, err := s.convertToProcessBatchResponse(txs, processBatchResponse)
	if err != nil {
		return nil, err
	}
	log.Debugf("ProcessSequencerBatch end")
	log.Debugf("*******************************************")
	return result, nil
}

// ProcessBatch processes a batch
func (s *State) ProcessBatch(ctx context.Context, request ProcessRequest, updateMerkleTree bool) (*ProcessBatchResponse, error) {
	log.Debugf("*******************************************")
	log.Debugf("ProcessBatch start")

	updateMT := uint32(cFalse)
	if updateMerkleTree {
		updateMT = cTrue
	}

	forkID := GetForkIDByBatchNumber(s.cfg.ForkIDIntervals, request.BatchNumber)
	// Create Batch
	processBatchRequest := &pb.ProcessBatchRequest{
		OldBatchNum:      request.BatchNumber - 1,
		Coinbase:         request.Coinbase.String(),
		BatchL2Data:      request.Transactions,
		OldStateRoot:     request.OldStateRoot.Bytes(),
		GlobalExitRoot:   request.GlobalExitRoot.Bytes(),
		OldAccInputHash:  request.OldAccInputHash.Bytes(),
		EthTimestamp:     uint64(request.Timestamp.Unix()),
		UpdateMerkleTree: updateMT,
		ChainId:          s.cfg.ChainID,
		ForkId:           forkID,
	}
	res, err := s.sendBatchRequestToExecutor(ctx, processBatchRequest, request.Caller)
	if err != nil {
		return nil, err
	}

	txs, _, _, err := DecodeTxs(request.Transactions, 5)
	if err != nil && !errors.Is(err, ErrInvalidData) {
		return nil, err
	}

	var result *ProcessBatchResponse
	result, err = s.convertToProcessBatchResponse(txs, res)
	if err != nil {
		return nil, err
	}

	log.Debugf("ProcessBatch end")
	log.Debugf("*******************************************")

	return result, nil
}

// ExecuteBatch is used by the synchronizer to reprocess batches to compare generated state root vs stored one
// It is also used by the sequencer in order to calculate used zkCounter of a WIPBatch
func (s *State) ExecuteBatch(ctx context.Context, batch Batch, updateMerkleTree bool, dbTx pgx.Tx) (*pb.ProcessBatchResponse, error) {
	if dbTx == nil {
		return nil, ErrDBTxNil
	}

	// Get previous batch to get state root and local exit root
	previousBatch, err := s.PostgresStorage.GetBatchByNumber(ctx, batch.BatchNumber-1, dbTx)
	if err != nil {
		return nil, err
	}

	forkId := s.GetForkIDByBatchNumber(batch.BatchNumber)

	updateMT := uint32(cFalse)
	if updateMerkleTree {
		updateMT = cTrue
	}

	// Create Batch
	processBatchRequest := &pb.ProcessBatchRequest{
		OldBatchNum:     batch.BatchNumber - 1,
		Coinbase:        batch.Coinbase.String(),
		BatchL2Data:     batch.BatchL2Data,
		OldStateRoot:    previousBatch.StateRoot.Bytes(),
		GlobalExitRoot:  batch.GlobalExitRoot.Bytes(),
		OldAccInputHash: previousBatch.AccInputHash.Bytes(),
		EthTimestamp:    uint64(batch.Timestamp.Unix()),
		// Changed for new sequencer strategy
		UpdateMerkleTree: updateMT,
		ChainId:          s.cfg.ChainID,
		ForkId:           forkId,
	}

	// Send Batch to the Executor
	log.Debugf("ExecuteBatch[processBatchRequest.OldBatchNum]: %v", processBatchRequest.OldBatchNum)
	log.Debugf("ExecuteBatch[processBatchRequest.BatchL2Data]: %v", hex.EncodeToHex(processBatchRequest.BatchL2Data))
	log.Debugf("ExecuteBatch[processBatchRequest.From]: %v", processBatchRequest.From)
	log.Debugf("ExecuteBatch[processBatchRequest.OldStateRoot]: %v", hex.EncodeToHex(processBatchRequest.OldStateRoot))
	log.Debugf("ExecuteBatch[processBatchRequest.GlobalExitRoot]: %v", hex.EncodeToHex(processBatchRequest.GlobalExitRoot))
	log.Debugf("ExecuteBatch[processBatchRequest.OldAccInputHash]: %v", hex.EncodeToHex(processBatchRequest.OldAccInputHash))
	log.Debugf("ExecuteBatch[processBatchRequest.EthTimestamp]: %v", processBatchRequest.EthTimestamp)
	log.Debugf("ExecuteBatch[processBatchRequest.Coinbase]: %v", processBatchRequest.Coinbase)
	log.Debugf("ExecuteBatch[processBatchRequest.UpdateMerkleTree]: %v", processBatchRequest.UpdateMerkleTree)
	log.Debugf("ExecuteBatch[processBatchRequest.ChainId]: %v", processBatchRequest.ChainId)
	log.Debugf("ExecuteBatch[processBatchRequest.ForkId]: %v", processBatchRequest.ForkId)

	processBatchResponse, err := s.executorClient.ProcessBatch(ctx, processBatchRequest)
	if err != nil {
		log.Error("error executing batch: ", err)
		return nil, err
	} else if processBatchResponse != nil && processBatchResponse.Error != executor.EXECUTOR_ERROR_NO_ERROR {
		err = executor.ExecutorErr(processBatchResponse.Error)
		//s.eventLog.LogExecutorError(ctx, processBatchResponse.Error, processBatchRequest)
	}

	return processBatchResponse, err
}

func (s *State) processBatch(ctx context.Context, batchNumber uint64, batchL2Data []byte, caller metrics.CallerLabel, dbTx pgx.Tx) (*pb.ProcessBatchResponse, error) {
	if dbTx == nil {
		return nil, ErrDBTxNil
	}
	if s.executorClient == nil {
		return nil, ErrExecutorNil
	}

	lastBatches, err := s.PostgresStorage.GetLastNBatches(ctx, two, dbTx)
	if err != nil {
		return nil, err
	}

	// Get latest batch from the database to get globalExitRoot and Timestamp
	lastBatch := lastBatches[0]

	// Get batch before latest to get state root and local exit root
	previousBatch := lastBatches[0]
	if len(lastBatches) > 1 {
		previousBatch = lastBatches[1]
	}

	isBatchClosed, err := s.PostgresStorage.IsBatchClosed(ctx, batchNumber, dbTx)
	if err != nil {
		return nil, err
	}
	if isBatchClosed {
		return nil, ErrBatchAlreadyClosed
	}

	// Check provided batch number is the latest in db
	if lastBatch.BatchNumber != batchNumber {
		return nil, ErrInvalidBatchNumber
	}
	forkID := GetForkIDByBatchNumber(s.cfg.ForkIDIntervals, lastBatch.BatchNumber)
	// Create Batch
	processBatchRequest := &pb.ProcessBatchRequest{
		OldBatchNum:      lastBatch.BatchNumber - 1,
		Coinbase:         lastBatch.Coinbase.String(),
		BatchL2Data:      batchL2Data,
		OldStateRoot:     previousBatch.StateRoot.Bytes(),
		GlobalExitRoot:   lastBatch.GlobalExitRoot.Bytes(),
		OldAccInputHash:  previousBatch.AccInputHash.Bytes(),
		EthTimestamp:     uint64(lastBatch.Timestamp.Unix()),
		UpdateMerkleTree: cTrue,
		ChainId:          s.cfg.ChainID,
		ForkId:           forkID,
	}

	res, err := s.sendBatchRequestToExecutor(ctx, processBatchRequest, caller)

	return res, err
}

func (s *State) sendBatchRequestToExecutor(ctx context.Context, processBatchRequest *pb.ProcessBatchRequest, caller metrics.CallerLabel) (*pb.ProcessBatchResponse, error) {
	if s.executorClient == nil {
		return nil, ErrExecutorNil
	}
	// Send Batch to the Executor
	if caller != metrics.DiscardCallerLabel {
		log.Debugf("processBatch[processBatchRequest.OldBatchNum]: %v", processBatchRequest.OldBatchNum)
		log.Debugf("processBatch[processBatchRequest.BatchL2Data]: %v", hex.EncodeToHex(processBatchRequest.BatchL2Data))
		log.Debugf("processBatch[processBatchRequest.From]: %v", processBatchRequest.From)
		log.Debugf("processBatch[processBatchRequest.OldStateRoot]: %v", hex.EncodeToHex(processBatchRequest.OldStateRoot))
		log.Debugf("processBatch[processBatchRequest.GlobalExitRoot]: %v", hex.EncodeToHex(processBatchRequest.GlobalExitRoot))
		log.Debugf("processBatch[processBatchRequest.OldAccInputHash]: %v", hex.EncodeToHex(processBatchRequest.OldAccInputHash))
		log.Debugf("processBatch[processBatchRequest.EthTimestamp]: %v", processBatchRequest.EthTimestamp)
		log.Debugf("processBatch[processBatchRequest.Coinbase]: %v", processBatchRequest.Coinbase)
		log.Debugf("processBatch[processBatchRequest.UpdateMerkleTree]: %v", processBatchRequest.UpdateMerkleTree)
		log.Debugf("processBatch[processBatchRequest.ChainId]: %v", processBatchRequest.ChainId)
		log.Debugf("processBatch[processBatchRequest.ForkId]: %v", processBatchRequest.ForkId)
	}
	now := time.Now()
	res, err := s.executorClient.ProcessBatch(ctx, processBatchRequest)
	if err != nil {
		log.Errorf("Error s.executorClient.ProcessBatch: %v", err)
		log.Errorf("Error s.executorClient.ProcessBatch: %s", err.Error())
		log.Errorf("Error s.executorClient.ProcessBatch response: %v", res)
	} else if res.Error != executor.EXECUTOR_ERROR_NO_ERROR {
		err = executor.ExecutorErr(res.Error)
		//s.eventLog.LogExecutorError(ctx, res.Error, processBatchRequest)
	}
	elapsed := time.Since(now)
	if caller != metrics.DiscardCallerLabel {
		metrics.ExecutorProcessingTime(string(caller), elapsed)
	}
	log.Infof("Batch: %d took %v to be processed by the executor ", processBatchRequest.OldBatchNum+1, elapsed)

	return res, err
}

func (s *State) isBatchClosable(ctx context.Context, receipt ProcessingReceipt, dbTx pgx.Tx) error {
	// Check if the batch that is being closed is the last batch
	lastBatchNum, err := s.PostgresStorage.GetLastBatchNumber(ctx, dbTx)
	if err != nil {
		return err
	}
	if lastBatchNum != receipt.BatchNumber {
		return fmt.Errorf("%w number %d, should be %d", ErrUnexpectedBatch, receipt.BatchNumber, lastBatchNum)
	}
	// Check if last batch is closed
	isLastBatchClosed, err := s.PostgresStorage.IsBatchClosed(ctx, lastBatchNum, dbTx)
	if err != nil {
		return err
	}
	if isLastBatchClosed {
		return ErrBatchAlreadyClosed
	}

	return nil
}

// CloseBatch is used by sequencer to close the current batch
func (s *State) CloseBatch(ctx context.Context, receipt ProcessingReceipt, dbTx pgx.Tx) error {
	if dbTx == nil {
		return ErrDBTxNil
	}

	err := s.isBatchClosable(ctx, receipt, dbTx)
	if err != nil {
		return err
	}

	return s.PostgresStorage.closeBatch(ctx, receipt, dbTx)
}

// ProcessAndStoreClosedBatch is used by the Synchronizer to add a closed batch into the data base
func (s *State) ProcessAndStoreClosedBatch(ctx context.Context, processingCtx ProcessingContext, encodedTxs []byte, dbTx pgx.Tx, caller metrics.CallerLabel) (common.Hash, error) {
	// Decode transactions
	decodedTransactions, _, _, err := DecodeTxs(encodedTxs, 5)
	if err != nil && !errors.Is(err, ErrInvalidData) {
		log.Debugf("error decoding transactions: %v", err)
		return common.Hash{}, err
	}

	// Open the batch and process the txs
	if dbTx == nil {
		return common.Hash{}, ErrDBTxNil
	}
	if err := s.OpenBatch(ctx, processingCtx, dbTx); err != nil {
		return common.Hash{}, err
	}
	processed, err := s.processBatch(ctx, processingCtx.BatchNumber, encodedTxs, caller, dbTx)
	if err != nil {
		return common.Hash{}, err
	}

	// Sanity check
	if len(decodedTransactions) != len(processed.Responses) {
		log.Errorf("number of decoded (%d) and processed (%d) transactions do not match", len(decodedTransactions), len(processed.Responses))
	}

	// Filter unprocessed txs and decode txs to store metadata
	// note that if the batch is not well encoded it will result in an empty batch (with no txs)
	for i := 0; i < len(processed.Responses); i++ {
		if !isProcessed(processed.Responses[i].Error) {
			if executor.IsROMOutOfCountersError(processed.Responses[i].Error) {
				processed.Responses = []*pb.ProcessTransactionResponse{}
				break
			}

			// Remove unprocessed tx
			if i == len(processed.Responses)-1 {
				processed.Responses = processed.Responses[:i]
				decodedTransactions = decodedTransactions[:i]
			} else {
				processed.Responses = append(processed.Responses[:i], processed.Responses[i+1:]...)
				decodedTransactions = append(decodedTransactions[:i], decodedTransactions[i+1:]...)
			}
			i--
		}
	}

	processedBatch, err := s.convertToProcessBatchResponse(decodedTransactions, processed)
	if err != nil {
		return common.Hash{}, err
	}

	if len(processedBatch.Responses) > 0 {
		/* III NOT NEEDED
		// Store processed txs into the batch
		err = s.StoreTransactions(ctx, processingCtx.BatchNumber, processedBatch.Responses, dbTx)
		if err != nil {
			return common.Hash{}, err
		}
		*/
	}

	// Close batch
	return common.BytesToHash(processed.NewStateRoot), s.closeBatch(ctx, ProcessingReceipt{
		BatchNumber:   processingCtx.BatchNumber,
		StateRoot:     processedBatch.NewStateRoot,
		LocalExitRoot: processedBatch.NewLocalExitRoot,
		AccInputHash:  processedBatch.NewAccInputHash,
		BatchL2Data:   encodedTxs,
	}, dbTx)
}

// GetLastBatch gets latest batch (closed or not) on the data base
func (s *State) GetLastBatch(ctx context.Context, dbTx pgx.Tx) (*Batch, error) {
	batches, err := s.PostgresStorage.GetLastNBatches(ctx, 1, dbTx)
	if err != nil {
		return nil, err
	}
	if len(batches) == 0 {
		return nil, ErrNotFound
	}
	return batches[0], nil
}
