package legacy_executor_verifier

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"encoding/hex"
	"errors"
	"fmt"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

var ErrNoExecutorAvailable = fmt.Errorf("no executor available")

type VerifierRequest struct {
	BatchNumber  uint64
	BlockNumbers []uint64
	ForkId       uint64
	StateRoot    common.Hash
	Counters     map[string]int
	creationTime time.Time
	timeout      time.Duration
}

func NewVerifierRequest(forkId, batchNumber uint64, blockNumbers []uint64, stateRoot common.Hash, counters map[string]int) *VerifierRequest {
	return NewVerifierRequestWithTimeout(forkId, batchNumber, blockNumbers, stateRoot, counters, 0)
}

func NewVerifierRequestWithTimeout(forkId, batchNumber uint64, blockNumbers []uint64, stateRoot common.Hash, counters map[string]int, timeout time.Duration) *VerifierRequest {
	return &VerifierRequest{
		BatchNumber:  batchNumber,
		BlockNumbers: blockNumbers,
		ForkId:       forkId,
		StateRoot:    stateRoot,
		Counters:     counters,
		creationTime: time.Now(),
		timeout:      timeout,
	}
}

func (vr *VerifierRequest) IsOverdue() bool {
	if vr.timeout == 0 {
		return false
	}

	return time.Since(vr.creationTime) > vr.timeout
}

func (vr *VerifierRequest) GetFirstBlockNumber() uint64 {
	return vr.BlockNumbers[0]
}

func (vr *VerifierRequest) GetLastBlockNumber() uint64 {
	return vr.BlockNumbers[len(vr.BlockNumbers)-1]
}

type VerifierResponse struct {
	Valid            bool
	Witness          []byte
	ExecutorResponse *executor.ProcessBatchResponseV2
	OriginalCounters map[string]int
	Error            error
}

type VerifierBundle struct {
	Request  *VerifierRequest
	Response *VerifierResponse
}

func NewVerifierBundle(request *VerifierRequest, response *VerifierResponse) *VerifierBundle {
	return &VerifierBundle{
		Request:  request,
		Response: response,
	}
}

type WitnessGenerator interface {
	GetWitnessByBlockRange(tx kv.Tx, ctx context.Context, startBlock, endBlock uint64, debug, witnessFull bool) ([]byte, error)
}

type LegacyExecutorVerifier struct {
	db                     kv.RwDB
	cfg                    ethconfig.Zk
	executors              []*Executor
	executorNumber         int
	cancelAllVerifications atomic.Bool

	streamServer     *server.DataStreamServer
	WitnessGenerator WitnessGenerator

	promises    []*Promise[*VerifierBundle]
	mtxPromises *sync.Mutex
}

func NewLegacyExecutorVerifier(
	cfg ethconfig.Zk,
	executors []*Executor,
	chainCfg *chain.Config,
	db kv.RwDB,
	witnessGenerator WitnessGenerator,
	stream *datastreamer.StreamServer,
) *LegacyExecutorVerifier {
	streamServer := server.NewDataStreamServer(stream, chainCfg.ChainID.Uint64())
	return &LegacyExecutorVerifier{
		db:                     db,
		cfg:                    cfg,
		executors:              executors,
		executorNumber:         0,
		cancelAllVerifications: atomic.Bool{},
		streamServer:           streamServer,
		WitnessGenerator:       witnessGenerator,
		promises:               make([]*Promise[*VerifierBundle], 0),
		mtxPromises:            &sync.Mutex{},
	}
}

func (v *LegacyExecutorVerifier) StartAsyncVerification(
	logPrefix string,
	forkId uint64,
	batchNumber uint64,
	stateRoot common.Hash,
	counters map[string]int,
	blockNumbers []uint64,
	useRemoteExecutor bool,
	requestTimeout time.Duration,
) {
	var promise *Promise[*VerifierBundle]

	request := NewVerifierRequestWithTimeout(forkId, batchNumber, blockNumbers, stateRoot, counters, requestTimeout)
	if useRemoteExecutor {
		promise = v.VerifyAsync(request)
	} else {
		promise = v.VerifyWithoutExecutor(request)
	}

	size := v.appendPromise(promise)
	log.Info(fmt.Sprintf("[%s] Starting verification request", logPrefix), "batch-number", batchNumber, "blocks-range", fmt.Sprintf("[%d;%d]", request.GetFirstBlockNumber(), request.GetLastBlockNumber()), "pending-requests", size)
}

func (v *LegacyExecutorVerifier) appendPromise(promise *Promise[*VerifierBundle]) int {
	v.mtxPromises.Lock()
	defer v.mtxPromises.Unlock()
	v.promises = append(v.promises, promise)
	return len(v.promises)
}

func (v *LegacyExecutorVerifier) VerifySync(tx kv.Tx, request *VerifierRequest, witness, streamBytes []byte, timestampLimit uint64, l1InfoTreeMinTimestamps map[uint64]uint64) error {
	oldAccInputHash := common.HexToHash("0x0")
	payload := &Payload{
		Witness:                 witness,
		DataStream:              streamBytes,
		Coinbase:                v.cfg.AddressSequencer.String(),
		OldAccInputHash:         oldAccInputHash.Bytes(),
		L1InfoRoot:              nil,
		TimestampLimit:          timestampLimit,
		ForcedBlockhashL1:       []byte{0},
		ContextId:               strconv.FormatUint(request.BatchNumber, 10),
		L1InfoTreeMinTimestamps: l1InfoTreeMinTimestamps,
	}

	e := v.GetNextOnlineAvailableExecutor()
	if e == nil {
		return ErrNoExecutorAvailable
	}

	t := utils.StartTimer("legacy-executor-verifier", "verify-sync")
	defer t.LogTimer()

	e.AquireAccess()
	defer e.ReleaseAccess()

	previousBlock, err := rawdb.ReadBlockByNumber(tx, request.GetFirstBlockNumber()-1)
	if err != nil {
		return err
	}

	_, _, executorErr, generalErr := e.Verify(payload, request, previousBlock.Root())
	if generalErr != nil {
		return generalErr
	}
	return executorErr
}

func (v *LegacyExecutorVerifier) VerifyAsync(request *VerifierRequest) *Promise[*VerifierBundle] {
	// eager promise will do the work as soon as called in a goroutine, then we can retrieve the result later
	// ProcessResultsSequentiallyUnsafe relies on the fact that this function returns ALWAYS non-verifierBundle and error. The only exception is the case when verifications has been canceled. Only then the verifierBundle can be nil
	return NewPromise[*VerifierBundle](func() (*VerifierBundle, error) {
		verifierBundle := NewVerifierBundle(request, nil)
		blockNumbers := verifierBundle.Request.BlockNumbers

		e := v.GetNextOnlineAvailableExecutor()
		if e == nil {
			return verifierBundle, ErrNoExecutorAvailable
		}

		t := utils.StartTimer("legacy-executor-verifier", "verify-async")
		defer t.LogTimer()

		e.AquireAccess()
		defer e.ReleaseAccess()
		if v.cancelAllVerifications.Load() {
			return nil, ErrPromiseCancelled
		}

		var err error
		ctx := context.Background()
		// mapmutation has some issue with us not having a quit channel on the context call to `Done` so
		// here we're creating a cancelable context and just deferring the cancel
		innerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		tx, err := v.db.BeginRo(innerCtx)
		if err != nil {
			return verifierBundle, err
		}
		defer tx.Rollback()

		hermezDb := hermez_db.NewHermezDbReader(tx)

		l1InfoTreeMinTimestamps := make(map[uint64]uint64)
		streamBytes, err := v.GetWholeBatchStreamBytes(request.BatchNumber, tx, blockNumbers, hermezDb, l1InfoTreeMinTimestamps, nil)
		if err != nil {
			return verifierBundle, err
		}

		witness, err := v.WitnessGenerator.GetWitnessByBlockRange(tx, innerCtx, blockNumbers[0], blockNumbers[len(blockNumbers)-1], false, v.cfg.WitnessFull)
		if err != nil {
			return verifierBundle, err
		}

		log.Debug("witness generated", "data", hex.EncodeToString(witness))

		// now we need to figure out the timestamp limit for this payload.  It must be:
		// timestampLimit >= currentTimestamp (from batch pre-state) + deltaTimestamp
		// so to ensure we have a good value we can take the timestamp of the last block in the batch
		// and just add 5 minutes
		lastBlock, err := rawdb.ReadBlockByNumber(tx, blockNumbers[len(blockNumbers)-1])
		if err != nil {
			return verifierBundle, err
		}

		// executor is perfectly happy with just an empty hash here
		oldAccInputHash := common.HexToHash("0x0")
		timestampLimit := lastBlock.Time()
		payload := &Payload{
			Witness:                 witness,
			DataStream:              streamBytes,
			Coinbase:                v.cfg.AddressSequencer.String(),
			OldAccInputHash:         oldAccInputHash.Bytes(),
			L1InfoRoot:              nil,
			TimestampLimit:          timestampLimit,
			ForcedBlockhashL1:       []byte{0},
			ContextId:               strconv.FormatUint(request.BatchNumber, 10),
			L1InfoTreeMinTimestamps: l1InfoTreeMinTimestamps,
		}

		previousBlock, err := rawdb.ReadBlockByNumber(tx, blockNumbers[0]-1)
		if err != nil {
			return verifierBundle, err
		}

		ok, executorResponse, executorErr, generalErr := e.Verify(payload, request, previousBlock.Root())
		if generalErr != nil {
			return verifierBundle, generalErr
		}

		if executorErr != nil {
			if errors.Is(executorErr, ErrExecutorStateRootMismatch) {
				log.Error("[Verifier] State root mismatch detected", "err", executorErr)
			} else if errors.Is(executorErr, ErrExecutorUnknownError) {
				log.Error("[Verifier] Unexpected error found from executor", "err", executorErr)
			} else {
				log.Error("[Verifier] Error", "err", executorErr)
			}
		}

		verifierBundle.Response = &VerifierResponse{
			Valid:            ok,
			Witness:          witness,
			ExecutorResponse: executorResponse,
			Error:            executorErr,
		}
		return verifierBundle, nil
	})
}

func (v *LegacyExecutorVerifier) VerifyWithoutExecutor(request *VerifierRequest) *Promise[*VerifierBundle] {
	promise := NewPromise[*VerifierBundle](func() (*VerifierBundle, error) {
		response := &VerifierResponse{
			Valid:            true,
			OriginalCounters: request.Counters,
			Witness:          nil,
			ExecutorResponse: nil,
			Error:            nil,
		}
		return NewVerifierBundle(request, response), nil
	})
	promise.Wait()

	return promise
}

func (v *LegacyExecutorVerifier) ProcessResultsSequentially(logPrefix string) ([]*VerifierBundle, error) {
	v.mtxPromises.Lock()
	defer v.mtxPromises.Unlock()

	var verifierResponse []*VerifierBundle

	// not a stop signal, so we can start to process our promises now
	for idx, promise := range v.promises {
		verifierBundle, err := promise.TryGet()
		if verifierBundle == nil && err == nil {
			// If code enters here this means that this promise is not yet completed
			// We must processes responses sequentially so if this one is not ready we can just break
			break
		}

		if err != nil {
			// let leave it for debug purposes
			// a cancelled promise is removed from v.promises => it should never appear here, that's why let's panic if it happens, because it will indicate for massive error
			if errors.Is(err, ErrPromiseCancelled) {
				panic("this should never happen")
			}

			log.Error("error on our end while preparing the verification request, re-queueing the task", "err", err)

			if verifierBundle.Request.IsOverdue() {
				// signal an error, the caller can check on this and stop the process if needs be
				return nil, fmt.Errorf("error: batch %d couldn't be processed in 30 minutes", verifierBundle.Request.BatchNumber)
			}

			// re-queue the task - it should be safe to replace the index of the slice here as we only add to it
			v.promises[idx] = promise.CloneAndRerun()

			// break now as we know we can't proceed here until this promise is attempted again
			break
		}

		log.Info(fmt.Sprintf("[%s] Finished verification request", logPrefix), "batch-number", verifierBundle.Request.BatchNumber, "blocks-range", fmt.Sprintf("[%d;%d]", verifierBundle.Request.GetFirstBlockNumber(), verifierBundle.Request.GetLastBlockNumber()), "is-valid", verifierBundle.Response.Valid, "pending-requests", len(v.promises)-1-idx)
		verifierResponse = append(verifierResponse, verifierBundle)
	}

	// remove processed promises from the list
	v.promises = v.promises[len(verifierResponse):]

	return verifierResponse, nil
}

func (v *LegacyExecutorVerifier) Wait() {
	for _, p := range v.promises {
		p.Wait()
	}
}

func (v *LegacyExecutorVerifier) CancelAllRequests() {
	// cancel all promises
	// all queued promises will return ErrPromiseCancelled while getting its result
	for _, p := range v.promises {
		p.Cancel()
	}

	// the goal of this car is to ensure that running promises are stopped as soon as possible
	// we need it because the promise's function must finish and then the promise checks if it has been cancelled
	v.cancelAllVerifications.Store(true)

	for _, e := range v.executors {
		// let's wait for all threads that are waiting to add to v.openRequests to finish
		for e.QueueLength() > 0 {
			time.Sleep(1 * time.Millisecond)
		}
	}

	v.cancelAllVerifications.Store(false)

	v.promises = make([]*Promise[*VerifierBundle], 0)
}

func (v *LegacyExecutorVerifier) GetNextOnlineAvailableExecutor() *Executor {
	var exec *Executor

	// TODO: find executors with spare capacity

	// attempt to find an executor that is online amongst them all
	for i := 0; i < len(v.executors); i++ {
		v.executorNumber++
		if v.executorNumber >= len(v.executors) {
			v.executorNumber = 0
		}
		temp := v.executors[v.executorNumber]
		if temp.CheckOnline() {
			exec = temp
			break
		}
	}

	return exec
}

func (v *LegacyExecutorVerifier) GetWholeBatchStreamBytes(
	batchNumber uint64,
	tx kv.Tx,
	blockNumbers []uint64,
	hermezDb *hermez_db.HermezDbReader,
	l1InfoTreeMinTimestamps map[uint64]uint64,
	transactionsToIncludeByIndex [][]int, // passing nil here will include all transactions in the blocks
) (streamBytes []byte, err error) {
	blocks := make([]types.Block, 0, len(blockNumbers))
	txsPerBlock := make(map[uint64][]types.Transaction)

	// as we only ever use the executor verifier for whole batches we can safely assume that the previous batch
	// will always be the request batch - 1 and that the first block in the batch will be at the batch
	// boundary so we will always add in the batch bookmark to the stream
	previousBatch := batchNumber - 1

	for idx, blockNumber := range blockNumbers {
		block, err := rawdb.ReadBlockByNumber(tx, blockNumber)

		if err != nil {
			return nil, err
		}
		blocks = append(blocks, *block)

		filteredTransactions := block.Transactions()
		// filter transactions by indexes that should be included
		if transactionsToIncludeByIndex != nil {
			filteredTransactions = filterTransactionByIndexes(block.Transactions(), transactionsToIncludeByIndex[idx])
		}

		txsPerBlock[blockNumber] = filteredTransactions
	}

	entries, err := server.BuildWholeBatchStreamEntriesProto(tx, hermezDb, v.streamServer.GetChainId(), batchNumber, previousBatch, blocks, txsPerBlock, l1InfoTreeMinTimestamps)
	if err != nil {
		return nil, err
	}

	return entries.Marshal()
}

func filterTransactionByIndexes(
	filteredTransactions types.Transactions,
	transactionsToIncludeByIndex []int,
) types.Transactions {
	if transactionsToIncludeByIndex != nil {
		filteredTransactionsBuilder := make(types.Transactions, len(transactionsToIncludeByIndex))
		for i, txIndexInBlock := range transactionsToIncludeByIndex {
			filteredTransactionsBuilder[i] = filteredTransactions[txIndexInBlock]
		}

		filteredTransactions = filteredTransactionsBuilder
	}

	return filteredTransactions
}
