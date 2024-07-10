package legacy_executor_verifier

import (
	"context"
	"sync/atomic"
	"time"

	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/log/v3"
	"sync"
)

var ErrNoExecutorAvailable = fmt.Errorf("no executor available")

type VerifierRequest struct {
	BatchNumber  uint64
	ForkId       uint64
	StateRoot    common.Hash
	Counters     map[string]int
	creationTime time.Time
}

func NewVerifierRequest(batchNumber, forkId uint64, stateRoot common.Hash, counters map[string]int) *VerifierRequest {
	return &VerifierRequest{
		BatchNumber:  batchNumber,
		ForkId:       forkId,
		StateRoot:    stateRoot,
		Counters:     counters,
		creationTime: time.Now(),
	}
}

func (vr *VerifierRequest) isOverdue() bool {
	return time.Since(vr.creationTime) > time.Duration(30*time.Minute)
}

type VerifierResponse struct {
	BatchNumber      uint64
	Valid            bool
	Witness          []byte
	ExecutorResponse *executor.ProcessBatchResponseV2
	Error            error
}

type VerifierBundle struct {
	request  *VerifierRequest
	response *VerifierResponse
}

func NewVerifierBundle(request *VerifierRequest, response *VerifierResponse) *VerifierBundle {
	return &VerifierBundle{
		request:  request,
		response: response,
	}
}

type ILegacyExecutor interface {
	Verify(*Payload, *VerifierRequest, common.Hash) (bool, *executor.ProcessBatchResponseV2, error)
	CheckOnline() bool
	QueueLength() int
	AquireAccess()
	ReleaseAccess()
}

type WitnessGenerator interface {
	GetWitnessByBlockRange(tx kv.Tx, ctx context.Context, startBlock, endBlock uint64, debug, witnessFull bool) ([]byte, error)
}

type LegacyExecutorVerifier struct {
	db                     kv.RwDB
	cfg                    ethconfig.Zk
	executors              []ILegacyExecutor
	executorNumber         int
	cancelAllVerifications atomic.Bool

	quit chan struct{}

	streamServer     *server.DataStreamServer
	stream           *datastreamer.StreamServer
	witnessGenerator WitnessGenerator
	l1Syncer         *syncer.L1Syncer

	promises     []*Promise[*VerifierBundle]
	addedBatches map[uint64]struct{}

	// these three items are used to keep track of where the datastream is at
	// compared with the executor checks.  It allows for data to arrive in strange
	// orders and will backfill the stream as needed.
	lowestWrittenBatch uint64
	responsesToWrite   map[uint64]struct{}
	responsesMtx       *sync.Mutex
}

func NewLegacyExecutorVerifier(
	cfg ethconfig.Zk,
	executors []ILegacyExecutor,
	chainCfg *chain.Config,
	db kv.RwDB,
	witnessGenerator WitnessGenerator,
	l1Syncer *syncer.L1Syncer,
	stream *datastreamer.StreamServer,
) *LegacyExecutorVerifier {
	streamServer := server.NewDataStreamServer(stream, chainCfg.ChainID.Uint64())
	return &LegacyExecutorVerifier{
		db:                     db,
		cfg:                    cfg,
		executors:              executors,
		executorNumber:         0,
		cancelAllVerifications: atomic.Bool{},
		quit:                   make(chan struct{}),
		streamServer:           streamServer,
		stream:                 stream,
		witnessGenerator:       witnessGenerator,
		l1Syncer:               l1Syncer,
		promises:               make([]*Promise[*VerifierBundle], 0),
		addedBatches:           make(map[uint64]struct{}),
		responsesToWrite:       map[uint64]struct{}{},
		responsesMtx:           &sync.Mutex{},
	}
}

func (v *LegacyExecutorVerifier) VerifySync(tx kv.Tx, request *VerifierRequest, witness, streamBytes []byte, timestampLimit, firstBlockNumber uint64, l1InfoTreeMinTimestamps map[uint64]uint64) error {
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

	e := v.getNextOnlineAvailableExecutor()
	if e == nil {
		return ErrNoExecutorAvailable
	}

	e.AquireAccess()
	defer e.ReleaseAccess()

	previousBlock, err := rawdb.ReadBlockByNumber(tx, firstBlockNumber-1)
	if err != nil {
		return err
	}

	_, _, executorErr := e.Verify(payload, request, previousBlock.Root())
	return executorErr
}

// Unsafe is not thread-safe so it MUST be invoked only from a single thread
func (v *LegacyExecutorVerifier) AddRequestUnsafe(request *VerifierRequest, sequencerBatchSealTime time.Duration) *Promise[*VerifierBundle] {
	// eager promise will do the work as soon as called in a goroutine, then we can retrieve the result later
	// ProcessResultsSequentiallyUnsafe relies on the fact that this function returns ALWAYS non-verifierBundle and error. The only exception is the case when verifications has been canceled. Only then the verifierBundle can be nil
	promise := NewPromise[*VerifierBundle](func() (*VerifierBundle, error) {
		verifierBundle := NewVerifierBundle(request, nil)

		e := v.getNextOnlineAvailableExecutor()
		if e == nil {
			return verifierBundle, ErrNoExecutorAvailable
		}

		e.AquireAccess()
		defer e.ReleaseAccess()
		if v.cancelAllVerifications.Load() {
			return nil, ErrPromiseCancelled
		}

		var err error
		var blocks []uint64
		startTime := time.Now()
		ctx := context.Background()
		// mapmutation has some issue with us not having a quit channel on the context call to `Done` so
		// here we're creating a cancelable context and just deferring the cancel
		innerCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// get the data stream bytes
		for time.Since(startTime) < 3*sequencerBatchSealTime {
			// we might not have blocks yet as the underlying stage loop might still be running and the tx hasn't been
			// committed yet so just requeue the request
			blocks, err = v.availableBlocksToProcess(innerCtx, request.BatchNumber)
			if err != nil {
				return verifierBundle, err
			}

			if len(blocks) > 0 {
				break
			}

			time.Sleep(time.Second)
		}

		if len(blocks) == 0 {
			return verifierBundle, fmt.Errorf("still not blocks in this batch")
		}

		tx, err := v.db.BeginRo(innerCtx)
		if err != nil {
			return verifierBundle, err
		}
		defer tx.Rollback()

		hermezDb := hermez_db.NewHermezDbReader(tx)

		l1InfoTreeMinTimestamps := make(map[uint64]uint64)
		streamBytes, err := v.GetStreamBytes(request.BatchNumber, tx, blocks, hermezDb, l1InfoTreeMinTimestamps, nil)
		if err != nil {
			return verifierBundle, err
		}

		witness, err := v.witnessGenerator.GetWitnessByBlockRange(tx, ctx, blocks[0], blocks[len(blocks)-1], false, v.cfg.WitnessFull)
		if err != nil {
			return nil, err
		}

		log.Debug("witness generated", "data", hex.EncodeToString(witness))

		// now we need to figure out the timestamp limit for this payload.  It must be:
		// timestampLimit >= currentTimestamp (from batch pre-state) + deltaTimestamp
		// so to ensure we have a good value we can take the timestamp of the last block in the batch
		// and just add 5 minutes
		lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[len(blocks)-1])
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

		previousBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
		if err != nil {
			return verifierBundle, err
		}

		ok, executorResponse, executorErr := e.Verify(payload, request, previousBlock.Root())
		if executorErr != nil {
			if errors.Is(executorErr, ErrExecutorStateRootMismatch) {
				log.Error("[Verifier] State root mismatch detected", "err", executorErr)
			} else if errors.Is(executorErr, ErrExecutorUnknownError) {
				log.Error("[Verifier] Unexpected error found from executor", "err", executorErr)
			} else {
				log.Error("[Verifier] Error", "err", executorErr)
			}
		}

		if err = v.checkAndWriteToStream(tx, hermezDb, request.BatchNumber); err != nil {
			log.Error("error writing data to stream", "err", err)
		}

		verifierBundle.response = &VerifierResponse{
			BatchNumber:      request.BatchNumber,
			Valid:            ok,
			Witness:          witness,
			ExecutorResponse: executorResponse,
			Error:            executorErr,
		}
		return verifierBundle, nil
	})

	// add batch to the list of batches we've added
	v.addedBatches[request.BatchNumber] = struct{}{}

	// add the promise to the list of promises
	v.promises = append(v.promises, promise)
	return promise
}

func (v *LegacyExecutorVerifier) checkAndWriteToStream(tx kv.Tx, hdb *hermez_db.HermezDbReader, newBatch uint64) error {
	v.responsesMtx.Lock()
	defer v.responsesMtx.Unlock()

	v.responsesToWrite[newBatch] = struct{}{}

	// if we haven't written anything yet - cold start of the node
	if v.lowestWrittenBatch == 0 {
		// we haven't written anything yet so lets make sure there is no gap
		// in the stream for this batch
		latestBatch, err := v.streamServer.GetHighestBatchNumber()
		if err != nil {
			return err
		}
		log.Info("[Verifier] Initialising on cold start", "latestBatch", latestBatch, "newBatch", newBatch)

		v.lowestWrittenBatch = latestBatch

		// check if we have the next batch we're waiting for
		if latestBatch == newBatch-1 {
			v.lowestWrittenBatch = newBatch
			if err := v.WriteBatchToStream(newBatch, hdb, tx); err != nil {
				return err
			}
			delete(v.responsesToWrite, newBatch)
		}
	}

	// now check if the batch we want next is good
	for {
		// check if we have the next batch to write
		nextBatch := v.lowestWrittenBatch + 1
		if _, ok := v.responsesToWrite[nextBatch]; !ok {
			break
		}

		if err := v.WriteBatchToStream(nextBatch, hdb, tx); err != nil {
			return err
		}
		delete(v.responsesToWrite, nextBatch)
		v.lowestWrittenBatch = nextBatch
	}

	return nil
}

// Unsafe is not thread-safe so it MUST be invoked only from a single thread
func (v *LegacyExecutorVerifier) ProcessResultsSequentiallyUnsafe(tx kv.RwTx) ([]*VerifierResponse, error) {
	results := make([]*VerifierResponse, 0, len(v.promises))
	for i := 0; i < len(v.promises); i++ {
		verifierBundle, err := v.promises[i].TryGet()
		if verifierBundle == nil && err == nil {
			break
		}

		if err != nil {
			// let leave it for debug purposes
			// a cancelled promise is removed from v.promises => it should never appear here, that's why let's panic if it happens, because it will indicate for massive error
			if errors.Is(err, ErrPromiseCancelled) {
				panic("this should never happen")
			}

			log.Error("error on our end while preparing the verification request, re-queueing the task", "err", err)
			// this is an error on our end, so just re-create the promise at exact position where it was
			if verifierBundle.request.isOverdue() {
				return nil, fmt.Errorf("error: batch %d couldn't be processed in 30 minutes", verifierBundle.request.BatchNumber)
			}

			v.promises[i] = NewPromise[*VerifierBundle](v.promises[i].task)
			break
		}

		verifierResponse := verifierBundle.response
		results = append(results, verifierResponse)
		delete(v.addedBatches, verifierResponse.BatchNumber)

		// no point to process any further responses if we've found an invalid one
		if !verifierResponse.Valid {
			break
		}
	}

	// leave only non-processed promises
	// v.promises = v.promises[len(results):]

	return results, nil
}

func (v *LegacyExecutorVerifier) MarkTopResponseAsProcessed(batchNumber uint64) {
	v.promises = v.promises[1:]
	delete(v.addedBatches, batchNumber)
}

// Unsafe is not thread-safe so it MUST be invoked only from a single thread
func (v *LegacyExecutorVerifier) CancelAllRequestsUnsafe() {
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
	v.addedBatches = map[uint64]struct{}{}
}

// Unsafe is not thread-safe so it MUST be invoked only from a single thread
func (v *LegacyExecutorVerifier) HasExecutorsUnsafe() bool {
	return len(v.executors) > 0
}

// Unsafe is not thread-safe so it MUST be invoked only from a single thread
func (v *LegacyExecutorVerifier) IsRequestAddedUnsafe(batch uint64) bool {
	_, ok := v.addedBatches[batch]
	return ok
}

func (v *LegacyExecutorVerifier) WriteBatchToStream(batchNumber uint64, hdb *hermez_db.HermezDbReader, roTx kv.Tx) error {
	log.Info("[Verifier] Writing batch to stream", "batch", batchNumber)
	blks, err := hdb.GetL2BlockNosByBatch(batchNumber)
	if err != nil {
		return err
	}

	if err := v.streamServer.WriteBlocksToStream(roTx, hdb, blks[0], blks[len(blks)-1], "verifier"); err != nil {
		return err
	}
	return nil
}

func (v *LegacyExecutorVerifier) getNextOnlineAvailableExecutor() ILegacyExecutor {
	var exec ILegacyExecutor

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

func (v *LegacyExecutorVerifier) availableBlocksToProcess(innerCtx context.Context, batchNumber uint64) ([]uint64, error) {
	tx, err := v.db.BeginRo(innerCtx)
	if err != nil {
		return []uint64{}, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)
	blocks, err := hermezDb.GetL2BlockNosByBatch(batchNumber)
	if err != nil {
		return []uint64{}, err
	}

	for _, blockNum := range blocks {
		block, err := rawdb.ReadBlockByNumber(tx, blockNum)
		if err != nil {
			return []uint64{}, err
		}
		if block == nil {
			return []uint64{}, nil
		}
	}

	return blocks, nil
}

func (v *LegacyExecutorVerifier) GetStreamBytes(
	batchNumber uint64,
	tx kv.Tx,
	blocks []uint64,
	hermezDb *hermez_db.HermezDbReader,
	l1InfoTreeMinTimestamps map[uint64]uint64,
	transactionsToIncludeByIndex [][]int, // passing nil here will include all transactions in the blocks
) ([]byte, error) {
	lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
	if err != nil {
		return nil, err
	}
	var streamBytes []byte

	// as we only ever use the executor verifier for whole batches we can safely assume that the previous batch
	// will always be the request batch - 1 and that the first block in the batch will be at the batch
	// boundary so we will always add in the batch bookmark to the stream
	previousBatch := batchNumber - 1

	for idx, blockNumber := range blocks {
		block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
		if err != nil {
			return nil, err
		}

		var sBytes []byte

		isBatchEnd := idx == len(blocks)-1

		var transactionsToIncludeByIndexInBlock []int = nil
		if transactionsToIncludeByIndex != nil {
			transactionsToIncludeByIndexInBlock = transactionsToIncludeByIndex[idx]
		}
		sBytes, err = server.CreateAndBuildStreamEntryBytesProto(v.streamServer.GetChainId(), block, hermezDb, tx, lastBlock, batchNumber, previousBatch, l1InfoTreeMinTimestamps, isBatchEnd, transactionsToIncludeByIndexInBlock)
		if err != nil {
			return nil, err
		}

		streamBytes = append(streamBytes, sBytes...)
		lastBlock = block
		// we only put in the batch bookmark at the start of the stream data once
		previousBatch = batchNumber
	}

	return streamBytes, nil
}
