package legacy_executor_verifier

import (
	"context"
	"encoding/hex"
	"strconv"
	"sync"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	dstypes "github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/log/v3"
	"fmt"
	"sort"
	"time"
	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
)

const (
	ROLLUP_ID = 1 // todo [zkevm] this should be read from config to anticipate more than 1 rollup per manager contract
)

type VerifierRequest struct {
	BatchNumber uint64
	ForkId      uint64
	StateRoot   common.Hash
	CheckCount  int
	Counters    map[string]int
}

type VerifierResponse struct {
	BatchNumber uint64
	Valid       bool
	Witness     []byte
}

var ErrNoExecutorAvailable = fmt.Errorf("no executor available")

type ILegacyExecutor interface {
	Verify(*Payload, *VerifierRequest, common.Hash) (bool, error)
	CheckOnline() bool
}

type WitnessGenerator interface {
	GenerateWitness(tx kv.Tx, ctx context.Context, startBlock, endBlock uint64, debug, witnessFull bool) ([]byte, error)
}

type LegacyExecutorVerifier struct {
	db             kv.RwDB
	cfg            ethconfig.Zk
	executors      []ILegacyExecutor
	executorNumber int

	working       *sync.Mutex
	openRequests  []*VerifierRequest
	requestsMap   map[uint64]uint64
	responses     []*VerifierResponse
	responseMutex *sync.Mutex
	quit          chan struct{}

	streamServer     *server.DataStreamServer
	stream           *datastreamer.StreamServer
	witnessGenerator WitnessGenerator
	l1Syncer         *syncer.L1Syncer
	executorGrpc     executor.ExecutorServiceClient
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
	executorLocks := make([]*sync.Mutex, len(executors))
	for i := range executorLocks {
		executorLocks[i] = &sync.Mutex{}
	}

	streamServer := server.NewDataStreamServer(stream, chainCfg.ChainID.Uint64(), server.ExecutorOperationMode)

	verifier := &LegacyExecutorVerifier{
		cfg:              cfg,
		executors:        executors,
		db:               db,
		executorNumber:   0,
		working:          &sync.Mutex{},
		openRequests:     make([]*VerifierRequest, 0),
		requestsMap:      make(map[uint64]uint64),
		responses:        make([]*VerifierResponse, 0),
		responseMutex:    &sync.Mutex{},
		quit:             make(chan struct{}),
		streamServer:     streamServer,
		stream:           stream,
		witnessGenerator: witnessGenerator,
		l1Syncer:         l1Syncer,
	}

	return verifier
}

func (v *LegacyExecutorVerifier) StopWork() {
	close(v.quit)
}

func (v *LegacyExecutorVerifier) StartWork() {
	go func() {
		tick := time.NewTicker(1 * time.Second)
	LOOP:
		for {
			select {
			case <-v.quit:
				break LOOP
			case <-tick.C:
				go v.processOpenRequests()
			}
		}
	}()
}

func (v *LegacyExecutorVerifier) processOpenRequests() {
	v.working.Lock()
	defer v.working.Unlock()

	if len(v.openRequests) == 0 {
		return
	}

	// sort the requests by batch number ascending
	sort.Slice(v.openRequests, func(i, j int) bool {
		return v.openRequests[i].BatchNumber < v.openRequests[j].BatchNumber
	})

	// we want to trim down the requests once we have worked through them so keep track of
	// where we got to
	successCount := 0

	// process the requests
	for _, request := range v.openRequests {
		processed, err := v.handleRequest(context.Background(), request)
		if err != nil {
			log.Error("[Verifier] error handling request", "batch", request.BatchNumber, "err", err)
			break
		}
		if !processed {
			// likely the underlying stage loop is still running so the batch had no transactions
			// in this case subsequent batches will also be in the same state so exit now and wait
			// for the next iteration
			break
		}
		successCount++
	}

	v.openRequests = v.openRequests[successCount:]
}

func (v *LegacyExecutorVerifier) getNextOnlineExecutor() ILegacyExecutor {
	var exec ILegacyExecutor

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

func (v *LegacyExecutorVerifier) handleRequest(ctx context.Context, request *VerifierRequest) (bool, error) {
	// if we have no executor config then just skip this step and treat everything as OK
	if len(v.executors) == 0 {
		response := &VerifierResponse{
			BatchNumber: request.BatchNumber,
			Valid:       true,
		}
		v.handleResponse(response)
		return true, nil
	}

	execer := v.getNextOnlineExecutor()
	if execer == nil {
		return false, ErrNoExecutorAvailable
	}

	// mapmutation has some issue with us not having a quit channel on the context call to `Done` so
	// here we're creating a cancelable context and just deferring the cancel
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := v.db.BeginRo(innerCtx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	hermezDb := hermez_db.NewHermezDbReader(tx)

	// get the data stream bytes
	blocks, err := hermezDb.GetL2BlockNosByBatch(request.BatchNumber)
	if err != nil {
		return false, err
	}

	// we might not have blocks yet as the underlying stage loop might still be running and the tx hasn't been
	// committed yet so just requeue the request
	if len(blocks) == 0 {
		request.CheckCount++
		return false, nil
	}

	l1InfoTreeMinTimestamps := make(map[uint64]uint64)
	streamBytes, err := v.GetStreamBytes(request, tx, blocks, hermezDb, l1InfoTreeMinTimestamps)
	if err != nil {
		return false, err
	}

	witness, err := v.witnessGenerator.GenerateWitness(tx, innerCtx, blocks[0], blocks[len(blocks)-1], false, v.cfg.WitnessFull)
	if err != nil {
		return false, err
	}

	log.Debug("witness generated", "data", hex.EncodeToString(witness))

	// executor is perfectly happy with just an empty hash here
	oldAccInputHash := common.HexToHash("0x0")

	// now we need to figure out the timestamp limit for this payload.  It must be:
	// timestampLimit >= currentTimestamp (from batch pre-state) + deltaTimestamp
	// so to ensure we have a good value we can take the timestamp of the last block in the batch
	// and just add 5 minutes
	lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[len(blocks)-1])
	if err != nil {
		return false, err
	}
	timestampLimit := lastBlock.Time()

	payload := &Payload{
		Witness:                 witness,
		DataStream:              streamBytes,
		Coinbase:                v.cfg.AddressSequencer.String(),
		OldAccInputHash:         oldAccInputHash.Bytes(),
		L1InfoRoot:              nil,
		TimestampLimit:          timestampLimit,
		ForcedBlockhashL1:       []byte{0},
		ContextId:               strconv.Itoa(int(request.BatchNumber)),
		L1InfoTreeMinTimestamps: l1InfoTreeMinTimestamps,
	}

	previousBlock, _ := rawdb.ReadBlockByNumber(tx, blocks[0]-1)

	ok, err := execer.Verify(payload, request, previousBlock.Root())
	if err != nil {
		return false, err
	}

	if ok {
		// update the datastream now that we know the batch is OK
		if err = server.WriteBlocksToStream(tx, hermezDb, v.streamServer, v.stream, blocks[0], blocks[len(blocks)-1], "verifier"); err != nil {
			return true, err
		}
	}

	response := &VerifierResponse{
		BatchNumber: request.BatchNumber,
		Valid:       ok,
		Witness:     witness,
	}
	v.handleResponse(response)

	return true, nil
}

func (v *LegacyExecutorVerifier) GetStreamBytes(request *VerifierRequest, tx kv.Tx, blocks []uint64, hermezDb *hermez_db.HermezDbReader, l1InfoTreeMinTimestamps map[uint64]uint64) ([]byte, error) {
	lastBlock, err := rawdb.ReadBlockByNumber(tx, blocks[0]-1)
	if err != nil {
		return nil, err
	}
	var streamBytes []byte

	// as we only ever use the executor verifier for whole batches we can safely assume that the previous batch
	// will always be the request batch - 1 and that the first block in the batch will be at the batch
	// boundary so we will always add in the batch bookmark to the stream
	previousBatch := request.BatchNumber - 1

	for _, blockNumber := range blocks {
		block, err := rawdb.ReadBlockByNumber(tx, blockNumber)
		if err != nil {
			return nil, err
		}

		//TODO: get ger updates between blocks
		gerUpdates := []dstypes.GerUpdate{}

		sBytes, err := v.streamServer.CreateAndBuildStreamEntryBytes(block, hermezDb, lastBlock, request.BatchNumber, previousBatch, true, &gerUpdates, l1InfoTreeMinTimestamps)
		if err != nil {
			return nil, err
		}
		streamBytes = append(streamBytes, sBytes...)
		lastBlock = block
		// we only put in the batch bookmark at the start of the stream data once
		previousBatch = request.BatchNumber
	}

	return streamBytes, nil
}

func (v *LegacyExecutorVerifier) handleResponse(response *VerifierResponse) {
	v.responseMutex.Lock()
	defer v.responseMutex.Unlock()
	v.responses = append(v.responses, response)
}

// not thread safe
func (v *LegacyExecutorVerifier) IsRequestAdded(batchNumber uint64) bool {
	_, exists := v.requestsMap[batchNumber]
	return exists
}

// not thread safe
func (v *LegacyExecutorVerifier) AddRequest(request *VerifierRequest) {
	_, exists := v.requestsMap[request.BatchNumber]
	if exists {
		return
	}

	v.requestsMap[request.BatchNumber] = request.BatchNumber
	go v.addRequestWhenFree(request)
}

func (v *LegacyExecutorVerifier) addRequestWhenFree(request *VerifierRequest) {
	v.working.Lock()
	v.openRequests = append(v.openRequests, request)
	v.working.Unlock()

	v.processOpenRequests()
}

// not thread safe
func (v *LegacyExecutorVerifier) MarkRequestAsHandled(batchNumber uint64) {
	delete(v.requestsMap, batchNumber)
}

func (v *LegacyExecutorVerifier) GetAllResponses() []*VerifierResponse {
	v.responseMutex.Lock()
	defer v.responseMutex.Unlock()
	result := make([]*VerifierResponse, len(v.responses))
	copy(result, v.responses)
	return result
}

func (v *LegacyExecutorVerifier) RemoveResponse(batchNumber uint64) {
	v.responseMutex.Lock()
	defer v.responseMutex.Unlock()

	result := make([]*VerifierResponse, 0, len(v.responses))
	for _, response := range v.responses {
		if response.BatchNumber != batchNumber {
			result = append(result, response)
		}
	}
	v.responses = result
}

func (v *LegacyExecutorVerifier) HasExecutors() bool {
	return len(v.executors) > 0
}
