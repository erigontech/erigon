package legacy_executor_verifier

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"encoding/hex"
	"encoding/json"
	"os"
	"path"

	"github.com/dustin/go-humanize"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/google/uuid"
)

var (
	ErrExecutorStateRootMismatch = errors.New("executor state root mismatches")
	ErrExecutorUnknownError      = errors.New("unknown error from executor")
)

type Config struct {
	GrpcUrls              []string
	Timeout               time.Duration
	MaxConcurrentRequests int
	OutputLocation        string
}

type Payload struct {
	Witness         []byte // SMT partial tree, SCs, (indirectly) old state root
	DataStream      []byte // txs, old batch num, chain id, fork id, effective gas price, block header, index of L1 info tree (global exit root, min timestamp, ...)
	Coinbase        string // sequencer address
	OldAccInputHash []byte // 0 for executor, required for the prover
	// Used by injected/first batches (do not use it for regular batches)
	L1InfoRoot              []byte            // 0 for executor, required for the prover
	TimestampLimit          uint64            // if 0, replace by now + 10 min internally
	ForcedBlockhashL1       []byte            // we need it, 0 in regular batches, hash in forced batches, also used in injected/first batches, 0 by now
	ContextId               string            // batch ID to be shown in the executor traces, for your convenience: "Erigon_candidate_batch_N"
	L1InfoTreeMinTimestamps map[uint64]uint64 // info tree index to min timestamp mappings
}

type RpcPayload struct {
	Witness         string `json:"witness"`         // SMT partial tree, SCs, (indirectly) old state root
	Coinbase        string `json:"coinbase"`        // sequencer address
	OldAccInputHash string `json:"oldAccInputHash"` // 0 for executor, required for the prover
	// Used by injected/first batches (do not use it for regular batches)
	TimestampLimit    uint64 `json:"timestampLimit"`    // if 0, replace by now + 10 min internally
	ForcedBlockhashL1 string `json:"forcedBlockhashL1"` // we need it, 0 in regular batches, hash in forced batches, also used in injected/first batches, 0 by now
}

type Executor struct {
	grpcUrl    string
	conn       *grpc.ClientConn
	connCancel context.CancelFunc
	client     executor.ExecutorServiceClient
	semaphore  chan struct{}

	// if not empty then the executor will write the payload to this location before sending it to the
	// remote executor
	outputLocation string
}

func NewExecutors(cfg Config) []*Executor {
	executors := make([]*Executor, len(cfg.GrpcUrls))
	for i, grpcUrl := range cfg.GrpcUrls {
		executors[i] = NewExecutor(grpcUrl, cfg.Timeout, cfg.MaxConcurrentRequests, cfg.OutputLocation)
	}
	return executors
}

func NewExecutor(grpcUrl string, timeout time.Duration, maxConcurrentRequests int, outputLocation string) *Executor {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, grpcUrl, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	// log the error but continue on because the executor may come back online later and we will re-attempt
	// to connect when it is asked to verify
	if err != nil {
		log.Error("Failed to dial grpc", "grpcUrl", grpcUrl, "error", err)
	}

	client := executor.NewExecutorServiceClient(conn)

	e := &Executor{
		grpcUrl:        grpcUrl,
		conn:           conn,
		connCancel:     cancel,
		client:         client,
		semaphore:      make(chan struct{}, maxConcurrentRequests),
		outputLocation: outputLocation,
	}

	return e
}

func (e *Executor) Close() {
	if e == nil || e.conn == nil {
		return
	}
	e.connCancel()
	err := e.conn.Close()
	if err != nil {
		log.Warn("Failed to close grpc connection", err)
	}
}

// QueueLength check 'how busy' the executor is
func (e *Executor) QueueLength() int {
	return len(e.semaphore)
}

func (e *Executor) AquireAccess() {
	e.semaphore <- struct{}{}
}

func (e *Executor) ReleaseAccess() {
	<-e.semaphore
}

func (e *Executor) CheckOnline() bool {
	// first ensure there is a connection to work with
	if e.conn == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(ctx, e.grpcUrl, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			log.Error("Failed to dial grpc", "grpcUrl", e.grpcUrl, "error", err)
			return false
		}
		e.conn = conn
		e.client = executor.NewExecutorServiceClient(conn)

		// no point in checking the state if we just connected so just return ok
		return true
	}

	state := e.conn.GetState()

	if state == connectivity.TransientFailure || state == connectivity.Shutdown {
		log.Info("Executor reconnecting to grpc server", "grpcUrl", e.grpcUrl)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if !e.conn.WaitForStateChange(ctx, state) {
			return false
		} else {
			log.Info("Executor reconnected to grpc server", "grpcUrl", e.grpcUrl)
		}
	}

	return true
}

func (e *Executor) Verify(p *Payload, request *VerifierRequest, oldStateRoot common.Hash) (bool, *executor.ProcessBatchResponseV2, error, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	correlation := uuid.New().String()
	witnessSize := humanize.Bytes(uint64(len(p.Witness)))
	dataStreamSize := humanize.Bytes(uint64(len(p.DataStream)))
	log.Info("Sending request to grpc server",
		"grpcUrl", e.grpcUrl,
		"ourRoot", request.StateRoot,
		"oldRoot", oldStateRoot,
		"batch", request.BatchNumber,
		"witness-size", witnessSize,
		"data-stream-size", dataStreamSize,
		"blocks-count", len(request.BlockNumbers),
		"correlation", correlation)

	size := 1024 * 1024 * 256 // 256mb maximum size - hack for now until trimmed witness is proved off

	grpcRequest := &executor.ProcessStatelessBatchRequestV2{
		Witness:                     p.Witness,
		DataStream:                  p.DataStream,
		Coinbase:                    p.Coinbase,
		OldAccInputHash:             p.OldAccInputHash,
		L1InfoRoot:                  p.L1InfoRoot,
		TimestampLimit:              p.TimestampLimit,
		ForcedBlockhashL1:           p.ForcedBlockhashL1,
		ContextId:                   p.ContextId,
		L1InfoTreeIndexMinTimestamp: p.L1InfoTreeMinTimestamps,
	}

	if e.outputLocation != "" {
		asJson, err := json.Marshal(grpcRequest)
		if err != nil {
			return false, nil, nil, err
		}
		file := path.Join(e.outputLocation, fmt.Sprintf("payload_%d.json", request.BatchNumber))
		err = os.WriteFile(file, asJson, 0644)
		if err != nil {
			return false, nil, nil, err
		}

		// now save the witness as a hex string along with the datastream
		// this is to allow for easy debugging of the witness and datastream
		witnessHexFile := path.Join(e.outputLocation, fmt.Sprintf("witness_%d.hex", request.BatchNumber))
		witnessAsHex := "0x" + hex.EncodeToString(p.Witness)
		err = os.WriteFile(witnessHexFile, []byte(witnessAsHex), 0644)
		if err != nil {
			return false, nil, nil, err
		}

		dataStreamHexFile := path.Join(e.outputLocation, fmt.Sprintf("datastream_%d.hex", request.BatchNumber))
		dataStreamAsHex := "0x" + hex.EncodeToString(p.DataStream)
		err = os.WriteFile(dataStreamHexFile, []byte(dataStreamAsHex), 0644)
		if err != nil {
			return false, nil, nil, err
		}
	}

	resp, err := e.client.ProcessStatelessBatchV2(ctx, grpcRequest, grpc.MaxCallSendMsgSize(size), grpc.MaxCallRecvMsgSize(size))
	if err != nil {
		return false, nil, nil, fmt.Errorf("failed to process stateless batch: %w", err)
	}
	if resp == nil {
		return false, nil, nil, fmt.Errorf("nil response")
	}

	counters := map[string]int{
		"SHA": int(resp.CntSha256Hashes),
		"A":   int(resp.CntArithmetics),
		"B":   int(resp.CntBinaries),
		"K":   int(resp.CntKeccakHashes),
		"M":   int(resp.CntMemAligns),
		"P":   int(resp.CntPoseidonHashes),
		"S":   int(resp.CntSteps),
		"D":   int(resp.CntPoseidonPaddings),
	}

	match := bytes.Equal(resp.NewStateRoot, request.StateRoot.Bytes())

	log.Info("executor result",
		"match", match,
		"grpcUrl", e.grpcUrl,
		"batch", request.BatchNumber,
		"blocks-count", len(resp.BlockResponses),
		"our-counters", request.Counters,
		"exec-counters", counters,
		"exec-root", common.BytesToHash(resp.NewStateRoot),
		"our-root", request.StateRoot,
		"exec-old-root", common.BytesToHash(resp.OldStateRoot),
		"our-old-root", oldStateRoot,
		"correlation", correlation)

	for addr, all := range resp.ReadWriteAddresses {
		log.Debug("executor result",
			"addr", addr,
			"nonce", all.Nonce,
			"balance", all.Balance,
			"sc-code", all.ScCode,
			"sc-storage", all.ScStorage,
			"sc-length", all.ScLength)
	}

	for i, bResp := range resp.BlockResponses {
		log.Debug("executor result",
			"index", i,
			"parent-hash", common.BytesToHash(bResp.ParentHash),
			"coinbase", bResp.Coinbase,
			"gas-limit", bResp.GasLimit,
			"block-number", bResp.BlockNumber,
			"timestamp", bResp.Timestamp,
			"ger", common.BytesToHash(bResp.Ger),
			"block-hash-l1", common.BytesToHash(bResp.BlockHashL1),
			"gas-used", bResp.GasUsed,
			"block-info-root", common.BytesToHash(bResp.BlockInfoRoot),
			"block-hash", common.BytesToHash(bResp.BlockHash))
	}

	counterUndershootCheck(counters, request.Counters, request.BatchNumber)

	log.Debug("Received response from executor", "grpcUrl", e.grpcUrl, "response", resp)

	ok, executorResponse, executorErr := responseCheck(resp, request)
	return ok, executorResponse, executorErr, nil
}

func responseCheck(resp *executor.ProcessBatchResponseV2, request *VerifierRequest) (bool, *executor.ProcessBatchResponseV2, error) {
	if resp.ForkId != request.ForkId {
		log.Warn("Executor fork id mismatch", "executor", resp.ForkId, "our", request.ForkId)
	}

	if resp.Debug != nil && resp.Debug.ErrorLog != "" {
		log.Error("executor error", "detail", resp.Debug.ErrorLog)
		return false, resp, fmt.Errorf("error in response: %s", resp.Debug.ErrorLog)
	}

	if resp.Error != executor.ExecutorError_EXECUTOR_ERROR_UNSPECIFIED &&
		resp.Error != executor.ExecutorError_EXECUTOR_ERROR_NO_ERROR {
		// prover id here is the only string field in the response and will contain info on what key failed from
		// the provided witness
		log.Error("executor error", "detail", resp.ProverId)
		return false, resp, fmt.Errorf("%w: error in response: %s", ErrExecutorUnknownError, resp.Error)
	}

	if resp.ErrorRom != executor.RomError_ROM_ERROR_NO_ERROR && resp.ErrorRom != executor.RomError_ROM_ERROR_UNSPECIFIED {
		log.Error("executor ROM error", "detail", resp.ErrorRom)
		return false, resp, fmt.Errorf("error in response: %s", resp.ErrorRom)
	}

	if !bytes.Equal(resp.NewStateRoot, request.StateRoot.Bytes()) {
		return false, resp, fmt.Errorf("%w: expected %s, got %s", ErrExecutorStateRootMismatch, request.StateRoot, common.BytesToHash(resp.NewStateRoot))
	}

	return true, resp, nil
}

func counterUndershootCheck(respCounters, counters map[string]int, batchNo uint64) {
	for k, legacy := range respCounters {
		if counters[k] < legacy {
			log.Warn("Counter undershoot", "counter", k, "erigon", counters[k], "legacy", legacy, "batch", batchNo)
		}
	}
}
