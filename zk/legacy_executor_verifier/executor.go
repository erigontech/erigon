package legacy_executor_verifier

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/connectivity"
)

type Config struct {
	GrpcUrls []string
	Timeout  time.Duration
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
}

func NewExecutors(cfg Config) []*Executor {
	executors := make([]*Executor, len(cfg.GrpcUrls))
	var err error
	for i, grpcUrl := range cfg.GrpcUrls {
		executors[i], err = NewExecutor(grpcUrl, cfg.Timeout)
		if err != nil {
			log.Warn("Failed to create executor", "error", err)
		}
	}
	return executors
}

func NewExecutor(grpcUrl string, timeout time.Duration) (*Executor, error) {
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
		grpcUrl:    grpcUrl,
		conn:       conn,
		connCancel: cancel,
		client:     client,
	}

	return e, nil
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

func (e *Executor) Verify(p *Payload, request *VerifierRequest, oldStateRoot common.Hash) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	log.Info("Sending request to grpc server", "grpcUrl", e.grpcUrl, "ourRoot", request.StateRoot, "oldRoot", oldStateRoot, "batch", request.BatchNumber)

	size := 1024 * 1024 * 256 // 256mb maximum size - hack for now until trimmed witness is proved off
	resp, err := e.client.ProcessStatelessBatchV2(ctx, &executor.ProcessStatelessBatchRequestV2{
		Witness:                     p.Witness,
		DataStream:                  p.DataStream,
		Coinbase:                    p.Coinbase,
		OldAccInputHash:             p.OldAccInputHash,
		L1InfoRoot:                  p.L1InfoRoot,
		TimestampLimit:              p.TimestampLimit,
		ForcedBlockhashL1:           p.ForcedBlockhashL1,
		ContextId:                   p.ContextId,
		L1InfoTreeIndexMinTimestamp: p.L1InfoTreeMinTimestamps,
		//TraceConfig: &executor.TraceConfigV2{
		//	DisableStorage:            0,
		//	DisableStack:              0,
		//	EnableMemory:              0,
		//	EnableReturnData:          0,
		//	TxHashToGenerateFullTrace: nil,
		//},
	}, grpc.MaxCallSendMsgSize(size), grpc.MaxCallRecvMsgSize(size))
	if err != nil {
		return false, fmt.Errorf("failed to process stateless batch: %w", err)
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

	log.Info("executor result",
		"grpcUrl", e.grpcUrl,
		"batch", request.BatchNumber,
		"counters", counters,
		"exec-root", common.BytesToHash(resp.NewStateRoot),
		"our-root", request.StateRoot,
		"exec-old-root", common.BytesToHash(resp.OldStateRoot),
		"our-old-root", oldStateRoot,
		"blocks-count", len(resp.BlockResponses))

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

	return responseCheck(resp, request)
}

func responseCheck(resp *executor.ProcessBatchResponseV2, request *VerifierRequest) (bool, error) {
	if resp == nil {
		return false, fmt.Errorf("nil response")
	}

	if resp.ForkId != request.ForkId {
		log.Warn("Executor fork id mismatch", "executor", resp.ForkId, "our", request.ForkId)
	}

	if resp.Debug != nil && resp.Debug.ErrorLog != "" {
		log.Error("executor error", "detail", resp.Debug.ErrorLog)
		return false, fmt.Errorf("error in response: %s", resp.Debug.ErrorLog)
	}

	if resp.Error != executor.ExecutorError_EXECUTOR_ERROR_UNSPECIFIED &&
		resp.Error != executor.ExecutorError_EXECUTOR_ERROR_NO_ERROR {
		// prover id here is the only string field in the response and will contain info on what key failed from
		// the provided witness
		log.Error("executor error", "detail", resp.ProverId)
		return false, fmt.Errorf("error in response: %s", resp.Error)

	}

	erigonStateRoot := request.StateRoot
	if !bytes.Equal(resp.NewStateRoot, erigonStateRoot.Bytes()) {
		return false, fmt.Errorf("erigon state root mismatch: expected %s, got %s", erigonStateRoot, common.BytesToHash(resp.NewStateRoot))
	}

	return true, nil
}

func counterUndershootCheck(respCounters, counters map[string]int, batchNo uint64) {
	for k, legacy := range respCounters {
		if counters[k] < legacy {
			log.Warn("Counter undershoot", "counter", k, "erigon", counters[k], "legacy", legacy, "batch", batchNo)
		}
	}
}
