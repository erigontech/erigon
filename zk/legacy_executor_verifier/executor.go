package legacy_executor_verifier

import (
	"bytes"
	"context"
	"fmt"
	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier/proto/github.com/0xPolygonHermez/zkevm-node/state/runtime/executor"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"time"
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
	L1InfoRoot        []byte // 0 for executor, required for the prover
	TimestampLimit    uint64 // if 0, replace by now + 10 min internally
	ForcedBlockhashL1 []byte // we need it, 0 in regular batches, hash in forced batches, also used in injected/first batches, 0 by now
	ContextId         string // batch ID to be shown in the executor traces, for your convenience: "Erigon_candidate_batch_N"
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
	conn, err := grpc.DialContext(ctx, grpcUrl, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to dial grpc: %w", err)
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

func (e *Executor) Verify(p *Payload, request *VerifierRequest, oldStateRoot common.Hash) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Debug("Sending request to grpc server", "grpcUrl", e.grpcUrl)

	size := 1024 * 1024 * 256 // 256mb maximum size - hack for now until trimmed witness is proved off
	resp, err := e.client.ProcessStatelessBatchV2(ctx, &executor.ProcessStatelessBatchRequestV2{
		Witness:           p.Witness,
		DataStream:        p.DataStream,
		Coinbase:          p.Coinbase,
		OldAccInputHash:   p.OldAccInputHash,
		L1InfoRoot:        p.L1InfoRoot,
		TimestampLimit:    p.TimestampLimit,
		ForcedBlockhashL1: p.ForcedBlockhashL1,
		ContextId:         p.ContextId,
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

	counters := fmt.Sprintf("[SHA: %v]", resp.CntSha256Hashes)
	counters += fmt.Sprintf("[A: %v]", resp.CntArithmetics)
	counters += fmt.Sprintf("[B: %v]", resp.CntBinaries)
	counters += fmt.Sprintf("[K: %v]", resp.CntKeccakHashes)
	counters += fmt.Sprintf("[M: %v]", resp.CntMemAligns)
	counters += fmt.Sprintf("[P: %v]", resp.CntPoseidonHashes)
	counters += fmt.Sprintf("[S: %v]", resp.CntSteps)
	counters += fmt.Sprintf("[D: %v]", resp.CntPoseidonPaddings)

	log.Info("executor result",
		"batch", request.BatchNumber,
		"counters", counters,
		"exec-root", common.BytesToHash(resp.NewStateRoot),
		"our-root", request.StateRoot,
		"exec-old-root", common.BytesToHash(resp.OldStateRoot),
		"our-old-root", oldStateRoot)

	log.Debug("Received response from executor", "grpcUrl", e.grpcUrl, "response", resp)

	return responseCheck(resp, request.StateRoot)
}

func responseCheck(resp *executor.ProcessBatchResponseV2, erigonStateRoot common.Hash) (bool, error) {
	if resp == nil {
		return false, fmt.Errorf("nil response")
	}
	if resp.Error != executor.ExecutorError_EXECUTOR_ERROR_UNSPECIFIED &&
		resp.Error != executor.ExecutorError_EXECUTOR_ERROR_NO_ERROR {
		return false, fmt.Errorf("error in response: %s", resp.Error)
	}

	if !bytes.Equal(resp.NewStateRoot, erigonStateRoot.Bytes()) {
		return false, fmt.Errorf("erigon state root mismatch: expected %s, got %s", erigonStateRoot, common.BytesToHash(resp.NewStateRoot))
	}

	return true, nil
}
