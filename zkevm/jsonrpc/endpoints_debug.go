package jsonrpc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/ledgerwatch/erigon-lib/common"
	ethTypes "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zkevm/jsonrpc/types"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/ledgerwatch/erigon/zkevm/state"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/instrumentation"
)

// DebugEndpoints is the debug jsonrpc endpoint
type DebugEndpoints struct {
	state types.StateInterface
	txMan dbTxManager
}

type traceConfig struct {
	DisableStorage   bool    `json:"disableStorage"`
	DisableStack     bool    `json:"disableStack"`
	EnableMemory     bool    `json:"enableMemory"`
	EnableReturnData bool    `json:"enableReturnData"`
	Tracer           *string `json:"tracer"`
}

// StructLogRes represents the debug trace information for each opcode
type StructLogRes struct {
	Pc            uint64             `json:"pc"`
	Op            string             `json:"op"`
	Gas           uint64             `json:"gas"`
	GasCost       uint64             `json:"gasCost"`
	Depth         int                `json:"depth"`
	Error         string             `json:"error,omitempty"`
	Stack         *[]types.ArgBig    `json:"stack,omitempty"`
	Memory        *[]string          `json:"memory,omitempty"`
	Storage       *map[string]string `json:"storage,omitempty"`
	RefundCounter uint64             `json:"refund,omitempty"`
}

type traceTransactionResponse struct {
	Gas         uint64         `json:"gas"`
	Failed      bool           `json:"failed"`
	ReturnValue interface{}    `json:"returnValue"`
	StructLogs  []StructLogRes `json:"structLogs"`
}

type traceBlockTransactionResponse struct {
	Result traceTransactionResponse `json:"result"`
}

// TraceTransaction creates a response for debug_traceTransaction request.
// See https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debugtracetransaction
func (d *DebugEndpoints) TraceTransaction(hash types.ArgHash, cfg *traceConfig) (interface{}, types.Error) {
	return d.txMan.NewDbTxScope(d.state, func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
		return d.buildTraceTransaction(ctx, hash.Hash(), cfg, dbTx)
	})
}

// TraceBlockByNumber creates a response for debug_traceBlockByNumber request.
// See https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debugtraceblockbynumber
func (d *DebugEndpoints) TraceBlockByNumber(number types.BlockNumber, cfg *traceConfig) (interface{}, types.Error) {
	return d.txMan.NewDbTxScope(d.state, func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
		blockNumber, rpcErr := number.GetNumericBlockNumber(ctx, d.state, dbTx)
		if rpcErr != nil {
			return nil, rpcErr
		}

		block, err := d.state.GetL2BlockByNumber(ctx, blockNumber, dbTx)
		if errors.Is(err, state.ErrNotFound) {
			return nil, types.NewRPCError(types.DefaultErrorCode, fmt.Sprintf("block #%d not found", blockNumber))
		} else if err == state.ErrNotFound {
			return rpcErrorResponse(types.DefaultErrorCode, "failed to get block by number", err)
		}

		traces, rpcErr := d.buildTraceBlock(ctx, block.Transactions(), cfg, dbTx)
		if err != nil {
			return nil, rpcErr
		}

		return traces, nil
	})
}

// TraceBlockByHash creates a response for debug_traceBlockByHash request.
// See https://geth.ethereum.org/docs/interacting-with-geth/rpc/ns-debug#debugtraceblockbyhash
func (d *DebugEndpoints) TraceBlockByHash(hash types.ArgHash, cfg *traceConfig) (interface{}, types.Error) {
	return d.txMan.NewDbTxScope(d.state, func(ctx context.Context, dbTx pgx.Tx) (interface{}, types.Error) {
		block, err := d.state.GetL2BlockByHash(ctx, hash.Hash(), dbTx)
		if errors.Is(err, state.ErrNotFound) {
			return nil, types.NewRPCError(types.DefaultErrorCode, fmt.Sprintf("block %s not found", hash.Hash().String()))
		} else if err == state.ErrNotFound {
			return rpcErrorResponse(types.DefaultErrorCode, "failed to get block by hash", err)
		}

		traces, rpcErr := d.buildTraceBlock(ctx, block.Transactions(), cfg, dbTx)
		if err != nil {
			return nil, rpcErr
		}

		return traces, nil
	})
}

func (d *DebugEndpoints) buildTraceBlock(ctx context.Context, txs []*ethTypes.Transaction, cfg *traceConfig, dbTx pgx.Tx) (interface{}, types.Error) {
	traces := []traceBlockTransactionResponse{}
	for _, tx := range txs {
		traceTransaction, err := d.buildTraceTransaction(ctx, tx.Hash(), cfg, dbTx)
		if err != nil {
			errMsg := fmt.Sprintf("failed to get trace for transaction %v", tx.Hash().String())
			return rpcErrorResponse(types.DefaultErrorCode, errMsg, err)
		}
		traceBlockTransaction := traceBlockTransactionResponse{
			Result: traceTransaction.(traceTransactionResponse),
		}
		traces = append(traces, traceBlockTransaction)
	}

	return traces, nil
}

func (d *DebugEndpoints) buildTraceTransaction(ctx context.Context, hash common.Hash, cfg *traceConfig, dbTx pgx.Tx) (interface{}, types.Error) {
	traceConfig := state.TraceConfig{}

	if cfg != nil {
		traceConfig.DisableStack = cfg.DisableStack
		traceConfig.DisableStorage = cfg.DisableStorage
		traceConfig.EnableMemory = cfg.EnableMemory
		traceConfig.EnableReturnData = cfg.EnableReturnData
		traceConfig.Tracer = cfg.Tracer
	}

	result, err := d.state.DebugTransaction(ctx, hash, traceConfig, dbTx)
	if errors.Is(err, state.ErrNotFound) {
		return rpcErrorResponse(types.DefaultErrorCode, "transaction not found", nil)
	} else if err != nil {
		const errorMessage = "failed to get trace"
		log.Infof("%v: %v", errorMessage, err)
		return nil, types.NewRPCError(types.DefaultErrorCode, errorMessage)
	}

	if traceConfig.Tracer != nil && *traceConfig.Tracer != "" && len(result.ExecutorTraceResult) > 0 {
		return result.ExecutorTraceResult, nil
	}

	failed := result.Failed()
	var returnValue interface{}
	if traceConfig.EnableReturnData {
		returnValue = common.Bytes2Hex(result.ReturnValue)
	}

	structLogs := d.buildStructLogs(result.StructLogs, cfg)

	resp := traceTransactionResponse{
		Gas:         result.GasUsed,
		Failed:      failed,
		ReturnValue: returnValue,
		StructLogs:  structLogs,
	}

	return resp, nil
}

func (d *DebugEndpoints) buildStructLogs(stateStructLogs []instrumentation.StructLog, cfg *traceConfig) []StructLogRes {
	structLogs := make([]StructLogRes, 0, len(stateStructLogs))
	for _, structLog := range stateStructLogs {
		errRes := ""
		if structLog.Err != nil {
			errRes = structLog.Err.Error()
		}

		op := structLog.Op
		if op == "SHA3" {
			op = "KECCAK256"
		}

		structLogRes := StructLogRes{
			Pc:            structLog.Pc,
			Op:            op,
			Gas:           structLog.Gas,
			GasCost:       structLog.GasCost,
			Depth:         structLog.Depth,
			Error:         errRes,
			RefundCounter: structLog.RefundCounter,
		}

		stack := make([]types.ArgBig, 0, len(structLog.Stack))
		if !cfg.DisableStack && len(structLog.Stack) > 0 {
			for _, stackItem := range structLog.Stack {
				if stackItem != nil {
					stack = append(stack, types.ArgBig(*stackItem))
				}
			}
		}
		structLogRes.Stack = &stack

		const memoryChunkSize = 32
		memory := make([]string, 0, len(structLog.Memory))
		if cfg.EnableMemory {
			for i := 0; i < len(structLog.Memory); i = i + memoryChunkSize {
				slice32Bytes := make([]byte, memoryChunkSize)
				copy(slice32Bytes, structLog.Memory[i:i+memoryChunkSize])
				memoryStringItem := hex.EncodeToString(slice32Bytes)
				memory = append(memory, memoryStringItem)
			}
		}
		structLogRes.Memory = &memory

		if !cfg.DisableStorage && len(structLog.Storage) > 0 {
			storage := make(map[string]string, len(structLog.Storage))
			for storageKey, storageValue := range structLog.Storage {
				k := hex.EncodeToString(storageKey.Bytes())
				v := hex.EncodeToString(storageValue.Bytes())
				storage[k] = v
			}
			structLogRes.Storage = &storage
		}

		structLogs = append(structLogs, structLogRes)
	}
	return structLogs
}
