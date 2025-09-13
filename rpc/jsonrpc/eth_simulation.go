// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"math"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/transactions"
	"github.com/holiman/uint256"
)

const (
	// maxSimulateBlocks is the maximum number of blocks that can be simulated in a single request.
	maxSimulateBlocks = 256

	// timestampIncrement is the default increment between block timestamps.
	timestampIncrement = 12
)

// SimulationRequest represents the parameters for an eth_simulateV1 call.
type SimulationRequest struct {
	BlockStateCalls        []SimulatedBlock `json:"blockStateCalls"`
	TraceTransfers         bool             `json:"traceTransfers"`
	Validation             bool             `json:"validation"`
	ReturnFullTransactions bool             `json:"returnFullTransactions"`
}

// SimulatedBlock defines the simulation for a single block.
type SimulatedBlock struct {
	BlockOverrides *transactions.BlockOverrides `json:"blockOverrides,omitempty"`
	StateOverrides *ethapi.StateOverrides       `json:"stateOverrides,omitempty"`
	Calls          []ethapi.CallArgs            `json:"calls"`
}

// SimulationResult represents the result of an eth_simulateV1 call.
type SimulationResult struct {
	Results []CallResult
}

// CallResult represents the result of a single call in the simulation.
type CallResult struct {
	ReturnData string          `json:"returnData"`
	Logs       []*types.RPCLog `json:"logs"`
	GasUsed    hexutil.Uint64  `json:"gasUsed"`
	Status     hexutil.Uint64  `json:"status"`
	Error      interface{}     `json:"error,omitempty"`
}

type RPCBlock map[string]interface{}

// SimulatedBlockResult represents the result of the simulated calls for a single block (i.e. one SimulatedBlock).
type SimulatedBlockResult map[string]interface{}

// SimulateV1 implements the eth_simulateV1 JSON-RPC method.
func (api *APIImpl) SimulateV1(ctx context.Context, req SimulationRequest, blockParameter rpc.BlockNumberOrHash) ([]SimulatedBlockResult, error) {
	if len(req.BlockStateCalls) == 0 {
		return nil, errors.New("empty input")
	}
	// Default to the latest block if no block parameter is given.
	if blockParameter.BlockHash == nil && blockParameter.BlockNumber == nil {
		latestBlock := rpc.LatestBlockNumber
		blockParameter.BlockNumber = &latestBlock
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	blockNumber, hash, _, err := rpchelper.GetBlockNumber(ctx, blockParameter, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}

	blockNumberOrHash := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber - 1))
	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNumberOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, err
	}

	simulatedBlockResults := make([]SimulatedBlockResult, 0, len(req.BlockStateCalls))

	// Create a simulator instance to help with sanitization
	sim := newSimulator(&req, block.Header(), chainConfig, api.engine(), api._blockReader, api.logger, api.GasCap, api.ReturnDataLimit, api.evmCallTimeout)
	simulatedBlocks, err := sim.sanitizeSimulatedBlocks(req.BlockStateCalls)
	if err != nil {
		return nil, err
	}
	headers, err := sim.makeHeaders(simulatedBlocks)
	if err != nil {
		return nil, err
	}

	// Iterate over each given SimulatedBlock
	intraBlockState := state.New(stateReader)
	for index, bsc := range simulatedBlocks {
		header := headers[index]
		blockResult, err := sim.simulateBlock(ctx, tx, intraBlockState, &bsc, header)
		if err != nil {
			return nil, err
		}
		simulatedBlockResults = append(simulatedBlockResults, blockResult)
	}

	return simulatedBlockResults, nil
}

type simulator struct {
	base             *types.Header
	chainConfig      *chain.Config
	engine           consensus.EngineReader
	canonicalReader  services.CanonicalReader
	logger           log.Logger
	gasCap           uint64
	returnDataLimit  int
	evmCallTimeout   time.Duration
	traceTransfers   bool
	validation       bool
	fullTransactions bool
}

func newSimulator(
	req *SimulationRequest,
	header *types.Header,
	chainConfig *chain.Config,
	engine consensus.EngineReader,
	canonicalReader services.CanonicalReader,
	logger log.Logger,
	gasCap uint64,
	returnDataLimit int,
	evmCallTimeout time.Duration,
) *simulator {
	return &simulator{
		base:             header,
		chainConfig:      chainConfig,
		engine:           engine,
		canonicalReader:  canonicalReader,
		logger:           logger,
		gasCap:           gasCap,
		returnDataLimit:  returnDataLimit,
		evmCallTimeout:   evmCallTimeout,
		traceTransfers:   req.TraceTransfers,
		validation:       req.Validation,
		fullTransactions: req.ReturnFullTransactions,
	}
}

// sanitizeSimulatedBlocks checks the integrity of the simulated input blocks, i.e. that block numbers and timestamps
// are strictly increasing, setting default values when necessary. Gaps in block numbers are filled with empty blocks.
// Note: this can modify BlockOverrides objects in simulated blocks.
func (s *simulator) sanitizeSimulatedBlocks(blocks []SimulatedBlock) ([]SimulatedBlock, error) {
	sanitizedBlocks := make([]SimulatedBlock, 0, len(blocks))
	prevNumber := s.base.Number
	prevTimestamp := s.base.Time
	for _, block := range blocks {
		if block.BlockOverrides == nil {
			block.BlockOverrides = &transactions.BlockOverrides{}
		}
		if block.BlockOverrides.BlockNumber == nil {
			nextNumber := prevNumber.Uint64() + 1
			block.BlockOverrides.BlockNumber = (*hexutil.Uint64)(&nextNumber)
		}
		blockNumber := new(big.Int).SetUint64(block.BlockOverrides.BlockNumber.Uint64())
		diff := new(big.Int).Sub(blockNumber, prevNumber)
		if diff.Cmp(common.Big0) <= 0 {
			return nil, fmt.Errorf("block numbers must be in order: %d <= %d", blockNumber, prevNumber)
		}
		if total := new(big.Int).Sub(blockNumber, s.base.Number); total.Cmp(big.NewInt(maxSimulateBlocks)) > 0 {
			return nil, fmt.Errorf("too many blocks: %d > %d", total, maxSimulateBlocks)
		}
		if diff.Cmp(big.NewInt(1)) > 0 {
			// Fill the gap with empty blocks.
			gap := new(big.Int).Sub(diff, big.NewInt(1))
			// Assign block number to the empty blocks.
			for i := uint64(0); i < gap.Uint64(); i++ {
				n := new(big.Int).Add(prevNumber, big.NewInt(int64(i+1))).Uint64()
				t := prevTimestamp + timestampIncrement
				b := SimulatedBlock{
					BlockOverrides: &transactions.BlockOverrides{
						BlockNumber: (*hexutil.Uint64)(&n),
						Time:        (*hexutil.Uint64)(&t),
					},
				}
				prevTimestamp = t
				sanitizedBlocks = append(sanitizedBlocks, b)
			}
		}
		// Only append block after filling a potential gap.
		prevNumber = blockNumber
		var timestamp uint64
		if block.BlockOverrides.Time == nil {
			timestamp = prevTimestamp + timestampIncrement
			block.BlockOverrides.Time = (*hexutil.Uint64)(&timestamp)
		} else {
			timestamp = block.BlockOverrides.Time.Uint64()
			if timestamp <= prevTimestamp {
				return nil, fmt.Errorf("block timestamps must be in order: %d <= %d", timestamp, prevTimestamp)
			}
		}
		prevTimestamp = timestamp
		sanitizedBlocks = append(sanitizedBlocks, block)
	}
	return sanitizedBlocks, nil
}

// makeHeaders makes Header objects with preliminary fields based on a simulated blocks.
// Some fields will be filled post-execution.
// Note: this assumes blocks are in order and numbers have been validated, i.e. sanitizeSimulatedBlocks has been called.
func (s *simulator) makeHeaders(blocks []SimulatedBlock) ([]*types.Header, error) {
	parent := s.base
	headers := make([]*types.Header, len(blocks))
	for bi, block := range blocks {
		if block.BlockOverrides == nil || block.BlockOverrides.BlockNumber == nil {
			return nil, errors.New("empty block number")
		}
		overrides := block.BlockOverrides

		var withdrawalsHash *common.Hash
		if s.chainConfig.IsShanghai((uint64)(*overrides.Time)) {
			withdrawalsHash = &empty.WithdrawalsHash
		}
		header := overrides.OverrideHeader(&types.Header{
			ParentHash:      parent.Hash(),
			UncleHash:       empty.UncleHash,
			ReceiptHash:     empty.ReceiptsHash,
			TxHash:          empty.TxsHash,
			Coinbase:        parent.Coinbase,
			Difficulty:      parent.Difficulty,
			GasLimit:        parent.GasLimit,
			WithdrawalsHash: withdrawalsHash,
		})
		if s.chainConfig.IsLondon(header.Number.Uint64()) {
			// In non-validation mode base fee is set to 0 if it is not overridden.
			// This is because it creates an edge case in EVM where gasPrice < baseFee.
			// Base fee could have been overridden.
			if header.BaseFee == nil {
				if s.validation {
					header.BaseFee = misc.CalcBaseFee(s.chainConfig, parent)
				} else {
					header.BaseFee = big.NewInt(0)
				}
			}
		}
		if s.chainConfig.IsCancun(header.Time) {
			var excess uint64
			if s.chainConfig.IsCancun(parent.Time) {
				excess = misc.CalcExcessBlobGas(s.chainConfig, parent, header.Time)
			}
			header.ExcessBlobGas = &excess
			parentBeaconRoot := &common.Hash{}
			/*if overrides.BeaconRoot != nil {
				parentBeaconRoot = overrides.BeaconRoot
			}*/
			header.ParentBeaconBlockRoot = parentBeaconRoot
		}
		headers[bi] = header
		parent = header
	}
	return headers, nil
}

func (s *simulator) sanitizeCall(
	args *ethapi.CallArgs,
	intraBlockState *state.IntraBlockState,
	blockContext *evmtypes.BlockContext,
	gasUsed uint64,
	globalGasCap uint64,
) error {
	if args.Nonce == nil {
		nonce, err := intraBlockState.GetNonce(args.FromOrEmpty())
		if err != nil {
			return fmt.Errorf("failed to get nonce for %s: %w", args.FromOrEmpty().Hex(), err)
		}
		args.Nonce = (*hexutil.Uint64)(&nonce)
	}
	// Let the call run wild unless explicitly specified.
	if args.Gas == nil {
		remaining := blockContext.GasLimit - gasUsed
		args.Gas = (*hexutil.Uint64)(&remaining)
	}
	if gasUsed+uint64(*args.Gas) > blockContext.GasLimit {
		return fmt.Errorf("block gas limit reached: %d >= %d", gasUsed, blockContext.GasLimit)
	}
	if args.ChainID == nil {
		args.ChainID = (*hexutil.Big)(s.chainConfig.ChainID)
	} else {
		if have := (*big.Int)(args.ChainID); have.Cmp(s.chainConfig.ChainID) != 0 {
			return fmt.Errorf("chainId does not match node's (have=%v, want=%v)", have, s.chainConfig.ChainID)
		}
	}
	if args.Gas == nil {
		gas := globalGasCap
		if gas == 0 {
			gas = uint64(math.MaxUint64 / 2)
		}
		args.Gas = (*hexutil.Uint64)(&gas)
	} else {
		if globalGasCap > 0 && globalGasCap < uint64(*args.Gas) {
			log.Warn("Caller gas above allowance, capping", "requested", args.Gas, "cap", globalGasCap)
			args.Gas = (*hexutil.Uint64)(&globalGasCap)
		}
	}
	return nil
}

func (s *simulator) simulateBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	intraBlockState *state.IntraBlockState,
	bsc *SimulatedBlock,
	header *types.Header,
) (SimulatedBlockResult, error) {
	blockHashOverrides := transactions.BlockHashOverrides{}
	txnList := make([]types.Transaction, 0, len(bsc.Calls))
	receiptList := make(types.Receipts, 0, len(bsc.Calls))
	tracer := rpchelper.NewLogTracer(s.traceTransfers, header.Number.Uint64(), common.Hash{}, common.Hash{}, 0)
	cumulativeGasUsed := uint64(0)
	cumulativeBlobGasUsed := uint64(0)

	// Override the state before execution.
	stateOverrides := bsc.StateOverrides
	if stateOverrides != nil {
		if err := stateOverrides.Override(intraBlockState); err != nil {
			return nil, err
		}
	}

	var callResults []CallResult
	for callIndex, call := range bsc.Calls {
		callResult, txn, receipt, err := s.simulateCall(ctx, tx, intraBlockState, callIndex, &call, bsc.BlockOverrides,
			blockHashOverrides, header, &cumulativeGasUsed, &cumulativeBlobGasUsed, tracer)
		if err != nil {
			return nil, err
		}
		txnList = append(txnList, txn)
		receiptList = append(receiptList, receipt)
		callResults = append(callResults, *callResult)
	}
	header.GasUsed = cumulativeGasUsed
	if s.chainConfig.IsCancun(header.Time) {
		header.BlobGasUsed = &cumulativeBlobGasUsed
	}
	var withdrawals types.Withdrawals
	if s.chainConfig.IsShanghai(header.Time) {
		withdrawals = types.Withdrawals{}
	}
	engine, ok := s.engine.(consensus.Engine)
	if !ok {
		return nil, errors.New("consensus engine reader does not support full consensus.Engine")
	}
	block, _, err := engine.FinalizeAndAssemble(s.chainConfig, header, intraBlockState, txnList, nil,
		receiptList, withdrawals, nil, nil, nil, s.logger)
	if err != nil {
		return nil, err
	}
	additionalFields := make(map[string]interface{})
	blockResult, err := ethapi.RPCMarshalBlock(block, true, s.fullTransactions, additionalFields)
	if err != nil {
		return nil, err
	}
	repairLogs(callResults, block.Hash())
	blockResult["calls"] = callResults
	return blockResult, nil
}

func (s *simulator) simulateCall(
	ctx context.Context,
	tx kv.TemporalTx,
	intraBlockState *state.IntraBlockState,
	callIndex int,
	call *ethapi.CallArgs,
	blockOverrides *transactions.BlockOverrides,
	blockHashOverrides transactions.BlockHashOverrides,
	header *types.Header,
	cumulativeGasUsed *uint64,
	cumulativeBlobGasUsed *uint64,
	logTracer *rpchelper.LogTracer,
) (*CallResult, types.Transaction, *types.Receipt, error) {
	vmConfig := vm.Config{NoBaseFee: !s.validation}
	if s.traceTransfers {
		// Transfers must be recorded as if they were logs: use a tracer that records all logs and ether transfers
		vmConfig.Tracer = logTracer.Hooks()
	}

	// Setup context, so it may be cancelled after the call has completed or in case of unmetered gas use a timeout.
	var cancel context.CancelFunc
	if s.evmCallTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.evmCallTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// Create a custom block context and apply any custom block overrides
	blockCtx := transactions.NewEVMBlockContextWithOverrides(ctx, s.engine, header, tx, s.canonicalReader, s.chainConfig,
		blockOverrides, blockHashOverrides)

	err := s.sanitizeCall(call, intraBlockState, &blockCtx, *cumulativeGasUsed, s.gasCap)
	if err != nil {
		return nil, nil, nil, err
	}

	// Prepare the transaction message
	var baseFee *uint256.Int
	if blockCtx.BaseFee != nil {
		baseFee = blockCtx.BaseFee
	} else if header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return nil, nil, nil, errors.New("header.BaseFee uint256 overflow")
		}
	}
	msg, err := call.ToMessage(s.gasCap, baseFee)
	if err != nil {
		return nil, nil, nil, err
	}
	txCtx := core.NewEVMTxContext(msg)
	txn, err := call.ToTransaction(s.gasCap, baseFee)
	if err != nil {
		return nil, nil, nil, err
	}
	intraBlockState.SetTxContext(header.Number.Uint64(), callIndex)
	logTracer.Reset(txn.Hash(), uint(callIndex))

	// Create a new instance of the EVM with necessary configuration options
	evm := vm.NewEVM(blockCtx, txCtx, intraBlockState, s.chainConfig, vmConfig)

	// Wait for the context to be done and cancel the EVM. Even if the EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	result, err := core.ApplyMessage(evm, msg, gp, true, false, s.engine)
	if err != nil {
		return nil, nil, nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, nil, nil, fmt.Errorf("execution aborted (timeout = %v)", s.evmCallTimeout)
	}
	*cumulativeGasUsed += result.GasUsed
	receipt := core.MakeReceipt(header.Number, common.Hash{}, msg, txn, *cumulativeGasUsed, result, intraBlockState, evm)
	*cumulativeBlobGasUsed += receipt.BlobGasUsed

	var logs []*types.Log
	if s.traceTransfers {
		logs = logTracer.Logs()
	} else {
		logs = receipt.Logs
	}

	callResult := CallResult{GasUsed: hexutil.Uint64(result.GasUsed)}
	callResult.Logs = make([]*types.RPCLog, 0, len(logs))
	for _, l := range logs {
		rpcLog := &types.RPCLog{
			Log: *l,
		}
		callResult.Logs = append(callResult.Logs, rpcLog)
	}
	if len(result.ReturnData) > s.returnDataLimit {
		callResult.Status = hexutil.Uint64(types.ReceiptStatusFailed)
		callResult.ReturnData = "0x"
		callResult.Error = rpc.NewJsonErrorFromErr(
			fmt.Errorf("call returned result on length %d exceeding --rpc.returndata.limit %d", len(result.ReturnData), s.returnDataLimit))
	} else {
		if result.Failed() {
			callResult.Status = hexutil.Uint64(types.ReceiptStatusFailed)
			callResult.ReturnData = "0x"
			if errors.Is(result.Err, vm.ErrExecutionReverted) {
				// If the result contains a revert reason, try to unpack and return it.
				revertError := ethapi.NewRevertError(result)
				callResult.Error = rpc.NewJsonError(rpc.ErrCodeReverted, revertError.Error(), revertError.ErrorData().(string))
			} else {
				// Otherwise, we just capture the error message.
				callResult.Error = rpc.NewJsonError(rpc.ErrCodeVMError, result.Err.Error(), "")
			}
		} else {
			// If the call was successful, we capture the return data, the gas used and logs.
			callResult.Status = hexutil.Uint64(types.ReceiptStatusSuccessful)
			callResult.ReturnData = fmt.Sprintf("0x%x", result.ReturnData)
		}
	}
	return &callResult, txn, receipt, nil
}

// repairLogs updates the block hash in the logs present in the result of a simulated block.
// This is needed because when logs are collected during execution, the block hash is not known.
func repairLogs(calls []CallResult, hash common.Hash) {
	for i := range calls {
		for j := range calls[i].Logs {
			calls[i].Logs[j].BlockHash = hash
		}
	}
}
