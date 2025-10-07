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
	"math"
	"math/big"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/transactions"
)

const (
	// maxSimulateBlocks is the maximum number of blocks that can be simulated in a single request.
	maxSimulateBlocks = 256

	// timestampIncrement is the default increment between block timestamps.
	timestampIncrement = 12
)

// SimulationRequest represents the parameters for an eth_simulateV1 request.
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

// CallResult represents the result of a single call in the simulation.
type CallResult struct {
	ReturnData string          `json:"returnData"`
	Logs       []*types.RPCLog `json:"logs"`
	GasUsed    hexutil.Uint64  `json:"gasUsed"`
	Status     hexutil.Uint64  `json:"status"`
	Error      interface{}     `json:"error,omitempty"`
}

// SimulatedBlockResult represents the result of the simulated calls for a single block (i.e. one SimulatedBlock).
type SimulatedBlockResult map[string]interface{}

// SimulationResult represents the result contained in an eth_simulateV1 response.
type SimulationResult []SimulatedBlockResult

// SimulateV1 implements the eth_simulateV1 JSON-RPC method.
func (api *APIImpl) SimulateV1(ctx context.Context, req SimulationRequest, blockParameter rpc.BlockNumberOrHash) (SimulationResult, error) {
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

	blockNumber, blockHash, _, err := rpchelper.GetBlockNumber(ctx, blockParameter, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	latestBlockNumber, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return nil, err
	}
	if latestBlockNumber < blockNumber {
		return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlockNumber, blockNumber)
	}

	block, err := api.blockWithSenders(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, errors.New("header not found")
	}

	simulatedBlockResults := make(SimulationResult, 0, len(req.BlockStateCalls))

	// Check if we have commitment history: this is required to know if state root will be computed or left zero for historical state.
	commitmentHistory, _, err := rawdb.ReadDBCommitmentHistoryEnabled(tx)
	if err != nil {
		return nil, err
	}

	// Create a simulator instance to help with input sanitisation and execution of the simulated blocks.
	sim := newSimulator(&req, block.Header(), chainConfig, api.engine(), api._blockReader, api.logger, api.GasCap, api.ReturnDataLimit, api.evmCallTimeout, commitmentHistory)
	simulatedBlocks, err := sim.sanitizeSimulatedBlocks(req.BlockStateCalls)
	if err != nil {
		return nil, err
	}
	headers, err := sim.makeHeaders(simulatedBlocks)
	if err != nil {
		return nil, err
	}

	sharedDomains, err := dbstate.NewSharedDomains(tx, api.logger)
	if err != nil {
		return nil, err
	}
	defer sharedDomains.Close()

	// Iterate over each given SimulatedBlock
	parent := sim.base
	for index, bsc := range simulatedBlocks {
		blockResult, current, err := sim.simulateBlock(ctx, tx, api._txNumReader, sharedDomains, &bsc, headers[index], parent, headers[:index], blockNumber == latestBlockNumber)
		if err != nil {
			return nil, err
		}
		simulatedBlockResults = append(simulatedBlockResults, blockResult)
		headers[index] = current.Header()
		parent = current.Header()
	}

	return simulatedBlockResults, nil
}

type simulator struct {
	base              *types.Header
	chainConfig       *chain.Config
	engine            consensus.EngineReader
	blockReader       services.FullBlockReader
	logger            log.Logger
	gasPool           *core.GasPool
	returnDataLimit   int
	evmCallTimeout    time.Duration
	commitmentHistory bool
	traceTransfers    bool
	validation        bool
	fullTransactions  bool
}

func newSimulator(
	req *SimulationRequest,
	header *types.Header,
	chainConfig *chain.Config,
	engine consensus.EngineReader,
	blockReader services.FullBlockReader,
	logger log.Logger,
	gasCap uint64,
	returnDataLimit int,
	evmCallTimeout time.Duration,
	commitmentHistory bool,
) *simulator {
	return &simulator{
		base:              header,
		chainConfig:       chainConfig,
		engine:            engine,
		blockReader:       blockReader,
		logger:            logger,
		gasPool:           new(core.GasPool).AddGas(gasCap),
		returnDataLimit:   returnDataLimit,
		evmCallTimeout:    evmCallTimeout,
		commitmentHistory: commitmentHistory,
		traceTransfers:    req.TraceTransfers,
		validation:        req.Validation,
		fullTransactions:  req.ReturnFullTransactions,
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
			return nil, invalidBlockNumberError(fmt.Sprintf("block numbers must be in order: %d <= %d", blockNumber, prevNumber))
		}
		if total := new(big.Int).Sub(blockNumber, s.base.Number); total.Cmp(big.NewInt(maxSimulateBlocks)) > 0 {
			return nil, clientLimitExceededError(fmt.Sprintf("too many blocks: %d > %d", total, maxSimulateBlocks))
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
						Timestamp:   (*hexutil.Uint64)(&t),
					},
				}
				prevTimestamp = t
				sanitizedBlocks = append(sanitizedBlocks, b)
			}
		}
		// Only append block after filling a potential gap.
		prevNumber = blockNumber
		var timestamp uint64
		if block.BlockOverrides.Timestamp == nil {
			timestamp = prevTimestamp + timestampIncrement
			block.BlockOverrides.Timestamp = (*hexutil.Uint64)(&timestamp)
		} else {
			timestamp = block.BlockOverrides.Timestamp.Uint64()
			if timestamp <= prevTimestamp {
				return nil, invalidBlockTimestampError(fmt.Sprintf("block timestamps must be in order: %d <= %d", timestamp, prevTimestamp))
			}
		}
		prevTimestamp = timestamp
		sanitizedBlocks = append(sanitizedBlocks, block)
	}
	return sanitizedBlocks, nil
}

// makeHeaders makes Header objects with preliminary fields based on simulated blocks. Not all header fields are filled here:
// some of them will be filled post-simulation because dependent on the execution result, some others post-simulation of
// the parent header.
// Note: this assumes blocks are in order and numbers have been validated, i.e. sanitizeSimulatedBlocks has been called.
func (s *simulator) makeHeaders(blocks []SimulatedBlock) ([]*types.Header, error) {
	header := s.base
	headers := make([]*types.Header, len(blocks))
	for bi, block := range blocks {
		if block.BlockOverrides == nil || block.BlockOverrides.BlockNumber == nil {
			return nil, errors.New("empty block number")
		}
		overrides := block.BlockOverrides

		var withdrawalsHash *common.Hash
		if s.chainConfig.IsShanghai((uint64)(*overrides.Timestamp)) {
			withdrawalsHash = &empty.WithdrawalsHash
		}
		var parentBeaconRoot *common.Hash
		if s.chainConfig.IsCancun((uint64)(*overrides.Timestamp)) {
			parentBeaconRoot = &common.Hash{}
			if overrides.BeaconRoot != nil {
				parentBeaconRoot = overrides.BeaconRoot
			}
		}
		header = overrides.OverrideHeader(&types.Header{
			UncleHash:             empty.UncleHash,
			ReceiptHash:           empty.ReceiptsHash,
			TxHash:                empty.TxsHash,
			Coinbase:              header.Coinbase,
			Difficulty:            header.Difficulty,
			GasLimit:              header.GasLimit,
			WithdrawalsHash:       withdrawalsHash,
			ParentBeaconBlockRoot: parentBeaconRoot,
		})
		headers[bi] = header
	}
	return headers, nil
}

// sanitizeCall checks and fills missing fields in call arguments, returning an error if it cannot fix them.
func (s *simulator) sanitizeCall(
	args *ethapi.CallArgs,
	intraBlockState *state.IntraBlockState,
	blockContext *evmtypes.BlockContext,
	baseFee *big.Int,
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
		return blockGasLimitReachedError(fmt.Sprintf("block gas limit reached: %d >= %d", gasUsed, blockContext.GasLimit))
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
	if baseFee == nil {
		// If there's no base fee, then it must be a non-1559 execution
		if args.GasPrice == nil {
			args.GasPrice = new(hexutil.Big)
		}
	} else {
		// A base fee is provided, requiring 1559-type execution
		if args.MaxFeePerGas == nil {
			args.MaxFeePerGas = new(hexutil.Big)
		}
		if args.MaxPriorityFeePerGas == nil {
			args.MaxPriorityFeePerGas = new(hexutil.Big)
		}
	}
	if args.MaxFeePerBlobGas == nil && args.BlobVersionedHashes != nil {
		args.MaxFeePerBlobGas = new(hexutil.Big)
	}
	return nil
}

func (s *simulator) simulateBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	txNumReader rawdbv3.TxNumsReader,
	sharedDomains *dbstate.SharedDomains,
	bsc *SimulatedBlock,
	header *types.Header,
	parent *types.Header,
	ancestors []*types.Header,
	latest bool,
) (SimulatedBlockResult, *types.Block, error) {
	header.ParentHash = parent.Hash()
	if s.chainConfig.IsLondon(header.Number.Uint64()) {
		// In non-validation mode base fee is set to 0 if not overridden to avoid an edge case in EVM where gasPrice < baseFee.
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
	}

	blockNumber := header.Number.Uint64()

	blockHashOverrides := transactions.BlockHashOverrides{}
	txnList := make([]types.Transaction, 0, len(bsc.Calls))
	receiptList := make(types.Receipts, 0, len(bsc.Calls))
	tracer := rpchelper.NewLogTracer(s.traceTransfers, blockNumber, common.Hash{}, common.Hash{}, 0)
	cumulativeGasUsed := uint64(0)
	cumulativeBlobGasUsed := uint64(0)

	minTxNum, err := txNumReader.Min(tx, blockNumber)
	if err != nil {
		return nil, nil, err
	}
	txnIndex := len(bsc.Calls)
	txNum := minTxNum + 1 + uint64(txnIndex)
	sharedDomains.SetBlockNum(blockNumber)
	sharedDomains.SetTxNum(txNum)

	var stateReader state.StateReader
	if latest {
		stateReader = state.NewReaderV3(sharedDomains.AsGetter(tx))
	} else {
		var err error
		stateReader, err = rpchelper.CreateHistoryStateReader(tx, blockNumber, txnIndex, txNumReader)
		if err != nil {
			return nil, nil, err
		}

		commitmentStartingTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
		if s.commitmentHistory && txNum < commitmentStartingTxNum {
			return nil, nil, state.PrunedError
		}

		sharedDomains.GetCommitmentContext().SetLimitReadAsOfTxNum(txNum, false)
		if err := sharedDomains.SeekCommitment(context.Background(), tx); err != nil {
			return nil, nil, err
		}
	}
	intraBlockState := state.New(stateReader)

	// Override the state before block execution.
	stateOverrides := bsc.StateOverrides
	if stateOverrides != nil {
		if err := stateOverrides.Override(intraBlockState); err != nil {
			return nil, nil, err
		}
		intraBlockState.SoftFinalise()
	}

	vmConfig := vm.Config{NoBaseFee: !s.validation}
	if s.traceTransfers {
		// Transfers must be recorded as if they were logs: use a tracer that records all logs and ether transfers
		vmConfig.Tracer = tracer.Hooks()
	}

	// Apply pre-transaction state modifications before block execution.
	engine, ok := s.engine.(consensus.Engine)
	if !ok {
		return nil, nil, errors.New("consensus engine reader does not support full consensus.Engine")
	}
	systemCallCustom := func(contract common.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		return core.SysCallContract(contract, data, s.chainConfig, ibs, header, engine, constCall, vmConfig)
	}
	chainReader := consensuschain.NewReader(s.chainConfig, tx, s.blockReader, s.logger)
	engine.Initialize(s.chainConfig, chainReader, header, intraBlockState, systemCallCustom, s.logger, vmConfig.Tracer)
	intraBlockState.SoftFinalise()

	// Create a custom block context and apply any custom block overrides
	blockCtx := transactions.NewEVMBlockContextWithOverrides(ctx, s.engine, header, tx, s.newSimulatedCanonicalReader(ancestors), s.chainConfig,
		bsc.BlockOverrides, blockHashOverrides)
	if bsc.BlockOverrides.BlobBaseFee != nil {
		blockCtx.BlobBaseFee = bsc.BlockOverrides.BlobBaseFee.ToUint256()
	}
	rules := blockCtx.Rules(s.chainConfig)

	stateWriter := state.NewWriter(sharedDomains.AsPutDel(tx), nil, sharedDomains.TxNum())
	callResults := make([]CallResult, 0, len(bsc.Calls))
	for callIndex, call := range bsc.Calls {
		callResult, txn, receipt, err := s.simulateCall(ctx, blockCtx, intraBlockState, callIndex, &call, header,
			&cumulativeGasUsed, &cumulativeBlobGasUsed, tracer, vmConfig)
		if err != nil {
			return nil, nil, err
		}
		txnList = append(txnList, txn)
		receiptList = append(receiptList, receipt)
		callResults = append(callResults, *callResult)
		err = intraBlockState.FinalizeTx(rules, stateWriter)
		if err != nil {
			return nil, nil, err
		}
	}
	header.GasUsed = cumulativeGasUsed
	if s.chainConfig.IsCancun(header.Time) {
		header.BlobGasUsed = &cumulativeBlobGasUsed
	}

	if err := intraBlockState.CommitBlock(rules, stateWriter); err != nil {
		return nil, nil, fmt.Errorf("call to CommitBlock to stateWriter: %w", err)
	}

	// Compute the state root for execution on the latest state and also on the historical state if commitment history is present.
	if latest || s.commitmentHistory {
		stateRoot, err := sharedDomains.ComputeCommitment(ctx, tx, false, header.Number.Uint64(), txNum, "eth_simulateV1", nil)
		if err != nil {
			return nil, nil, err
		}
		header.Root = common.BytesToHash(stateRoot)
	} else {
		// We cannot compute the state root for historical state w/o commitment history, so we just use the zero hash (default value).
	}

	var withdrawals types.Withdrawals
	if s.chainConfig.IsShanghai(header.Time) {
		withdrawals = types.Withdrawals{}
	}
	systemCall := func(contract common.Address, data []byte) ([]byte, error) {
		return systemCallCustom(contract, data, intraBlockState, header, false)
	}
	block, _, err := engine.FinalizeAndAssemble(s.chainConfig, header, intraBlockState, txnList, nil,
		receiptList, withdrawals, nil, systemCall, nil, s.logger)
	if err != nil {
		return nil, nil, err
	}
	// Marshal the block in RPC format including the call results in a custom field.
	additionalFields := make(map[string]interface{})
	blockResult, err := ethapi.RPCMarshalBlock(block, true, s.fullTransactions, additionalFields)
	if err != nil {
		return nil, nil, err
	}
	repairLogs(callResults, block.Hash())
	blockResult["calls"] = callResults
	return blockResult, block, nil
}

// simulateCall simulates a single call in the EVM using the given intra-block state and possibly tracing transfers.
func (s *simulator) simulateCall(
	ctx context.Context,
	blockCtx evmtypes.BlockContext,
	intraBlockState *state.IntraBlockState,
	callIndex int,
	call *ethapi.CallArgs,
	header *types.Header,
	cumulativeGasUsed *uint64,
	cumulativeBlobGasUsed *uint64,
	logTracer *rpchelper.LogTracer,
	vmConfig vm.Config,
) (*CallResult, types.Transaction, *types.Receipt, error) {
	// Setup context, so it may be cancelled after the call has completed or in case of unmetered gas use a timeout.
	var cancel context.CancelFunc
	if s.evmCallTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.evmCallTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	err := s.sanitizeCall(call, intraBlockState, &blockCtx, header.BaseFee, *cumulativeGasUsed, s.gasPool.Gas())
	if err != nil {
		return nil, nil, nil, err
	}

	// Prepare the transaction message
	msg, err := call.ToMessage(s.gasPool.Gas(), blockCtx.BaseFee)
	if err != nil {
		return nil, nil, nil, err
	}
	txCtx := core.NewEVMTxContext(msg)
	txn, err := call.ToTransaction(s.gasPool.Gas(), blockCtx.BaseFee)
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

	// Treat gas and blob gas as part of the same pool.
	err = s.gasPool.AddBlobGas(msg.BlobGas()).SubGas(msg.BlobGas())
	if err != nil {
		return nil, nil, nil, err
	}
	result, err := core.ApplyMessage(evm, msg, s.gasPool, true, false, s.engine)
	if err != nil {
		return nil, nil, nil, txValidationError(err)
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
			Log:            *l,
			BlockTimestamp: header.Time,
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
	// Set the sender just to make it appear in the result if it was provided in the request.
	if call.From != nil {
		txn.SetSender(*call.From)
	}
	return &callResult, txn, receipt, nil
}

type simulatedCanonicalReader struct {
	canonicalReader services.CanonicalReader
	headers         []*types.Header
}

func (s *simulatedCanonicalReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockNum uint64) (common.Hash, bool, error) {
	hash, ok, err := s.canonicalReader.CanonicalHash(ctx, tx, blockNum)
	if err == nil && ok {
		return hash, true, nil
	}
	for _, header := range s.headers {
		if header.Number.Uint64() == blockNum {
			return header.Hash(), true, nil
		}
	}
	return common.Hash{}, false, errors.New("header not found")
}

func (s *simulatedCanonicalReader) IsCanonical(context.Context, kv.Getter, common.Hash, uint64) (bool, error) {
	return true, nil
}

func (s *simulatedCanonicalReader) BadHeaderNumber(context.Context, kv.Getter, common.Hash) (blockHeight *uint64, err error) {
	return nil, errors.New("bad header not found")
}

func (s *simulator) newSimulatedCanonicalReader(headers []*types.Header) services.CanonicalReader {
	return &simulatedCanonicalReader{s.blockReader, headers}
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

// txValidationError maps errors from core.ApplyMessage to appropriate JSON-RPC errors.
func txValidationError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, core.ErrNonceTooHigh):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeNonceTooHigh}
	case errors.Is(err, core.ErrNonceTooLow):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeNonceTooLow}
	case errors.Is(err, core.ErrSenderNoEOA):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeSenderIsNotEOA}
	case errors.Is(err, core.ErrFeeCapVeryHigh):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, core.ErrTipVeryHigh):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, core.ErrTipAboveFeeCap):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, core.ErrFeeCapTooLow):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, core.ErrInsufficientFunds):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInsufficientFunds}
	case errors.Is(err, core.ErrIntrinsicGas):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeIntrinsicGas}
	case errors.Is(err, core.ErrMaxInitCodeSizeExceeded):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeMaxInitCodeSizeExceeded}
	}
	return &rpc.CustomError{
		Message: err.Error(),
		Code:    rpc.ErrCodeInternalError,
	}
}

func invalidBlockNumberError(message string) error {
	return &rpc.CustomError{Message: message, Code: rpc.ErrCodeBlockNumberInvalid}
}

func invalidBlockTimestampError(message string) error {
	return &rpc.CustomError{Message: message, Code: rpc.ErrCodeBlockTimestampInvalid}
}

func blockGasLimitReachedError(message string) error {
	return &rpc.CustomError{Message: message, Code: rpc.ErrCodeBlockGasLimitReached}
}

func clientLimitExceededError(message string) error {
	return &rpc.CustomError{Message: message, Code: rpc.ErrCodeClientLimitExceeded}
}
