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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	protocolrules "github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
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
	BlockOverrides *ethapi.BlockOverrides `json:"blockOverrides,omitempty"`
	StateOverrides *ethapi.StateOverrides `json:"stateOverrides,omitempty"`
	Calls          []ethapi.CallArgs      `json:"calls"`
}

// CallResult represents the result of a single call in the simulation.
type CallResult struct {
	ReturnData string          `json:"returnData"`
	Logs       []*types.RPCLog `json:"logs"`
	GasUsed    hexutil.Uint64  `json:"gasUsed"`
	MaxUsedGas hexutil.Uint64  `json:"maxUsedGas"`
	Status     hexutil.Uint64  `json:"status"`
	Error      any             `json:"error,omitempty"`
}

// SimulatedBlockResult represents the result of the simulated calls for a single block (i.e. one SimulatedBlock).
type SimulatedBlockResult map[string]any

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
	sim := newSimulator(&req, block.Header(), chainConfig, api.dirs, api.engine(), api._txNumReader, api._blockReader, api.logger, api.GasCap, api.ReturnDataLimit, api.evmCallTimeout, commitmentHistory)
	simulatedBlocks, err := sim.sanitizeSimulatedBlocks(req.BlockStateCalls)
	if err != nil {
		return nil, err
	}
	headers, err := sim.makeHeaders(simulatedBlocks)
	if err != nil {
		return nil, err
	}

	sharedDomains, err := execctx.NewSharedDomains(ctx, tx, api.logger)
	if err != nil {
		return nil, err
	}
	defer sharedDomains.Close()
	sharedDomains.GetCommitmentContext().SetDeferBranchUpdates(false)

	// Iterate over each given SimulatedBlock
	parent := sim.base
	for index, bsc := range simulatedBlocks {
		blockResult, current, err := sim.simulateBlock(ctx, tx, sharedDomains, &bsc, headers[index], parent, headers[:index], blockNumber == latestBlockNumber)
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
	dirs              datadir.Dirs
	engine            protocolrules.EngineReader
	txNumReader       rawdbv3.TxNumsReader
	blockReader       services.FullBlockReader
	logger            log.Logger
	gasPool           *protocol.GasPool
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
	dirs datadir.Dirs,
	engine protocolrules.EngineReader,
	txNumReader rawdbv3.TxNumsReader,
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
		dirs:              dirs,
		engine:            engine,
		txNumReader:       txNumReader,
		blockReader:       blockReader,
		logger:            logger,
		gasPool:           new(protocol.GasPool).AddGas(gasCap),
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
	prevNumber := s.base.Number.Uint64()
	prevTimestamp := s.base.Time
	for _, block := range blocks {
		if block.BlockOverrides == nil {
			block.BlockOverrides = &ethapi.BlockOverrides{}
		}
		if block.BlockOverrides.Number == nil {
			nextNumber := prevNumber + 1
			block.BlockOverrides.Number = (*hexutil.Big)(new(big.Int).SetUint64(nextNumber))
		}
		blockNumber := block.BlockOverrides.Number.Uint64()
		if blockNumber <= prevNumber {
			return nil, invalidBlockNumberError(fmt.Sprintf("block numbers must be in order: %d <= %d", blockNumber, prevNumber))
		}
		if total := blockNumber - s.base.Number.Uint64(); total > maxSimulateBlocks {
			return nil, clientLimitExceededError(fmt.Sprintf("too many blocks: %d > %d", total, maxSimulateBlocks))
		}
		diff := blockNumber - prevNumber
		if diff > 1 {
			// Fill the gap with empty blocks.
			gap := diff - 1
			// Assign block number to the empty blocks.
			for i := uint64(0); i < gap; i++ {
				n := prevNumber + i + 1
				t := prevTimestamp + timestampIncrement
				b := SimulatedBlock{
					BlockOverrides: &ethapi.BlockOverrides{
						Number: (*hexutil.Big)(new(big.Int).SetUint64(n)),
						Time:   (*hexutil.Uint64)(&t),
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
		if block.BlockOverrides == nil || block.BlockOverrides.Number == nil {
			return nil, errors.New("empty block number")
		}
		overrides := block.BlockOverrides

		var withdrawalsHash *common.Hash
		if s.chainConfig.IsShanghai((uint64)(*overrides.Time)) {
			withdrawalsHash = &empty.WithdrawalsHash
		}
		var parentBeaconRoot *common.Hash
		if s.chainConfig.IsCancun((uint64)(*overrides.Time)) {
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
	baseFee *uint256.Int,
	gasUsed uint64,
	globalGasCap uint64,
) error {
	if args.Nonce == nil {
		nonce, err := intraBlockState.GetNonce(args.FromOrEmpty())
		if err != nil {
			return fmt.Errorf("failed to get nonce for %s: %w", args.FromOrEmpty(), err)
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

// diffTrackingWriter is a state.Writer delegating its write responsibilities to the inner writer while tracking
// the touched state keys.
type diffTrackingWriter struct {
	delegate    *state.Writer
	touchedKeys keysByAccount
}

type storageKeys []accounts.StorageKey
type keysByAccount map[accounts.Address]storageKeys

var _ state.StateWriter = (*diffTrackingWriter)(nil)

func newDiffTrackingWriter(tx kv.TemporalPutDel, txNum uint64) *diffTrackingWriter {
	return &diffTrackingWriter{
		delegate:    state.NewWriter(tx, nil, txNum),
		touchedKeys: make(map[accounts.Address]storageKeys),
	}
}

func (w diffTrackingWriter) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	err := w.delegate.UpdateAccountData(address, original, account)
	if err != nil {
		return err
	}
	if _, ok := w.touchedKeys[address]; !ok {
		w.touchedKeys[address] = storageKeys{}
	}
	return nil
}

func (w diffTrackingWriter) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	err := w.delegate.UpdateAccountCode(address, incarnation, codeHash, code)
	if err != nil {
		return err
	}
	if _, ok := w.touchedKeys[address]; !ok {
		w.touchedKeys[address] = storageKeys{}
	}
	return nil
}

func (w diffTrackingWriter) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	err := w.delegate.DeleteAccount(address, original)
	if err != nil {
		return err
	}
	if _, ok := w.touchedKeys[address]; !ok {
		w.touchedKeys[address] = storageKeys{}
	}
	return nil
}

func (w diffTrackingWriter) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	err := w.delegate.WriteAccountStorage(address, incarnation, key, original, value)
	if err != nil {
		return err
	}
	if _, ok := w.touchedKeys[address]; !ok {
		w.touchedKeys[address] = storageKeys{}
	}
	w.touchedKeys[address] = append(w.touchedKeys[address], key)
	return nil
}

func (w diffTrackingWriter) CreateContract(address accounts.Address) error {
	err := w.delegate.CreateContract(address)
	if err != nil {
		return err
	}
	if _, ok := w.touchedKeys[address]; !ok {
		w.touchedKeys[address] = storageKeys{}
	}
	return nil
}

func (s *simulator) simulateBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	sharedDomains *execctx.SharedDomains,
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
				header.BaseFee = uint256.NewInt(0)
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

	blockHashOverrides := ethapi.BlockHashOverrides{}
	txnList := make([]types.Transaction, 0, len(bsc.Calls))
	receiptList := make(types.Receipts, 0, len(bsc.Calls))
	tracer := rpchelper.NewLogTracer(s.traceTransfers, blockNumber, common.Hash{}, common.Hash{}, 0)
	cumulativeGasUsed := uint64(0)
	cumulativeBlobGasUsed := uint64(0)

	stateReader, minTxNum, firstMinTxNum, err := s.newStateReaderForBlock(ctx, tx, sharedDomains, blockNumber, ancestors, latest)
	if err != nil {
		return nil, nil, err
	}
	intraBlockState := state.New(stateReader)

	// Create a custom block context and apply any custom block overrides
	blockCtx := transactions.NewEVMBlockContextWithOverrides(ctx, s.engine, header, tx, s.newSimulatedCanonicalReader(ancestors), s.chainConfig,
		bsc.BlockOverrides, blockHashOverrides)
	rules := blockCtx.Rules(s.chainConfig)

	// Determine the active precompiled contracts for this block.
	activePrecompiles := vm.ActivePrecompiledContracts(rules)

	// Override the state before block execution.
	stateOverrides := bsc.StateOverrides
	var overrideDirtyAccounts map[accounts.Address]struct{}
	if stateOverrides != nil {
		if err := stateOverrides.Override(intraBlockState, activePrecompiles, rules); err != nil {
			return nil, nil, err
		}
		// Snapshot and clear dirty set so CommitBlock won't apply EIP-161 to
		// override-only accounts (they were not "touched" by any transaction).
		overrideDirtyAccounts = intraBlockState.ExtractAndClearDirty()
	}

	vmConfig := vm.Config{NoBaseFee: !s.validation}
	if s.traceTransfers {
		// Transfers must be recorded as if they were logs: use a tracer that records all logs and ether transfers
		vmConfig.Tracer = tracer.Hooks()
		intraBlockState.SetHooks(vmConfig.Tracer)
	}

	// Apply pre-transaction state modifications before block execution.
	engine, ok := s.engine.(protocolrules.Engine)
	if !ok {
		return nil, nil, errors.New("rules engine reader does not support full rules.Engine")
	}
	systemCallCustom := func(contract accounts.Address, data []byte, ibs *state.IntraBlockState, header *types.Header, constCall bool) ([]byte, error) {
		return protocol.SysCallContract(contract, data, s.chainConfig, ibs, header, engine, constCall, vmConfig)
	}
	chainReader := consensuschain.NewReader(s.chainConfig, tx, s.blockReader, s.logger)
	err = engine.Initialize(s.chainConfig, chainReader, header, intraBlockState, systemCallCustom, s.logger, vmConfig.Tracer)
	if err != nil {
		return nil, nil, err
	}
	err = intraBlockState.FinalizeTx(rules, state.NewNoopWriter())
	if err != nil {
		return nil, nil, err
	}

	stateWriter := newDiffTrackingWriter(sharedDomains.AsPutDel(tx), minTxNum)
	callResults := make([]CallResult, 0, len(bsc.Calls))
	for callIndex, call := range bsc.Calls {
		callResult, txn, receipt, err := s.simulateCall(ctx, blockCtx, intraBlockState, callIndex, &call, header,
			&cumulativeGasUsed, &cumulativeBlobGasUsed, tracer, vmConfig, activePrecompiles)
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

	var withdrawals types.Withdrawals
	if s.chainConfig.IsShanghai(header.Time) {
		withdrawals = types.Withdrawals{}
	}
	systemCall := func(contract accounts.Address, data []byte) ([]byte, error) {
		return systemCallCustom(contract, data, intraBlockState, header, false)
	}
	block, _, err := engine.FinalizeAndAssemble(s.chainConfig, header, intraBlockState, txnList, nil,
		receiptList, withdrawals, nil, systemCall, nil, s.logger)
	if err != nil {
		return nil, nil, err
	}

	if err := intraBlockState.CommitBlock(rules, stateWriter); err != nil {
		return nil, nil, fmt.Errorf("call to CommitBlock to stateWriter: %w", err)
	}
	// Write override-only accounts that CommitBlock skipped (EIP-161 disabled for these).
	if len(overrideDirtyAccounts) > 0 {
		if err := intraBlockState.CommitOverrideDirtyAccounts(rules, stateWriter, overrideDirtyAccounts); err != nil {
			return nil, nil, fmt.Errorf("committing override accounts: %w", err)
		}
	}

	if err := s.computeSimulatedStateRoot(ctx, tx, sharedDomains, bsc, block, parent, minTxNum, firstMinTxNum, stateWriter.touchedKeys, ancestors, latest); err != nil {
		return nil, nil, err
	}

	// Marshal the block in RPC format including the call results in a custom field.
	additionalFields := make(map[string]any)
	blockResult, err := ethapi.RPCMarshalBlock(block, true, s.fullTransactions, additionalFields)
	if err != nil {
		return nil, nil, err
	}
	repairLogs(callResults, block.Hash())
	blockResult["calls"] = callResults
	return blockResult, block, nil
}

// newStateReaderForBlock returns the appropriate StateReader for a simulated block, along with
// minTxNum (txNum of the current block) and firstMinTxNum (txNum of the first simulated block).
//
// For the first simulated block (len(ancestors)==0): uses HistoryReaderV3 anchored at minTxNum.
// For subsequent blocks (len(ancestors)>0): uses simulationIntraBlockStateReader which overlays
// sharedDomains.mem (state changes from previous simulation blocks) on top of the canonical base state.
func (s *simulator) newStateReaderForBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	sharedDomains *execctx.SharedDomains,
	blockNumber uint64,
	ancestors []*types.Header,
	latest bool,
) (state.StateReader, uint64, uint64, error) {
	minTxNum, err := s.txNumReader.Min(ctx, tx, blockNumber)
	if err != nil {
		return nil, 0, 0, err
	}

	// firstMinTxNum: minTxNum of the first simulated block (or this block if it's the first).
	// Used both here and for the commitment reader to anchor reads at the canonical base-parent.
	firstMinTxNum := minTxNum
	if len(ancestors) > 0 {
		firstMinTxNum, err = s.txNumReader.Min(ctx, tx, ancestors[0].Number.Uint64())
		if err != nil {
			return nil, 0, 0, err
		}
	}

	if latest {
		return state.NewReaderV3(sharedDomains.AsGetter(tx)), minTxNum, firstMinTxNum, nil
	}

	if minTxNum < state.StateHistoryStartTxNum(tx) {
		return nil, 0, 0, fmt.Errorf("%w: min tx: %d", state.PrunedError, minTxNum)
	}
	commitmentStartingTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
	if s.commitmentHistory && minTxNum < commitmentStartingTxNum {
		return nil, 0, 0, fmt.Errorf("%w: min commitment: %d, min tx: %d", state.PrunedError, commitmentStartingTxNum, minTxNum)
	}

	if len(ancestors) > 0 {
		// Multi-block simulation: overlay sharedDomains.mem (previous blocks' simulation changes)
		// on top of the canonical base state so the EVM sees the correct simulated balances.
		return newSimulationIntraBlockStateReader(tx, sharedDomains, firstMinTxNum), minTxNum, firstMinTxNum, nil
	}
	return state.NewHistoryReaderV3(tx, minTxNum), minTxNum, firstMinTxNum, nil
}

// computeSimulatedStateRoot computes the stateRoot for the simulated block and sets it on the block header.
func (s *simulator) computeSimulatedStateRoot(
	ctx context.Context,
	tx kv.TemporalTx,
	sharedDomains *execctx.SharedDomains,
	bsc *SimulatedBlock,
	block *types.Block,
	parent *types.Header,
	minTxNum, firstMinTxNum uint64,
	touchedKeys keysByAccount,
	ancestors []*types.Header,
	latest bool,
) error {
	if latest || s.commitmentHistory {
		commitTxNum := minTxNum
		if !latest {
			// In a multi-block simulation (len(ancestors) > 0) the trie is already at parent.Root
			// because the previous simulation step left it there.  Calling SeekCommitment would
			// overwrite that correct trie state with the canonical chain's state, producing a wrong stateRoot.
			if len(ancestors) == 0 {
				// First simulated block: restore the commitment state from history and seek to it.
				sharedDomains.GetCommitmentContext().SetHistoryStateReader(tx, minTxNum)
				var err error
				commitTxNum, _, err = sharedDomains.SeekCommitment(ctx, tx)
				if err != nil {
					return err
				}
			} else {
				// Subsequent simulated blocks: trie is already correct from the previous step.
				// SeekCommitment always returns commitTxNum = minTxNum - 1 (off-by-1, expected).
				commitTxNum = minTxNum - 1
			}
			// firstMinTxNum anchors both readers at the canonical base-parent state:
			// - commitmentAsOfTxNum = firstMinTxNum+1: trie branches from the base-parent commitment.
			// - plainStateAsOfTxNum = firstMinTxNum: clean sibling accounts from the base-parent state.
			sharedDomains.GetCommitmentContext().SetStateReader(
				newHistoryCommitmentOnlyReader(tx, sharedDomains, firstMinTxNum+1, firstMinTxNum),
			)
		}
		stateRoot, err := sharedDomains.ComputeCommitment(ctx, tx, false, block.NumberU64(), commitTxNum, "eth_simulateV1", nil)
		if err != nil {
			return err
		}
		block.HeaderNoCopy().Root = common.BytesToHash(stateRoot)
		return nil
	}

	// No commitment history: compute from state history if blocks are not frozen, otherwise leave root as zero.
	if s.blockReader.FrozenBlocks() == 0 {
		txNum := minTxNum + 1 + uint64(len(bsc.Calls))
		stateRoot, err := s.computeCommitmentFromStateHistory(ctx, tx, sharedDomains, touchedKeys, parent.Number.Uint64(), txNum)
		if err != nil {
			return err
		}
		s.logger.Debug("stateRoot", "root", common.Bytes2Hex(stateRoot))
		block.HeaderNoCopy().Root = common.BytesToHash(stateRoot)
	}
	return nil
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
	precompiles vm.PrecompiledContracts,
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
	msg, err := call.ToMessage(s.gasPool.Gas(), &blockCtx.BaseFee)
	if err != nil {
		return nil, nil, nil, err
	}
	msg.SetCheckGas(s.validation)
	msg.SetCheckNonce(s.validation)
	txCtx := protocol.NewEVMTxContext(msg)
	txn, err := call.ToTransaction(s.gasPool.Gas(), &blockCtx.BaseFee)
	if err != nil {
		return nil, nil, nil, err
	}
	intraBlockState.SetTxContext(header.Number.Uint64(), callIndex)
	logTracer.Reset(txn.Hash(), uint(callIndex))

	// Create a new instance of the EVM with necessary configuration options
	evm := vm.NewEVM(blockCtx, txCtx, intraBlockState, s.chainConfig, vmConfig)

	// It is possible to override precompiles with EVM bytecode or move them to another address.
	evm.SetPrecompiles(precompiles)

	// Wait for the context to be done and cancel the EVM. Even if the EVM has finished, cancelling may be done (repeatedly)
	go func() {
		<-ctx.Done()
		evm.Cancel()
	}()

	s.gasPool.AddBlobGas(msg.BlobGas())
	result, err := protocol.ApplyMessage(evm, msg, s.gasPool, true, false, s.engine)
	if err != nil {
		return nil, nil, nil, txValidationError(err)
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, nil, nil, fmt.Errorf("execution aborted (timeout = %v)", s.evmCallTimeout)
	}
	*cumulativeGasUsed += result.ReceiptGasUsed
	receipt := protocol.MakeReceipt(&header.Number, common.Hash{}, msg, txn, *cumulativeGasUsed, result, intraBlockState, evm)
	*cumulativeBlobGasUsed += receipt.BlobGasUsed

	var logs []*types.Log
	if s.traceTransfers {
		logs = logTracer.Logs()
	} else {
		logs = receipt.Logs
	}

	callResult := CallResult{GasUsed: hexutil.Uint64(result.ReceiptGasUsed), MaxUsedGas: hexutil.Uint64(result.ReceiptGasUsed)}
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
				callResult.Error = rpc.NewJsonError(revertError.ErrorCode(), revertError.Error(), revertError.ErrorData().(string))
			} else {
				// Otherwise, we just capture the error message.
				callResult.Error = rpc.NewJsonError(rpc.ErrCodeVMError, result.Err.Error(), nil)
			}
		} else {
			// If the call was successful, we capture the return data, the gas used and logs.
			callResult.Status = hexutil.Uint64(types.ReceiptStatusSuccessful)
			callResult.ReturnData = fmt.Sprintf("0x%x", result.ReturnData)
		}
	}
	// Set the sender just to make it appear in the result if it was provided in the request.
	if call.From != nil {
		txn.SetSender(accounts.InternAddress(*call.From))
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
	case errors.Is(err, protocol.ErrNonceTooHigh):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeNonceTooHigh}
	case errors.Is(err, protocol.ErrNonceTooLow):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeNonceTooLow}
	case errors.Is(err, protocol.ErrSenderNoEOA):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeSenderIsNotEOA}
	case errors.Is(err, protocol.ErrFeeCapVeryHigh):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, protocol.ErrTipVeryHigh):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, protocol.ErrTipAboveFeeCap):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, protocol.ErrFeeCapTooLow):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInvalidParams}
	case errors.Is(err, protocol.ErrInsufficientFunds):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeInsufficientFunds}
	case errors.Is(err, protocol.ErrIntrinsicGas):
		return &rpc.CustomError{Message: err.Error(), Code: rpc.ErrCodeIntrinsicGas}
	case errors.Is(err, protocol.ErrMaxInitCodeSizeExceeded):
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

// simulationStateReader implements commitmentdb.StateReader for eth_simulateV1.
//
// WithHistory()=false enables PutBranch to write modified trie branches to sd.mem.
// This is critical for multi-block simulations: after block N's ComputeCommitment,
// trie branches are folded back (stored as hashes). When block N+1 needs to re-read
// a branch that block N modified, this reader finds it in sd.mem (written by PutBranch)
// rather than falling back to the canonical parent's version (which would not include
// block N's simulation changes, causing a wrong stateRoot).
//
// Read routing:
//   - sd.GetMemBatch() hit (any domain): post-simulation value from previous or current block.
//   - CommitmentDomain miss: GetAsOf at commitmentAsOfTxNum = base-parent commitment.
//   - Other domains miss: GetAsOf at plainStateAsOfTxNum = base-parent account state.
type simulationStateReader struct {
	sd                  *execctx.SharedDomains
	roTx                kv.TemporalTx
	commitmentAsOfTxNum uint64
	plainStateAsOfTxNum uint64
}

func (r *simulationStateReader) WithHistory() bool { return false }

func (r *simulationStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *simulationStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) (enc []byte, step kv.Step, err error) {
	if v, s, ok := r.sd.GetMemBatch().GetLatest(d, plainKey); ok {
		return v, s, nil
	}
	asOf := r.plainStateAsOfTxNum
	if d == kv.CommitmentDomain {
		asOf = r.commitmentAsOfTxNum
	}
	enc, _, err = r.roTx.GetAsOf(d, plainKey, asOf)
	if err != nil {
		return nil, 0, fmt.Errorf("simulationStateReader(GetAsOf) %q: %w", d, err)
	}
	return enc, kv.Step(asOf / stepSize), nil
}

func (r *simulationStateReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	return newHistoryCommitmentOnlyReader(tx, r.sd, r.commitmentAsOfTxNum, r.plainStateAsOfTxNum)
}

func newHistoryCommitmentOnlyReader(roTx kv.TemporalTx, sd *execctx.SharedDomains, commitmentAsOfTxNum uint64, plainStateAsOfTxNum uint64) commitmentdb.StateReader {
	return &simulationStateReader{sd: sd, roTx: roTx, commitmentAsOfTxNum: commitmentAsOfTxNum, plainStateAsOfTxNum: plainStateAsOfTxNum}
}

// simulationIntraBlockStateReader is a state.StateReader for the 2nd+ block of an eth_simulateV1
// multi-block simulation.
//
// A plain HistoryReaderV3 reads from the canonical DB via GetAsOf and misses state changes written
// by previous simulation blocks (which live in sharedDomains.mem). This reader overlays those
// in-memory changes on top of the canonical base-parent state so that block N+1's EVM execution
// sees the correct simulated state from block N.
//
// Read routing:
//   - sharedDomains.GetMemBatch().GetLatest(domain, key) hit: return the simulated value.
//   - miss: GetAsOf at firstMinTxNum (canonical state at the start of the first simulated block).
type simulationIntraBlockStateReader struct {
	sd            *execctx.SharedDomains
	roTx          kv.TemporalTx
	firstMinTxNum uint64
	composite     []byte // reusable buffer for storage composite key
}

var _ state.StateReader = (*simulationIntraBlockStateReader)(nil)

func newSimulationIntraBlockStateReader(roTx kv.TemporalTx, sd *execctx.SharedDomains, firstMinTxNum uint64) *simulationIntraBlockStateReader {
	return &simulationIntraBlockStateReader{
		sd:            sd,
		roTx:          roTx,
		firstMinTxNum: firstMinTxNum,
		composite:     make([]byte, 20+32),
	}
}

// getEncoded returns the encoded value for a domain key: checks mem batch first, then falls back to GetAsOf.
func (r *simulationIntraBlockStateReader) getEncoded(domain kv.Domain, key []byte) ([]byte, error) {
	enc, _, ok := r.sd.GetMemBatch().GetLatest(domain, key)
	if ok {
		return enc, nil
	}
	enc, _, err := r.roTx.GetAsOf(domain, key, r.firstMinTxNum)
	return enc, err
}

func (r *simulationIntraBlockStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addressValue := address.Value()
	enc, err := r.getEncoded(kv.AccountsDomain, addressValue[:])
	if err != nil || len(enc) == 0 {
		return nil, err
	}
	var a accounts.Account
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, fmt.Errorf("simulationIntraBlockStateReader ReadAccountData(%x): %w", address, err)
	}
	return &a, nil
}

func (r *simulationIntraBlockStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (r *simulationIntraBlockStateReader) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addressValue := address.Value()
	keyValue := key.Value()
	r.composite = append(append(r.composite[:0], addressValue[:]...), keyValue[:]...)
	enc, err := r.getEncoded(kv.StorageDomain, r.composite)
	if err != nil {
		return uint256.Int{}, false, err
	}
	var res uint256.Int
	if len(enc) > 0 {
		(&res).SetBytes(enc)
	}
	return res, len(enc) > 0, nil
}

func (r *simulationIntraBlockStateReader) HasStorage(address accounts.Address) (bool, error) {
	addressValue := address.Value()

	// Check the RAM batch first: storage written by prior simulated blocks lives only in the
	// in-memory btree and is not yet visible via RangeAsOf(firstMinTxNum).
	if r.sd.GetMemBatch().HasPrefixInRAM(kv.StorageDomain, addressValue[:]) {
		return true, nil
	}

	to, ok := kv.NextSubtree(addressValue[:])
	if !ok {
		to = nil
	}
	it, err := r.roTx.RangeAsOf(kv.StorageDomain, addressValue[:], to, r.firstMinTxNum, order.Asc, kv.Unlim)
	if err != nil {
		return false, err
	}
	defer it.Close()
	for it.HasNext() {
		_, v, err := it.Next()
		if err != nil {
			return false, err
		}
		if len(v) != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (r *simulationIntraBlockStateReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addressValue := address.Value()
	return r.getEncoded(kv.CodeDomain, addressValue[:])
}

func (r *simulationIntraBlockStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := r.ReadAccountCode(address)
	return len(code), err
}

func (r *simulationIntraBlockStateReader) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	acc, err := r.ReadAccountData(address)
	if err != nil || acc == nil {
		return 0, err
	}
	if acc.Incarnation == 0 {
		return 0, nil
	}
	return acc.Incarnation - 1, nil
}

func (r *simulationIntraBlockStateReader) SetTrace(_ bool, _ string) {}
func (r *simulationIntraBlockStateReader) Trace() bool               { return false }
func (r *simulationIntraBlockStateReader) TracePrefix() string       { return "" }

func newSimulateStateReader(ttx, tx kv.TemporalTx, tsd, sd *execctx.SharedDomains) commitmentdb.StateReader {
	// Both commitment and account/storage/code values are read from latest state *but* on different SharedDomains instances.
	// The commitment reader uses ttx (temp DB) with tsd so in-memory simulated state is consulted first.
	// The plain state reader uses tx (outer real DB) with sd so non-modified accounts fall back to real on-chain state.
	// We use SimulateStateReader (not a plain SplitStateReader) so that Clone() propagates the new tx only to the
	// commitment reader (for warmup goroutines), keeping the plain state reader on the original outer-DB tx.
	return rpchelper.NewSimulateStateReader(
		commitmentdb.NewLatestStateReader(ttx, tsd),
		commitmentdb.NewLatestStateReader(tx, sd),
	)
}

// computeCommitmentFromStateHistory calculates the commitment root for simulated block from state history
func (s *simulator) computeCommitmentFromStateHistory(
	ctx context.Context,
	tx kv.TemporalTx,
	sd *execctx.SharedDomains,
	touched keysByAccount,
	baseBlockNum uint64,
	simMaxTxNum uint64,
) ([]byte, error) {
	replay := rpchelper.NewCommitmentReplay(s.dirs, s.txNumReader, s.logger)
	// For computing the simulated block commitment we need to:
	// - use a custom state reader which uses both the primary db (tx, sd) and temporary commitment db (ttx, tsd)
	// - touch the keys registered by diffTrackingWriter during IntraBlockState flush
	simBlockComputeCommitment := func(ctx context.Context, ttx kv.TemporalTx, tsd *execctx.SharedDomains) ([]byte, error) {
		simBlockNum := baseBlockNum + 1
		tsd.GetCommitmentCtx().SetStateReader(newSimulateStateReader(ttx, tx, tsd, sd))
		storageFullKey := make([]byte, length.Addr+length.Hash)
		for address, locations := range touched {
			addressKey := address.Value().Bytes()
			tsd.GetCommitmentCtx().TouchKey(kv.AccountsDomain, string(addressKey), nil)
			s.logger.Debug("Touch key", "domain", kv.AccountsDomain, "key", address.Value().Hex()[2:])
			for _, loc := range locations {
				locationKey := loc.Value().Bytes()
				copy(storageFullKey[:length.Addr], addressKey)
				copy(storageFullKey[length.Addr:], locationKey)
				tsd.GetCommitmentCtx().TouchKey(kv.StorageDomain, string(storageFullKey), nil)
				s.logger.Debug("Touch key", "domain", kv.StorageDomain, "key", address.Value().Hex()[2:]+loc.Value().Hex()[2:])
			}
		}

		return tsd.ComputeCommitment(ctx, ttx, false, simBlockNum, simMaxTxNum, "commitment-from-history", nil)
	}
	return replay.ComputeCustomCommitmentFromStateHistory(ctx, tx, baseBlockNum, simBlockComputeCommitment)
}
