// Copyright 2024 The Erigon Authors
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
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/jsonstream/ethjson"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

// errPendingNotSupported prevents committed tracing from silently substituting
// the latest executed block for pending. Committed tracing has no pending block,
// so pending is an invalid parameter.
var errPendingNotSupported = &rpc.InvalidParamsError{Message: "tracing on top of pending is not supported"}

func rejectPendingNumber(blockNr rpc.BlockNumber) error {
	if blockNr == rpc.PendingBlockNumber {
		return errPendingNotSupported
	}
	return nil
}

func rejectPending(blockNrOrHash rpc.BlockNumberOrHash) error {
	if blockNrOrHash.BlockNumber == nil {
		return nil
	}
	return rejectPendingNumber(*blockNrOrHash.BlockNumber)
}

// TraceBlockByNumber implements debug_traceBlockByNumber. Returns Geth style block traces.
func (api *DebugAPIImpl) TraceBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, config *tracersConfig.TraceConfig, stream *jsonstream.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), config, stream)
}

// TraceBlockByHash implements debug_traceBlockByHash. Returns Geth style block traces.
func (api *DebugAPIImpl) TraceBlockByHash(ctx context.Context, hash common.Hash, config *tracersConfig.TraceConfig, stream *jsonstream.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithHash(hash, true), config, stream)
}

func (api *DebugAPIImpl) traceBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, config *tracersConfig.TraceConfig, stream *jsonstream.Stream) error {
	if err := rejectPending(blockNrOrHash); err != nil {
		return err
	}
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader)
	if err != nil {
		return err
	}

	if (blockNrOrHash.BlockHash == nil && hash == common.Hash{}) {
		return fmt.Errorf("block #%d not found", *blockNrOrHash.BlockNumber)
	}

	if blockNumber == 0 {
		return fmt.Errorf("genesis is not traceable")
	}

	// if we've pruned this history away for this block then just return early
	// to save any red herring errors
	err = api.BaseAPI.checkBlockHistoryAvailable(ctx, tx, blockNumber)
	if err != nil {
		return err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return err
	}
	if block == nil {
		return fmt.Errorf("invalid arguments; block with hash %x not found", hash)
	}

	if config == nil {
		config = &tracersConfig.TraceConfig{}
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return err
	}
	engine := api.engine()

	err = rpchelper.CheckBlockExecuted(tx, blockNumber)
	if err != nil {
		return err
	}

	ibs, blockCtx, stateReader, rules, signer, err := transactions.ComputeBlockContext(ctx, engine, block.HeaderNoCopy(), chainConfig, api._blockReader, nil, api._txNumReader, tx, 0)
	if err != nil {
		return err
	}
	defer ibs.Close()
	if hsr, ok := stateReader.(state.HistoricalStateReader); ok && canTraceTxnsInParallel(config) && !api.remoteBlocks {
		if workers := debugTraceBlockWorkers(block.Transactions().Len()); workers > 1 {
			err := api.traceBlockParallel(ctx, tx, block, hsr.GetTxNum(), config, chainConfig, rules, signer, stream, workers)
			if !errors.Is(err, errNoReplayReadTx) {
				return err
			}
		}
	}
	transactions.VersionedCallState(ibs)
	api.shareCodeByHash(ibs)

	var precompiles vm.PrecompiledContracts
	if config.BlockOverrides != nil {
		if err := config.BlockOverrides.Override(&blockCtx); err != nil {
			return err
		}
	}
	if config.StateOverrides != nil {
		rules = blockCtx.Rules(chainConfig)
		precompiles = vm.ActivePrecompiledContracts(rules)
		if err := config.StateOverrides.Override(ibs, precompiles, rules); err != nil {
			return err
		}
	}

	stream.WriteArrayStart()

	txns := block.Transactions()
	firstTxIndex := ibs.TxnIndex()

	var gasUsed protocol.GasUsed
	for txnIndex := range txns {
		if err := ctx.Err(); err != nil {
			return err
		}
		ibs.SetTxContext(blockCtx.BlockNumber, firstTxIndex+txnIndex)
		usage := api.writeTxnTrace(ctx, stream, block, txnIndex, ibs, blockCtx, rules, signer, config, chainConfig, precompiles)
		gasUsed.BlockExecution += usage.BlockExecutionGasUsed
		gasUsed.BlockState += usage.BlockStateGasUsed

		// Trace output can fail after execution. Finalize even then so the next
		// transaction starts with its own storage originals, journal and refunds.
		if err := ethapi.FinalizeCallTx(ibs, rules); err != nil {
			return err
		}

		if err := stream.Flush(); err != nil { // Client can use result of 1 tx-trace
			return err
		}
	}

	assertTracedBlockGas(block, config, &gasUsed)
	stream.WriteArrayEnd()
	return nil
}

// writeTxnTrace writes the {txHash, result} object of the txn at txnIndex and returns its gas.
// A transaction's error is answered inside its own object; the block trace goes on.
func (api *DebugAPIImpl) writeTxnTrace(ctx context.Context, s *jsonstream.Stream, block *types.Block, txnIndex int, ibs *state.IntraBlockState,
	blockCtx evmtypes.BlockContext, rules *chain.Rules, signer *types.Signer, config *tracersConfig.TraceConfig, chainConfig *chain.Config,
	precompiles vm.PrecompiledContracts,
) (usage mdgas.TxnGasUsage) {
	txn := block.Transactions()[txnIndex]
	txnHash := txn.Hash()
	s.WriteObjectStart()
	ethjson.Data(s, "txHash", txnHash[:])
	_ = rpc.WriteFieldOrError(s, "result", func(s *jsonstream.Stream) error {
		msg, err := txn.AsMessage(*signer, block.BaseFee(), rules)
		if err != nil {
			return fmt.Errorf("convert transaction %s to message: %w", txnHash, err)
		}
		txCtx := evmtypes.TxContext{
			TxHash:     txnHash,
			Origin:     msg.From(),
			GasPrice:   *msg.GasPrice(),
			BlobHashes: msg.BlobHashes(),
		}
		usage, err = transactions.TraceTx(ctx, api.engine(), txn, msg, blockCtx, txCtx, &block.HeaderNoCopy().Number, block.Hash(), txnIndex, ibs, config, chainConfig, s, api.evmCallTimeout, precompiles)
		return err
	})
	s.WriteObjectEnd()
	return usage
}

// canTraceTxnsInParallel reports whether config lets every txn be traced on the canonical
// history state before it: the options that change the state one txn leaves to the next
// need a replay on one state.
func canTraceTxnsInParallel(config *tracersConfig.TraceConfig) bool {
	return config.Tracer != nil && config.StateOverrides == nil && config.BlockOverrides == nil &&
		(config.NoRefunds == nil || !*config.NoRefunds)
}

func assertTracedBlockGas(block *types.Block, config *tracersConfig.TraceConfig, gasUsed *protocol.GasUsed) {
	if !dbg.AssertEnabled || (config.NoRefunds != nil && *config.NoRefunds) {
		return
	}
	if blockGasUsed := gasUsed.BlockGasUsed(); block.GasUsed() != blockGasUsed {
		panic(fmt.Errorf("assert: block.GasUsed() %d != gasUsed %d. blockNum=%d", block.GasUsed(), blockGasUsed, block.NumberU64()))
	}
}

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *DebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *tracersConfig.TraceConfig, stream *jsonstream.Stream) error {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return err
	}
	// Retrieve the transaction and assemble its EVM context
	blockNum, txNum, ok, err := api.txnLookup(ctx, tx, hash)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("transaction not found")
	}

	if blockNum == 0 {
		return fmt.Errorf("genesis is not traceable")
	}

	err = api.BaseAPI.checkPruneBlocks(ctx, tx, blockNum)
	if err != nil {
		return err
	}

	block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		stream.WriteNil()
		return nil
	}

	txnIndex, err := api.txnIndexInBlock(ctx, tx, blockNum, txNum)
	if err != nil {
		return err
	}
	if txnIndex >= block.Transactions().Len() {
		return fmt.Errorf("transaction %#x not found", hash)
	}
	if err := api.checkPruneTransactionHistoryAtIndex(ctx, tx, blockNum, uint64(txnIndex)); err != nil {
		return err
	}
	engine := api.engine()

	ibs, blockCtx, _, rules, signer, err := transactions.ComputeBlockContext(ctx, engine, block.HeaderNoCopy(), chainConfig, api._blockReader, nil, api._txNumReader, tx, txnIndex)
	if err != nil {
		return err
	}
	defer ibs.Close()
	transactions.VersionedCallState(ibs)
	api.shareCodeByHash(ibs)

	// The state is built from history at txnIndex, so the earlier transactions of
	// the block never ran and the log counter they left has to be handed in.
	firstLogIndex, err := rawtemporaldb.FirstLogIndex(tx, txNum, txnIndex)
	if err != nil {
		return err
	}
	ibs.ResumeLogIndexAt(firstLogIndex)

	var precompiles vm.PrecompiledContracts
	if config != nil {
		if config.BlockOverrides != nil {
			if err := config.BlockOverrides.Override(&blockCtx); err != nil {
				return err
			}
		}
		if config.StateOverrides != nil {
			rules = blockCtx.Rules(chainConfig)
			precompiles = vm.ActivePrecompiledContracts(rules)
		}
	}

	msg, txCtx, err := transactions.ComputeTxContext(ibs, rules, signer, block, txnIndex)
	if err != nil {
		return err
	}
	txCtx.TxHash = hash
	// The overrides take the transaction's index, so the transaction runs after them.
	if config != nil && config.StateOverrides != nil {
		if err := config.StateOverrides.Override(ibs, precompiles, rules); err != nil {
			return err
		}
	}

	// Trace the transaction and return
	_, err = transactions.TraceTx(ctx, engine, block.Transactions()[txnIndex], msg, blockCtx, txCtx, &block.HeaderNoCopy().Number, block.Hash(), txnIndex, ibs, config, chainConfig, stream, api.evmCallTimeout, precompiles)
	return err
}

type traceCallState struct {
	header *types.Header
	reader state.StateReader
}

// Resolve and validate again if setup renews the remote view. Only setup may be
// retried: tracing writes to the response stream and cannot be safely replayed.
func prepareTraceCallState[I int | hexutil.Uint](ctx context.Context, api *DebugAPIImpl, tx kv.TemporalTx, block rpc.BlockNumberOrHash, txIndex *I) (traceCallState, error) {
	return readWithStableTxView(ctx, tx, func() (traceCallState, error) {
		var result traceCallState
		blockNumber, hash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, block, tx, canonicalViewReader{api._blockReader})
		if err != nil {
			return result, fmt.Errorf("get block number: %w", err)
		}
		if err := rpchelper.CheckBlockExecuted(tx, blockNumber); err != nil {
			return result, err
		}
		var minTxNum uint64
		var index int
		if txIndex != nil {
			minTxNum, index, err = checkTxIndexInBlock(ctx, api.BaseAPI, tx, blockNumber, *txIndex)
			if err != nil {
				return result, err
			}
		}
		postState := txIndex == nil || index == -1
		if postState {
			err = api.checkPruneState(ctx, tx, blockNumber)
		} else {
			err = api.checkPruneTransactionHistoryAtIndex(ctx, tx, blockNumber, uint64(index))
		}
		if err != nil {
			return result, err
		}
		if postState {
			result.reader, err = rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, tx, blockNumber, isLatest, -1, api._txNumReader)
		} else {
			// User transaction zero reads just after the initial system transaction.
			txNum := minTxNum + uint64(index) + 1
			result.reader, err = rpchelper.CreateHistoryStateReaderAtTxNum(ctx, tx, blockNumber, txNum, api._txNumReader)
		}
		if err != nil {
			return result, fmt.Errorf("create state reader: %w", err)
		}
		result.header, err = api.headerByHashAndNumber(ctx, tx, hash, blockNumber)
		if err != nil {
			return result, fmt.Errorf("could not fetch header %d(%x): %w", blockNumber, hash, err)
		}
		if result.header == nil {
			return result, fmt.Errorf("block %d(%x) not found", blockNumber, hash)
		}
		return result, nil
	})
}

// TraceCall implements debug_traceCall. Returns Geth style call traces.
func (api *DebugAPIImpl) TraceCall(ctx context.Context, args ethapi.CallArgs, requestedBlock *rpc.BlockNumberOrHash, config *tracersConfig.TraceConfig, stream *jsonstream.Stream) error {
	blockNrOrHash := blockOrLatest(requestedBlock)
	if err := rejectPending(blockNrOrHash); err != nil {
		return err
	}
	dbtx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("create ro transaction: %w", err)
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return fmt.Errorf("read chain config: %w", err)
	}
	if err := ethapi.CheckChainID(args.ChainID, chainConfig.ChainID); err != nil {
		return err
	}
	engine := api.engine()

	var txIndex *hexutil.Uint
	if config != nil {
		txIndex = config.TxIndex
	}
	prepared, err := prepareTraceCallState(ctx, api, dbtx, blockNrOrHash, txIndex)
	if err != nil {
		return err
	}
	header := prepared.header
	ibs := state.New(prepared.reader)
	defer ibs.Close()
	transactions.VersionedCallState(ibs)

	baseFee := overrideBaseFee(config, header.BaseFee)

	msg, err := args.ToMessage(api.GasCap, baseFee)
	if err != nil {
		return fmt.Errorf("convert args to msg: %w", err)
	}
	msg.SetSkipExecutionGasCap(true)
	transaction, err := args.ToTransaction(api.GasCap, baseFee)
	if err != nil {
		return fmt.Errorf("convert args to msg: %w", err)
	}

	var precompiles vm.PrecompiledContracts

	getHash := transactions.MakeBlockHashProvider(ctx, dbtx, canonicalViewReader{api._blockReader}, nil)
	blockCtx := protocol.NewEVMBlockContext(header, getHash, engine, accounts.NilAddress, chainConfig)
	if config != nil {
		if config.BlockOverrides != nil {
			err := config.BlockOverrides.Override(&blockCtx)
			if err != nil {
				return err
			}
		}

		// Override the state before block execution.
		if config.StateOverrides != nil {
			rules := blockCtx.Rules(chainConfig)
			precompiles = vm.ActivePrecompiledContracts(rules)
			if err := config.StateOverrides.Override(ibs, precompiles, rules); err != nil {
				return err
			}
		}
	}

	args.ZeroUnpricedBlobBaseFee(&blockCtx)
	txCtx := protocol.NewEVMTxContext(msg)
	// Trace the transaction and return
	_, err = transactions.TraceTx(ctx, engine, transaction, msg, blockCtx, txCtx, nil, common.Hash{}, 0, ibs, config, chainConfig, stream, api.evmCallTimeout, precompiles)
	return err
}

// TraceCallMany implements debug_traceCallMany and returns Geth-style call traces.
// A transaction index of -1 selects post-block state, including the final system transaction.
func (api *DebugAPIImpl) TraceCallMany(ctx context.Context, bundles []Bundle, simulateContext StateContext, config *tracersConfig.TraceConfig, stream *jsonstream.Stream) error {
	if err := requireBlockSelector(simulateContext.BlockNumber); err != nil {
		return err
	}
	if err := rejectPending(simulateContext.BlockNumber); err != nil {
		return err
	}
	var (
		evm               *vm.EVM
		blockCtx          evmtypes.BlockContext
		txCtx             evmtypes.TxContext
		overrideBlockHash map[uint64]common.Hash
	)

	if config == nil {
		config = &tracersConfig.TraceConfig{}
	}

	overrideBlockHash = make(map[uint64]common.Hash)
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateBundles(bundles, chainConfig.ChainID); err != nil {
		return err
	}

	defer func(start time.Time) { log.Trace("Tracing CallMany finished", "runtime", time.Since(start)) }(time.Now())

	prepared, err := prepareTraceCallState(ctx, api, tx, simulateContext.BlockNumber, simulateContext.TransactionIndex)
	if err != nil {
		return err
	}
	header := prepared.header
	ibs := state.New(prepared.reader)
	defer ibs.Close()
	transactions.VersionedCallState(ibs)

	getHash := transactions.MakeBlockHashProvider(ctx, tx, canonicalViewReader{api._blockReader}, overrideBlockHash)

	blockCtx = protocol.NewEVMBlockContext(header, getHash, api.engine(), accounts.NilAddress /* author */, chainConfig)
	// Apply global block overrides as the baseline for all bundles.
	// Use OverrideBlockContext (not Override) so that BlockHash overrides are also merged
	// into overrideBlockHash and honoured by the getHash closure above.
	if config.BlockOverrides != nil {
		config.BlockOverrides.OverrideBlockContext(&blockCtx, overrideBlockHash)
	}
	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})
	rules := evm.ChainRules()

	// after replaying the txns, we want to overload the state
	if config.StateOverrides != nil {
		err = config.StateOverrides.Override(ibs, vm.ActivePrecompiledContracts(rules), rules)
		if err != nil {
			return err
		}
	}

	stream.WriteArrayStart()
	// Every transaction of the request gets the next index, so each sees the ones before it.
	txIndex := ibs.TxnIndex()
	for _, bundle := range bundles {
		stream.WriteArrayStart()
		// first change block context
		bundle.BlockOverride.OverrideBlockContext(&blockCtx, overrideBlockHash)
		precompiles, err := config.StateOverrides.ActivePrecompiles(blockCtx.Rules(chainConfig))
		if err != nil {
			return err
		}
		// A bundle is a block of its own, so its logs number from zero. Only the
		// logs are reset: the overrides and state changes have to survive.
		ibs.ResetLogs()
		for txnIndex := range bundle.Transactions {
			txn := &bundle.Transactions[txnIndex]
			if txn.Gas == nil || *txn.Gas == 0 {
				txn.Gas = (*hexutil.Uint64)(&api.GasCap)
			}
			msg, err := txn.ToMessage(api.GasCap, &blockCtx.BaseFee)
			if err != nil {
				return err
			}
			msg.SetSkipExecutionGasCap(true)
			transaction, err := txn.ToTransaction(api.GasCap, &blockCtx.BaseFee)
			if err != nil {
				return err
			}
			txCtx = protocol.NewEVMTxContext(msg)
			ibs := evm.IntraBlockState()
			ibs.SetTxContext(blockCtx.BlockNumber, txIndex)
			txIndex++
			_, err = transactions.TraceTx(ctx, api.engine(), transaction, msg, blockCtx, txCtx, nil, common.Hash{}, txnIndex, evm.IntraBlockState(), config, chainConfig, stream, api.evmCallTimeout, precompiles)
			if err != nil {
				return err
			}

			if err := ethapi.FinalizeCallTx(ibs, rules); err != nil {
				return err
			}

		}
		stream.WriteArrayEnd()

		blockCtx.BlockNumber++
		blockCtx.Time++
	}
	stream.WriteArrayEnd()
	return nil
}
