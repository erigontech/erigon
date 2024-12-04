package jsonrpc

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func (api *PrivateDebugAPIImpl) traceBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig_ZkEvm, stream *jsoniter.Stream) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	var (
		block    *types.Block
		number   rpc.BlockNumber
		numberOk bool
		hash     common.Hash
		hashOk   bool
	)
	if number, numberOk = blockNrOrHash.Number(); numberOk {
		block, err = api.blockByRPCNumber(ctx, number, tx)
	} else if hash, hashOk = blockNrOrHash.Hash(); hashOk {
		block, err = api.blockByHashWithSenders(ctx, tx, hash)
	} else {
		return fmt.Errorf("invalid arguments; neither block nor hash specified")
	}

	if err != nil {
		stream.WriteNil()
		return err
	}

	if block == nil {
		if numberOk {
			return fmt.Errorf("invalid arguments; block with number %d not found", number)
		}
		return fmt.Errorf("invalid arguments; block with hash %x not found", hash)
	}

	// if we've pruned this history away for this block then just return early
	// to save any red herring errors
	err = api.BaseAPI.checkPruneHistory(tx, block.NumberU64())
	if err != nil {
		stream.WriteNil()
		return err
	}

	if config == nil {
		config = &tracers.TraceConfig_ZkEvm{}
	}

	if config.BorTraceEnabled == nil {
		config.BorTraceEnabled = newBoolPtr(false)
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		stream.WriteNil()
		return err
	}

	blockTracer := &blockTracer{
		ctx:            ctx,
		stream:         stream,
		engine:         api.engine(),
		tx:             tx,
		config:         config,
		chainConfig:    chainConfig,
		_blockReader:   api._blockReader,
		historyV3:      api.historyV3(tx),
		evmCallTimeout: api.evmCallTimeout,
	}

	return blockTracer.TraceBlock(block)
}

func (api *PrivateDebugAPIImpl) TraceCallMany(ctx context.Context, bundles []Bundle, simulateContext StateContext, config *tracers.TraceConfig_ZkEvm, stream *jsoniter.Stream) error {
	var (
		hash               common.Hash
		replayTransactions types.Transactions
		evm                *vm.EVM
		blockCtx           evmtypes.BlockContext
		txCtx              evmtypes.TxContext
		overrideBlockHash  map[uint64]common.Hash
		baseFee            uint256.Int
	)

	if config == nil {
		config = &tracers.TraceConfig_ZkEvm{}
	}

	overrideBlockHash = make(map[uint64]common.Hash)
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if len(bundles) == 0 {
		stream.WriteNil()
		return fmt.Errorf("empty bundles")
	}
	empty := true
	for _, bundle := range bundles {
		if len(bundle.Transactions) != 0 {
			empty = false
		}
	}

	if empty {
		stream.WriteNil()
		return fmt.Errorf("empty bundles")
	}

	defer func(start time.Time) { log.Trace("Tracing CallMany finished", "runtime", time.Since(start)) }(time.Now())

	blockNum, hash, _, err := rpchelper.GetBlockNumber_zkevm(simulateContext.BlockNumber, tx, api.filters)
	if err != nil {
		stream.WriteNil()
		return err
	}

	err = api.BaseAPI.checkPruneHistory(tx, blockNum)
	if err != nil {
		return err
	}

	block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}

	// -1 is a default value for transaction index.
	// If it's -1, we will try to replay every single transaction in that block
	transactionIndex := -1

	if simulateContext.TransactionIndex != nil {
		transactionIndex = *simulateContext.TransactionIndex
	}

	if transactionIndex == -1 {
		transactionIndex = len(block.Transactions())
	}

	replayTransactions = block.Transactions()[:transactionIndex]

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum-1)), 0, api.filters, api.stateCache, api.historyV3(tx), chainConfig.ChainName)
	if err != nil {
		stream.WriteNil()
		return err
	}

	st := state.New(stateReader)

	parent := block.Header()

	if parent == nil {
		stream.WriteNil()
		return fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	getHash := func(i uint64) common.Hash {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash
		}
		hash, err := rawdb.ReadCanonicalHash(tx, i)
		if err != nil {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true)
		}
		return hash
	}

	if parent.BaseFee != nil {
		baseFee.SetFromBig(parent.BaseFee)
	}

	blockCtx = evmtypes.BlockContext{
		CanTransfer: core.CanTransfer,
		Transfer:    core.Transfer,
		GetHash:     getHash,
		Coinbase:    parent.Coinbase,
		BlockNumber: parent.Number.Uint64(),
		Time:        parent.Time,
		Difficulty:  new(big.Int).Set(parent.Difficulty),
		GasLimit:    parent.GasLimit,
		BaseFee:     &baseFee,
	}

	// Get a new instance of the EVM

	hermezReader := hermez_db.NewHermezDbReader(tx)

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(core.GasPool).AddGas(math.MaxUint64)
	for idx, txn := range replayTransactions {
		//evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{Debug: false})
		txHash := txn.Hash()
		evm, effectiveGasPricePercentage, err := core.PrepareForTxExecution(chainConfig, &vm.Config{}, &blockCtx, hermezReader, evm.IntraBlockState().(*state.IntraBlockState), block, &txHash, idx)
		if err != nil {
			stream.WriteNil()
			return err
		}

		if _, _, err := core.ApplyTransaction_zkevm(chainConfig, api.engine(), evm, gp, st, state.NewNoopWriter(), parent, txn, nil, effectiveGasPricePercentage, true); err != nil {
			stream.WriteNil()
			return err
		}
	}

	// after replaying the txns, we want to overload the state
	if config.StateOverrides != nil {
		err = config.StateOverrides.Override(evm.IntraBlockState().(*state.IntraBlockState))
		if err != nil {
			stream.WriteNil()
			return err
		}
	}

	stream.WriteArrayStart()
	for bundle_index, bundle := range bundles {
		stream.WriteArrayStart()
		// first change blockContext
		blockHeaderOverride(&blockCtx, bundle.BlockOverride, overrideBlockHash)
		for txn_index, txn := range bundle.Transactions {
			if txn.Gas == nil || *(txn.Gas) == 0 {
				txn.Gas = (*hexutil.Uint64)(&api.GasCap)
			}
			msg, err := txn.ToMessage(api.GasCap, blockCtx.BaseFee)
			if err != nil {
				stream.WriteNil()
				return err
			}
			txCtx = core.NewEVMTxContext(msg)
			ibs := evm.IntraBlockState().(*state.IntraBlockState)
			ibs.Init(common.Hash{}, parent.Hash(), txn_index)
			err = transactions.TraceTx(ctx, msg, blockCtx, txCtx, evm.IntraBlockState(), config, chainConfig, stream, api.evmCallTimeout)

			if err != nil {
				stream.WriteNil()
				return err
			}

			_ = ibs.FinalizeTx(evm.ChainRules(), state.NewNoopWriter())

			if txn_index < len(bundle.Transactions)-1 {
				stream.WriteMore()
			}
		}
		stream.WriteArrayEnd()

		if bundle_index < len(bundles)-1 {
			stream.WriteMore()
		}
		blockCtx.BlockNumber++
		blockCtx.Time++
	}
	stream.WriteArrayEnd()
	return nil
}

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *PrivateDebugAPIImpl) TraceTransactionCounters(ctx context.Context, hash common.Hash, config *tracers.TraceConfig_ZkEvm, stream *jsoniter.Stream) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	// Retrieve the transaction and assemble its EVM context
	blockNum, ok, err := api.txnLookup(ctx, tx, hash)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if !ok {
		stream.WriteNil()
		return nil
	}

	// check pruning to ensure we have history at this block level
	if err = api.BaseAPI.checkPruneHistory(tx, blockNum); err != nil {
		stream.WriteNil()
		return err
	}

	// Private API returns 0 if transaction is not found.
	if blockNum == 0 && chainConfig.Bor != nil {
		blockNumPtr, err := rawdb.ReadBorTxLookupEntry(tx, hash)
		if err != nil {
			stream.WriteNil()
			return err
		}
		if blockNumPtr == nil {
			stream.WriteNil()
			return nil
		}
		blockNum = *blockNumPtr
	}
	block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if block == nil {
		stream.WriteNil()
		return nil
	}
	var txnIndex uint64
	var txn types.Transaction
	for i, transaction := range block.Transactions() {
		if transaction.Hash() == hash {
			txnIndex = uint64(i)
			txn = transaction
			break
		}
	}
	if txn == nil {
		borTx := rawdb.ReadBorTransactionForBlock(tx, blockNum)
		if err != nil {
			stream.WriteNil()
			return err
		}

		if borTx != nil {
			stream.WriteNil()
			return nil
		}
		stream.WriteNil()
		return fmt.Errorf("transaction %#x not found", hash)
	}
	engine := api.engine()

	txEnv, err := transactions.ComputeTxEnv_ZkEvm(ctx, engine, block, chainConfig, api._blockReader, tx, int(txnIndex), api.historyV3(tx))
	if err != nil {
		stream.WriteNil()
		return err
	}

	// counters work
	hermezDb := hermez_db.NewHermezDbReader(tx)
	forkId, err := hermezDb.GetForkIdByBlockNum(blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}

	smtDepth, err := getSmtDepth(hermezDb, blockNum, config)
	if err != nil {
		stream.WriteNil()
		return err
	}

	txCounters := vm.NewTransactionCounter(txn, int(smtDepth), uint16(forkId), api.config.Zk.VirtualCountersSmtReduction, false)
	batchCounters := vm.NewBatchCounterCollector(int(smtDepth), uint16(forkId), api.config.Zk.VirtualCountersSmtReduction, false, nil)

	if _, err = batchCounters.AddNewTransactionCounters(txCounters); err != nil {
		stream.WriteNil()
		return err
	}

	// set tracer to counter tracer
	if config == nil {
		config = &tracers.TraceConfig_ZkEvm{}
	}
	config.CounterCollector = txCounters

	// Trace the transaction and return
	return transactions.TraceTx(ctx, txEnv.Msg, txEnv.BlockContext, txEnv.TxContext, txEnv.Ibs, config, chainConfig, stream, api.evmCallTimeout)
}

func (api *PrivateDebugAPIImpl) TraceBatchByNumber(ctx context.Context, batchNum rpc.BlockNumber, config *tracers.TraceConfig_ZkEvm, stream *jsoniter.Stream) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()

	reader := hermez_db.NewHermezDbReader(tx)
	badBatch, err := reader.GetInvalidBatch(batchNum.Uint64())
	if err != nil {
		stream.WriteNil()
		return err
	}

	if badBatch {
		stream.WriteNil()
		return errors.New("batch is invalid")
	}

	blockNumbers, err := reader.GetL2BlockNosByBatch(batchNum.Uint64())
	if err != nil {
		stream.WriteNil()
		return fmt.Errorf("failed to get block numbers for batch %d: %w", batchNum, err)
	}
	if len(blockNumbers) == 0 {
		return fmt.Errorf("no blocks found for batch %d", batchNum)
	}

	// if we've pruned this history away for this block then just return early
	// to save any red herring errors
	if err = api.BaseAPI.checkPruneHistory(tx, blockNumbers[0]); err != nil {
		stream.WriteNil()
		return err
	}

	if config == nil {
		config = &tracers.TraceConfig_ZkEvm{}
	}

	if config.BorTraceEnabled == nil {
		config.BorTraceEnabled = newBoolPtr(false)
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	blockTracer := &blockTracer{
		ctx:            ctx,
		stream:         stream,
		engine:         api.engine(),
		tx:             tx,
		config:         config,
		chainConfig:    chainConfig,
		_blockReader:   api._blockReader,
		historyV3:      api.historyV3(tx),
		evmCallTimeout: api.evmCallTimeout,
	}

	for _, blockNum := range blockNumbers {
		block, err := api.blockByNumberWithSenders(ctx, tx, blockNum)
		if err != nil {
			stream.WriteNil()
			return nil
		}

		if err := blockTracer.TraceBlock(block); err != nil {
			stream.WriteNil()
			return err
		}
	}

	return nil
}
