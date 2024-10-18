package jsonrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
	bortypes "github.com/ledgerwatch/erigon/polygon/bor/types"
	polygontracer "github.com/ledgerwatch/erigon/polygon/tracer"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

// TraceBlockByNumber implements debug_traceBlockByNumber. Returns Geth style block traces.
func (api *PrivateDebugAPIImpl) TraceBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), config, stream)
}

// TraceBlockByHash implements debug_traceBlockByHash. Returns Geth style block traces.
func (api *PrivateDebugAPIImpl) TraceBlockByHash(ctx context.Context, hash common.Hash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithHash(hash, true), config, stream)
}

func (api *PrivateDebugAPIImpl) traceBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(blockNrOrHash, tx, api.filters)
	if err != nil {
		stream.WriteNil()
		return err
	}
	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if block == nil {
		stream.WriteNil()
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
		config = &tracers.TraceConfig{}
	}

	if config.BorTraceEnabled == nil {
		var disabled bool
		config.BorTraceEnabled = &disabled
	}

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	engine := api.engine()

	txns := block.Transactions()
	var borStateSyncTxn types.Transaction
	if *config.BorTraceEnabled {
		borStateSyncTxHash := bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash())
		_, ok, err := api._blockReader.EventLookup(ctx, tx, borStateSyncTxHash)
		if err != nil {
			stream.WriteArrayEnd()
			return err
		}
		if ok {
			borStateSyncTxn = bortypes.NewBorTransaction()
			txns = append(txns, borStateSyncTxn)
		}
	}

	_, blockCtx, _, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, 0, api.historyV3(tx), borStateSyncTxn != nil)
	if err != nil {
		stream.WriteNil()
		return err
	}

	signer := types.MakeSigner(chainConfig, block.NumberU64(), block.Time())
	rules := chainConfig.Rules(block.NumberU64(), block.Time())
	stream.WriteArrayStart()

	for idx, txn := range txns {
		isBorStateSyncTxn := borStateSyncTxn == txn
		var txnHash common.Hash
		if isBorStateSyncTxn {
			txnHash = bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash())
		} else {
			txnHash = txn.Hash()
		}

		stream.WriteObjectStart()
		stream.WriteObjectField("txHash")
		stream.WriteString(txnHash.Hex())
		stream.WriteMore()
		stream.WriteObjectField("result")
		select {
		default:
		case <-ctx.Done():
			stream.WriteNil()
			stream.WriteObjectEnd()
			stream.WriteArrayEnd()
			return ctx.Err()
		}
		ibs.SetTxContext(txnHash, block.Hash(), idx)
		msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)

		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, chainConfig, ibs, block.Header(), engine, true /* constCall */)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		txCtx := evmtypes.TxContext{
			TxHash:     txnHash,
			Origin:     msg.From(),
			GasPrice:   msg.GasPrice(),
			BlobHashes: msg.BlobHashes(),
		}

		if isBorStateSyncTxn {
			// recompute blockCtx
			_, borBlockCtx, _, _, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, idx, api.historyV3(tx), true)
			if err != nil {
				return err
			}

			err = polygontracer.TraceBorStateSyncTxnDebugAPI(
				ctx,
				tx,
				chainConfig,
				config,
				ibs,
				api._blockReader,
				block.Hash(),
				block.NumberU64(),
				block.Time(),
				borBlockCtx,
				stream,
				api.evmCallTimeout,
			)
		} else {
			err = transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream, api.evmCallTimeout)
		}
		if err == nil {
			err = ibs.FinalizeTx(rules, state.NewNoopWriter())
		}

		// if we have an error we want to output valid json for it before continuing after clearing down potential writes to the stream
		if err != nil {
			stream.WriteMore()
			rpc.HandleError(err, stream)
		}

		stream.WriteObjectEnd()
		if idx != len(txns)-1 {
			stream.WriteMore()
		}

		if err := stream.Flush(); err != nil {
			return err
		}
	}

	stream.WriteArrayEnd()
	if err := stream.Flush(); err != nil {
		return err
	}

	return nil
}

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *PrivateDebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
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
	var isBorStateSyncTxn bool
	blockNum, ok, err := api.txnLookup(ctx, tx, hash)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if !ok {
		if chainConfig.Bor == nil {
			stream.WriteNil()
			return nil
		}

		// otherwise this may be a bor state sync transaction - check
		blockNum, ok, err = api._blockReader.EventLookup(ctx, tx, hash)
		if err != nil {
			stream.WriteNil()
			return err
		}
		if !ok {
			stream.WriteNil()
			return nil
		}
		if config == nil || config.BorTraceEnabled == nil || *config.BorTraceEnabled == false {
			stream.WriteEmptyArray() // matches maticnetwork/bor API behaviour for consistency
			return nil
		}

		isBorStateSyncTxn = true
	}

	// check pruning to ensure we have history at this block level
	err = api.BaseAPI.checkPruneHistory(tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
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
	var txnIndex int
	var txn types.Transaction
	for i := 0; i < block.Transactions().Len() && !isBorStateSyncTxn; i++ {
		transaction := block.Transactions()[i]
		if transaction.Hash() == hash {
			txnIndex = i
			txn = transaction
			break
		}
	}
	if txn == nil {
		if isBorStateSyncTxn {
			// bor state sync tx is appended at the end of the block
			txnIndex = block.Transactions().Len()
		} else {
			stream.WriteNil()
			return fmt.Errorf("transaction %#x not found", hash)
		}
	}
	engine := api.engine()

	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, api._blockReader, tx, txnIndex, api.historyV3(tx), isBorStateSyncTxn)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if isBorStateSyncTxn {
		return polygontracer.TraceBorStateSyncTxnDebugAPI(
			ctx,
			tx,
			chainConfig,
			config,
			ibs,
			api._blockReader,
			block.Hash(),
			blockNum,
			block.Time(),
			blockCtx,
			stream,
			api.evmCallTimeout,
		)
	}
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream, api.evmCallTimeout)
}

// TraceCall implements debug_traceCall. Returns Geth style call traces.
func (api *PrivateDebugAPIImpl) TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return fmt.Errorf("create ro transaction: %v", err)
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return fmt.Errorf("read chain config: %v", err)
	}
	engine := api.engine()

	blockNumber, hash, isLatest, err := rpchelper.GetBlockNumber(blockNrOrHash, dbtx, api.filters)
	if err != nil {
		return fmt.Errorf("get block number: %v", err)
	}

	err = api.BaseAPI.checkPruneHistory(dbtx, blockNumber)
	if err != nil {
		return err
	}

	var stateReader state.StateReader
	if config == nil || config.TxIndex == nil || isLatest {
		stateReader, err = rpchelper.CreateStateReader(ctx, dbtx, blockNrOrHash, 0, api.filters, api.stateCache, api.historyV3(dbtx), chainConfig.ChainName)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(dbtx, blockNumber, int(*config.TxIndex), api.historyV3(dbtx), chainConfig.ChainName)
	}
	if err != nil {
		return fmt.Errorf("create state reader: %v", err)
	}
	header, err := api._blockReader.Header(ctx, dbtx, hash, blockNumber)
	if err != nil {
		return fmt.Errorf("could not fetch header %d(%x): %v", blockNumber, hash, err)
	}
	if header == nil {
		return fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}
	ibs := state.New(stateReader)

	if config != nil && config.StateOverrides != nil {
		if err := config.StateOverrides.Override(ibs); err != nil {
			return fmt.Errorf("override state: %v", err)
		}
	}

	var baseFee *uint256.Int
	if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return fmt.Errorf("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(api.GasCap, baseFee)
	if err != nil {
		return fmt.Errorf("convert args to msg: %v", err)
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, dbtx, api._blockReader)
	txCtx := core.NewEVMTxContext(msg)
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream, api.evmCallTimeout)
}

func (api *PrivateDebugAPIImpl) TraceCallMany(ctx context.Context, bundles []Bundle, simulateContext StateContext, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	var (
		hash               common.Hash
		replayTransactions types.Transactions
		evm                *vm.EVM
		blockCtx           evmtypes.BlockContext
		txCtx              evmtypes.TxContext
		overrideBlockHash  map[uint64]common.Hash
	)

	if config == nil {
		config = &tracers.TraceConfig{}
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

	blockNum, hash, _, err := rpchelper.GetBlockNumber(simulateContext.BlockNumber, tx, api.filters)
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
	if block == nil {
		stream.WriteNil()
		return fmt.Errorf("block %d not found", blockNum)
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

	header := block.Header()

	if header == nil {
		stream.WriteNil()
		return fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	getHash := func(i uint64) common.Hash {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash
		}
		hash, err := api._blockReader.CanonicalHash(ctx, tx, i)
		if err != nil {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true)
		}
		return hash
	}

	blockCtx = core.NewEVMBlockContext(header, getHash, api.engine(), nil /* author */)
	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, st, chainConfig, vm.Config{Debug: false})
	signer := types.MakeSigner(chainConfig, blockNum, block.Time())
	rules := chainConfig.Rules(blockNum, blockCtx.Time)

	// Setup the gas pool (also for unmetered requests)
	// and apply the message.
	gp := new(core.GasPool).AddGas(math.MaxUint64).AddBlobGas(math.MaxUint64)
	for idx, txn := range replayTransactions {
		st.SetTxContext(txn.Hash(), block.Hash(), idx)
		msg, err := txn.AsMessage(*signer, block.BaseFee(), rules)
		if err != nil {
			stream.WriteNil()
			return err
		}
		txCtx = core.NewEVMTxContext(msg)
		evm = vm.NewEVM(blockCtx, txCtx, evm.IntraBlockState(), chainConfig, vm.Config{Debug: false})
		// Execute the transaction message
		_, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			stream.WriteNil()
			return err
		}
		_ = st.FinalizeTx(rules, state.NewNoopWriter())

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
	for bundleIndex, bundle := range bundles {
		stream.WriteArrayStart()
		// first change blockContext
		blockHeaderOverride(&blockCtx, bundle.BlockOverride, overrideBlockHash)
		for txnIndex, txn := range bundle.Transactions {
			if txn.Gas == nil || *(txn.Gas) == 0 {
				txn.Gas = (*hexutil.Uint64)(&api.GasCap)
			}
			msg, err := txn.ToMessage(api.GasCap, blockCtx.BaseFee)
			if err != nil {
				stream.WriteArrayEnd()
				stream.WriteArrayEnd()
				return err
			}
			txCtx = core.NewEVMTxContext(msg)
			ibs := evm.IntraBlockState().(*state.IntraBlockState)
			ibs.SetTxContext(common.Hash{}, header.Hash(), txnIndex)
			err = transactions.TraceTx(ctx, msg, blockCtx, txCtx, evm.IntraBlockState(), config, chainConfig, stream, api.evmCallTimeout)
			if err != nil {
				stream.WriteArrayEnd()
				stream.WriteArrayEnd()
				return err
			}

			_ = ibs.FinalizeTx(rules, state.NewNoopWriter())

			if txnIndex < len(bundle.Transactions)-1 {
				stream.WriteMore()
			}
		}
		stream.WriteArrayEnd()

		if bundleIndex < len(bundles)-1 {
			stream.WriteMore()
		}
		blockCtx.BlockNumber++
		blockCtx.Time++
	}
	stream.WriteArrayEnd()
	return nil
}
