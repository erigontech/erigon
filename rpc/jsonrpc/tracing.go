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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/jsonstream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	tracersConfig "github.com/erigontech/erigon/eth/tracers/config"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	polygontracer "github.com/erigontech/erigon/polygon/tracer"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/transactions"
)

// TraceBlockByNumber implements debug_traceBlockByNumber. Returns Geth style block traces.
func (api *PrivateDebugAPIImpl) TraceBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithNumber(blockNum), config, stream)
}

// TraceBlockByHash implements debug_traceBlockByHash. Returns Geth style block traces.
func (api *PrivateDebugAPIImpl) TraceBlockByHash(ctx context.Context, hash common.Hash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error {
	return api.traceBlock(ctx, rpc.BlockNumberOrHashWithHash(hash, true), config, stream)
}

func (api *PrivateDebugAPIImpl) traceBlock(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
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
	err = api.BaseAPI.checkPruneHistory(ctx, tx, block.NumberU64())
	if err != nil {
		stream.WriteNil()
		return err
	}

	if config == nil {
		config = &tracersConfig.TraceConfig{}
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

	ibs, blockCtx, _, rules, signer, err := transactions.ComputeBlockContext(ctx, engine, block.HeaderNoCopy(), chainConfig, api._blockReader, api._txNumReader, tx, 0)
	if err != nil {
		stream.WriteNil()
		return err
	}

	stream.WriteArrayStart()

	txns := block.Transactions()

	var borStateSyncTxn types.Transaction

	if *config.BorTraceEnabled {
		borStateSyncTxHash := bortypes.ComputeBorTxHash(block.NumberU64(), block.Hash())

		var ok bool
		if api.useBridgeReader {
			_, ok, err = api.bridgeReader.EventTxnLookup(ctx, borStateSyncTxHash)
		} else {
			_, ok, err = api._blockReader.EventLookup(ctx, tx, borStateSyncTxHash)
		}
		if err != nil {
			stream.WriteArrayEnd()
			return err
		}
		if ok {
			borStateSyncTxn = bortypes.NewBorTransaction()
			txns = append(txns, borStateSyncTxn)
		}
	}

	var gasUsed uint64
	for txnIndex, txn := range txns {
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
		ibs.SetTxContext(blockCtx.BlockNumber, txnIndex)
		msg, _ := txn.AsMessage(*signer, block.BaseFee(), rules)

		txCtx := evmtypes.TxContext{
			TxHash:     txnHash,
			Origin:     msg.From(),
			GasPrice:   msg.GasPrice(),
			BlobHashes: msg.BlobHashes(),
		}

		if isBorStateSyncTxn {
			var stateSyncEvents []*types.Message
			stateSyncEvents, err = api.stateSyncEvents(ctx, tx, block.Hash(), blockNumber, chainConfig)
			if err != nil {
				return err
			}

			var _gasUsed uint64
			_gasUsed, err = polygontracer.TraceBorStateSyncTxnDebugAPI(
				ctx,
				chainConfig,
				config,
				ibs,
				block.Hash(),
				block.NumberU64(),
				block.Time(),
				blockCtx,
				stream,
				api.evmCallTimeout,
				stateSyncEvents,
				txnIndex,
			)
			gasUsed += _gasUsed
		} else {
			var _gasUsed uint64
			_gasUsed, err = transactions.TraceTx(ctx, engine, txn, msg, blockCtx, txCtx, block.Hash(), txnIndex, ibs, config, chainConfig, stream, api.evmCallTimeout)
			gasUsed += _gasUsed
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
		if txnIndex != len(txns)-1 {
			stream.WriteMore()
		}

		if err := stream.Flush(); err != nil {
			return err
		}
	}

	if dbg.AssertEnabled {
		var refunds = true
		if config.NoRefunds != nil && *config.NoRefunds {
			refunds = false
		}

		if refunds == true && block.GasUsed() != gasUsed {
			panic(fmt.Errorf("assert: block.GasUsed() %d != gasUsed %d. blockNum=%d", block.GasUsed(), gasUsed, blockNumber))
		}
	}

	stream.WriteArrayEnd()
	if err := stream.Flush(); err != nil {
		return err
	}

	return nil
}

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *PrivateDebugAPIImpl) TraceTransaction(ctx context.Context, hash common.Hash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error {
	tx, err := api.db.BeginTemporalRo(ctx)
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
	blockNum, _, ok, err := api.txnLookup(ctx, tx, hash)
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
		if api.useBridgeReader {
			blockNum, ok, err = api.bridgeReader.EventTxnLookup(ctx, hash)
		} else {
			blockNum, ok, err = api._blockReader.EventLookup(ctx, tx, hash)
		}
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
	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNum)
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
			// bor state sync txn is appended at the end of the block
			txnIndex = block.Transactions().Len()
		} else {
			stream.WriteNil()
			return fmt.Errorf("transaction %#x not found", hash)
		}
	}
	engine := api.engine()

	ibs, blockCtx, _, rules, signer, err := transactions.ComputeBlockContext(ctx, engine, block.HeaderNoCopy(), chainConfig, api._blockReader, api._txNumReader, tx, txnIndex)
	if err != nil {
		stream.WriteNil()
		return err
	}

	if isBorStateSyncTxn {
		stateSyncEvents, err := api.stateSyncEvents(ctx, tx, block.Hash(), blockNum, chainConfig)
		if err != nil {
			return err
		}

		_, err = polygontracer.TraceBorStateSyncTxnDebugAPI(
			ctx,
			chainConfig,
			config,
			ibs,
			block.Hash(),
			blockNum,
			block.Time(),
			blockCtx,
			stream,
			api.evmCallTimeout,
			stateSyncEvents,
			txnIndex,
		)
		return err
	}

	msg, txCtx, err := transactions.ComputeTxContext(ibs, engine, rules, signer, block, chainConfig, txnIndex)
	if err != nil {
		stream.WriteNil()
		return err
	}

	// Trace the transaction and return
	_, err = transactions.TraceTx(ctx, engine, txn, msg, blockCtx, txCtx, block.Hash(), txnIndex, ibs, config, chainConfig, stream, api.evmCallTimeout)
	return err
}

// TraceCall implements debug_traceCall. Returns Geth style call traces.
func (api *PrivateDebugAPIImpl) TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error {
	dbtx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("create ro transaction: %v", err)
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return fmt.Errorf("read chain config: %v", err)
	}
	engine := api.engine()

	blockNumber, hash, isLatest, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, dbtx, api._blockReader, api.filters)
	if err != nil {
		return fmt.Errorf("get block number: %v", err)
	}

	err = api.BaseAPI.checkPruneHistory(ctx, dbtx, blockNumber)
	if err != nil {
		return err
	}

	var stateReader state.StateReader
	if config == nil || config.TxIndex == nil || isLatest {
		stateReader, err = rpchelper.CreateStateReader(ctx, dbtx, api._blockReader, blockNrOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(dbtx, blockNumber, int(*config.TxIndex), api._txNumReader)
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
	if header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return errors.New("header.BaseFee uint256 overflow")
		}
	}

	if config != nil && config.BlockOverrides != nil {
		if config.BlockOverrides.BaseFeePerGas != nil {
			overflow := baseFee.SetFromBig(config.BlockOverrides.BaseFeePerGas.ToInt())
			if overflow {
				return errors.New("BlockOverrides.BaseFee uint256 overflow")
			}
		}

		if config.BlockOverrides.BlobBaseFee != nil {
			args.MaxFeePerBlobGas = config.BlockOverrides.BlobBaseFee
		}
	}

	msg, err := args.ToMessage(api.GasCap, baseFee)
	if err != nil {
		return fmt.Errorf("convert args to msg: %v", err)
	}
	transaction, err := args.ToTransaction(api.GasCap, baseFee)
	if err != nil {
		return fmt.Errorf("convert args to msg: %v", err)
	}

	blockCtx := transactions.NewEVMBlockContext(engine, header, blockNrOrHash.RequireCanonical, dbtx, api._blockReader, chainConfig)
	if config != nil && config.BlockOverrides != nil {
		err := config.BlockOverrides.Override(blockCtx)
		if err != nil {
			return err
		}
	}
	txCtx := core.NewEVMTxContext(msg)
	// Trace the transaction and return
	_, err = transactions.TraceTx(ctx, engine, transaction, msg, blockCtx, txCtx, hash, 0, ibs, config, chainConfig, stream, api.evmCallTimeout)
	return err
}

func (api *PrivateDebugAPIImpl) TraceCallMany(ctx context.Context, bundles []Bundle, simulateContext StateContext, config *tracersConfig.TraceConfig, stream jsonstream.Stream) error {
	var (
		hash              common.Hash
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
		return errors.New("empty bundles")
	}
	empty := true
	for _, bundle := range bundles {
		if len(bundle.Transactions) != 0 {
			empty = false
		}
	}

	if empty {
		stream.WriteNil()
		return errors.New("empty bundles")
	}

	defer func(start time.Time) { log.Trace("Tracing CallMany finished", "runtime", time.Since(start)) }(time.Now())

	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, simulateContext.BlockNumber, tx, api._blockReader, api.filters)
	if err != nil {
		stream.WriteNil()
		return err
	}

	err = api.BaseAPI.checkPruneHistory(ctx, tx, blockNum)
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

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum-1)), transactionIndex, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		stream.WriteNil()
		return err
	}

	ibs := state.New(stateReader)

	header := block.Header()

	if header == nil {
		stream.WriteNil()
		return fmt.Errorf("block %d(%x) not found", blockNum, hash)
	}

	getHash := func(i uint64) (common.Hash, error) {
		if hash, ok := overrideBlockHash[i]; ok {
			return hash, nil
		}
		hash, ok, err := api._blockReader.CanonicalHash(ctx, tx, i)
		if err != nil || !ok {
			log.Debug("Can't get block hash by number", "number", i, "only-canonical", true, "err", err, "ok", ok)
			return common.Hash{}, err
		}
		return hash, nil
	}

	blockCtx = core.NewEVMBlockContext(header, getHash, api.engine(), nil /* author */, chainConfig)
	// Get a new instance of the EVM
	evm = vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})
	var arbosVersion uint64
	if chainConfig.IsArbitrum() {
		arbosVersion = types.DeserializeHeaderExtraInformation(header).ArbOSFormatVersion
	}
	rules := chainConfig.Rules(blockNum, blockCtx.Time, arbosVersion)

	// after replaying the txns, we want to overload the state
	if config.StateOverrides != nil {
		err = config.StateOverrides.Override(ibs)
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
		// do not reset ibs, because we want to keep the overrides and state change
		// ibs.Reset()
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
			transaction, err := txn.ToTransaction(api.GasCap, blockCtx.BaseFee)
			if err != nil {
				stream.WriteNil()
				return err
			}
			txCtx = core.NewEVMTxContext(msg)
			ibs := evm.IntraBlockState()
			ibs.SetTxContext(blockCtx.BlockNumber, txnIndex)
			_, err = transactions.TraceTx(ctx, api.engine(), transaction, msg, blockCtx, txCtx, block.Hash(), txnIndex, evm.IntraBlockState(), config, chainConfig, stream, api.evmCallTimeout)
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
