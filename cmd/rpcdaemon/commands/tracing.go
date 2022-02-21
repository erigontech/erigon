package commands

import (
	"context"
	"fmt"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/internal/ethapi"
	"github.com/ledgerwatch/erigon/rpc"
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
	var block *types.Block
	if number, ok := blockNrOrHash.Number(); ok {
		block, err = api.blockByRPCNumber(number, tx)
	} else if hash, ok := blockNrOrHash.Hash(); ok {
		block, err = api.blockByHashWithSenders(tx, hash)
	} else {
		return fmt.Errorf("invalid arguments; neither block nor hash specified")
	}

	if err != nil {
		stream.WriteNil()
		return err
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		stream.WriteNil()
		return err
	}

	contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
	if api.TevmEnabled {
		contractHasTEVM = ethdb.GetHasTEVM(tx)
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}

	_, blockCtx, txCtx, ibs, reader, err := transactions.ComputeTxEnv(ctx, block, chainConfig, getHeader, contractHasTEVM, ethash.NewFaker(), tx, block.Hash(), 0)
	if err != nil {
		stream.WriteNil()
		return err
	}

	signer := types.MakeSigner(chainConfig, block.NumberU64())
	stream.WriteArrayStart()
	for idx, tx := range block.Transactions() {
		select {
		default:
		case <-ctx.Done():
			stream.WriteNil()
			return ctx.Err()
		}
		ibs.Prepare(tx.Hash(), block.Hash(), idx)
		msg, _ := tx.AsMessage(*signer, block.BaseFee())

		transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream)
		_ = ibs.FinalizeTx(chainConfig.Rules(blockCtx.BlockNumber), reader)
		if idx != len(block.Transactions())-1 {
			stream.WriteMore()
		}
		stream.Flush()
	}
	stream.WriteArrayEnd()
	stream.Flush()
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
	// Retrieve the transaction and assemble its EVM context
	blockNum, ok, err := api.txnLookup(ctx, tx, hash)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	block, err := api.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}
	blockHash := block.Hash()
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
		var borTx *types.Transaction
		borTx, _, _, _, err = rawdb.ReadBorTransaction(tx, hash)

		if err != nil {
			return err
		}

		if borTx != nil {
			return nil
		}
		stream.WriteNil()
		return fmt.Errorf("transaction %#x not found", hash)
	}
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		stream.WriteNil()
		return err
	}

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}
	contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
	if api.TevmEnabled {
		contractHasTEVM = ethdb.GetHasTEVM(tx)
	}
	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, block, chainConfig, getHeader, contractHasTEVM, ethash.NewFaker(), tx, blockHash, txnIndex)
	if err != nil {
		stream.WriteNil()
		return err
	}
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream)
}

func (api *PrivateDebugAPIImpl) TraceCall(ctx context.Context, args ethapi.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, config *tracers.TraceConfig, stream *jsoniter.Stream) error {
	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		stream.WriteNil()
		return err
	}

	blockNumber, hash, err := rpchelper.GetBlockNumber(blockNrOrHash, dbtx, api.filters)
	if err != nil {
		stream.WriteNil()
		return err
	}
	var stateReader state.StateReader
	if num, ok := blockNrOrHash.Number(); ok && num == rpc.LatestBlockNumber {
		cacheView, err := api.stateCache.View(ctx, dbtx)
		if err != nil {
			return err
		}
		stateReader = state.NewCachedReader2(cacheView, dbtx)
	} else {
		stateReader = state.NewPlainState(dbtx, blockNumber)
	}
	header := rawdb.ReadHeader(dbtx, hash, blockNumber)
	if header == nil {
		stream.WriteNil()
		return fmt.Errorf("block %d(%x) not found", blockNumber, hash)
	}
	ibs := state.New(stateReader)

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
		return err
	}

	contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
	if api.TevmEnabled {
		contractHasTEVM = ethdb.GetHasTEVM(dbtx)
	}
	blockCtx, txCtx := transactions.GetEvmContext(msg, header, blockNrOrHash.RequireCanonical, dbtx, contractHasTEVM)
	// Trace the transaction and return
	return transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, config, chainConfig, stream)
}
