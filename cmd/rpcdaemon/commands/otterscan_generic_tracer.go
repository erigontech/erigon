package commands

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

type GenericTracer interface {
	vm.EVMLogger
	SetTransaction(tx types.Transaction)
	Found() bool
}

func (api *OtterscanAPIImpl) genericTracer(dbtx kv.Tx, ctx context.Context, blockNum, txnID uint64, txIndex int, chainConfig *chain.Config, tracer GenericTracer) error {
	if api.historyV3(dbtx) {
		ttx := dbtx.(kv.TemporalTx)
		executor := txnExecutor(ttx, chainConfig, api.engine(), api._blockReader, tracer)

		// if block number changed, calculate all related field
		header, err := api._blockReader.HeaderByNumber(ctx, ttx, blockNum)
		if err != nil {
			return err
		}
		if header == nil {
			log.Warn("[rpc] header is nil", "blockNum", blockNum)
			return nil
		}
		executor.changeBlock(header)

		txn, err := api._txnReader.TxnByIdxInBlock(ctx, ttx, blockNum, txIndex)
		if err != nil {
			return err
		}
		if txn == nil {
			log.Warn("[rpc genericTracer] tx is nil", "blockNum", blockNum, "txIndex", txIndex)
			return nil
		}
		_, _, err = executor.execTx(txnID, txIndex, txn)
		if err != nil {
			return err
		}
		return nil
	}

	reader, err := rpchelper.CreateHistoryStateReader(dbtx, blockNum, txIndex, api.historyV3(dbtx), chainConfig.ChainName)
	if err != nil {
		return err
	}
	stateCache := shards.NewStateCache(32, 0 /* no limit */)
	cachedReader := state.NewCachedReader(reader, stateCache)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, stateCache)

	ibs := state.New(cachedReader)
	signer := types.MakeSigner(chainConfig, blockNum)

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, dbtx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	engine := api.engine()
	block, err := api.blockByNumberWithSenders(dbtx, blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		return nil
	}

	header := block.Header()
	rules := chainConfig.Rules(block.NumberU64(), header.Time)
	for idx, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), idx)

		msg, _ := tx.AsMessage(*signer, header.BaseFee, rules)

		BlockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil)
		TxContext := core.NewEVMTxContext(msg)

		vmenv := vm.NewEVM(BlockContext, TxContext, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return err
		}
		_ = ibs.FinalizeTx(rules, cachedWriter)

		if tracer.Found() {
			tracer.SetTransaction(tx)
			return nil
		}
	}

	return nil
}
