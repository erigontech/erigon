package commands

import (
	"context"
	"sync"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

func (api *OtterscanAPIImpl) searchTraceBlock(ctx context.Context, wg *sync.WaitGroup, addr common.Address, chainConfig *chain.Config, idx int, bNum uint64, results []*TransactionsWithReceipts) {
	defer wg.Done()

	// Trace block for Txs
	newdbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		log.Error("Search trace error", "err", err)
		results[idx] = nil
		return
	}
	defer newdbtx.Rollback()

	_, result, err := api.traceBlock(newdbtx, ctx, bNum, addr, chainConfig)
	if err != nil {
		log.Error("Search trace error", "err", err)
		results[idx] = nil
		return
	}
	results[idx] = result
}

func (api *OtterscanAPIImpl) traceBlock(dbtx kv.Tx, ctx context.Context, blockNum uint64, searchAddr common.Address, chainConfig *chain.Config) (bool, *TransactionsWithReceipts, error) {
	rpcTxs := make([]*RPCTransaction, 0)
	receipts := make([]map[string]interface{}, 0)

	// Retrieve the transaction and assemble its EVM context
	blockHash, err := rawdb.ReadCanonicalHash(dbtx, blockNum)
	if err != nil {
		return false, nil, err
	}

	block, senders, err := api._blockReader.BlockWithSenders(ctx, dbtx, blockHash, blockNum)
	if err != nil {
		return false, nil, err
	}

	reader, err := rpchelper.CreateHistoryStateReader(dbtx, blockNum, 0, api.historyV3(dbtx), chainConfig.ChainName)
	if err != nil {
		return false, nil, err
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

	blockReceipts := rawdb.ReadReceipts(dbtx, block, senders)
	header := block.Header()
	excessDataGas := header.ParentExcessDataGas(getHeader)
	rules := chainConfig.Rules(block.NumberU64(), header.Time)
	found := false
	for idx, tx := range block.Transactions() {
		ibs.SetTxContext(tx.Hash(), block.Hash(), idx)

		msg, _ := tx.AsMessage(*signer, header.BaseFee, rules)

		tracer := NewTouchTracer(searchAddr)
		BlockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil, excessDataGas)
		TxContext := core.NewEVMTxContext(msg)

		vmenv := vm.NewEVM(BlockContext, TxContext, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(tx.GetGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return false, nil, err
		}
		_ = ibs.FinalizeTx(rules, cachedWriter)

		if tracer.Found {
			rpcTx := newRPCTransaction(tx, block.Hash(), blockNum, uint64(idx), block.BaseFee())
			mReceipt := marshalReceipt(blockReceipts[idx], tx, chainConfig, block.HeaderNoCopy(), tx.Hash(), true, excessDataGas)
			mReceipt["timestamp"] = block.Time()
			rpcTxs = append(rpcTxs, rpcTx)
			receipts = append(receipts, mReceipt)
			found = true
		}
	}

	return found, &TransactionsWithReceipts{rpcTxs, receipts, false, false}, nil
}
