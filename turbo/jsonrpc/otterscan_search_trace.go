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
	"fmt"

	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

func (api *OtterscanAPIImpl) searchTraceBlock(ctx context.Context, addr common.Address, chainConfig *chain.Config, idx int, bNum uint64, results []*TransactionsWithReceipts) {
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
	blockHash, err := api._blockReader.CanonicalHash(ctx, dbtx, blockNum)
	if err != nil {
		return false, nil, err
	}

	block, err := api.blockWithSenders(ctx, dbtx, blockHash, blockNum)
	if err != nil {
		return false, nil, err
	}
	if block == nil {
		return false, nil, nil
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))
	reader, err := rpchelper.CreateHistoryStateReader(dbtx, txNumsReader, blockNum, 0, chainConfig.ChainName)
	if err != nil {
		return false, nil, err
	}
	//stateCache := shards.NewStateCache(32, 0 /* no limit */)
	//cachedReader := state.NewCachedReader(reader, stateCache)
	cachedReader := reader
	noop := state.NewNoopWriter()
	//cachedWriter := state.NewCachedWriter(noop, stateCache)
	cachedWriter := noop

	ibs := state.New(cachedReader)
	signer := types.MakeSigner(chainConfig, blockNum, block.Time())

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := api._blockReader.Header(ctx, dbtx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	engine := api.engine()

	blockReceipts, err := api.getReceipts(ctx, dbtx, block)
	if err != nil {
		return false, nil, err
	}
	header := block.Header()
	rules := chainConfig.Rules(block.NumberU64(), header.Time)
	found := false
	for idx, txn := range block.Transactions() {
		select {
		case <-ctx.Done():
			return false, nil, ctx.Err()
		default:
		}
		ibs.SetTxContext(txn.Hash(), idx)

		msg, _ := txn.AsMessage(*signer, header.BaseFee, rules)

		tracer := NewTouchTracer(searchAddr)
		BlockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil, chainConfig)
		TxContext := core.NewEVMTxContext(msg)

		vmenv := vm.NewEVM(BlockContext, TxContext, ibs, chainConfig, vm.Config{Debug: true, Tracer: tracer})
		if _, err := core.ApplyMessage(vmenv, msg, new(core.GasPool).AddGas(txn.GetGas()).AddBlobGas(txn.GetBlobGas()), true /* refunds */, false /* gasBailout */); err != nil {
			return false, nil, err
		}
		_ = ibs.FinalizeTx(rules, cachedWriter)

		if tracer.Found {
			if idx > len(blockReceipts) {
				select { // it may happen because request canceled, then return canelation error
				case <-ctx.Done():
					return false, nil, ctx.Err()
				default:
				}
				return false, nil, fmt.Errorf("requested receipt idx %d, but have only %d", idx, len(blockReceipts)) // otherwise return some error for debugging
			}
			rpcTx := NewRPCTransaction(txn, block.Hash(), blockNum, uint64(idx), block.BaseFee())
			mReceipt := ethutils.MarshalReceipt(blockReceipts[idx], txn, chainConfig, block.HeaderNoCopy(), txn.Hash(), true)
			mReceipt["timestamp"] = block.Time()
			rpcTxs = append(rpcTxs, rpcTx)
			receipts = append(receipts, mReceipt)
			found = true
		}
	}

	return found, &TransactionsWithReceipts{rpcTxs, receipts, false, false}, nil
}
