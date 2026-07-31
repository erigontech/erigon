// Copyright 2026 The Erigon Authors
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

// Package receipts provides shared receipt derivation by replaying transactions.
// Used by both the RPC layer (rpc/jsonrpc/receipts) and the execution pipeline
// (execution/stagedsync) to avoid duplicating transaction replay logic.
package receipts

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// GetHeaderFunc returns a header by hash+number. Used for BLOCKHASH opcode.
type GetHeaderFunc = func(hash common.Hash, number uint64) (*types.Header, error)

// DeriveForRange replays transactions fromIdx..toIdx-1 (0-based within the block)
// against the provided IntraBlockState and returns receipts for each.
//
// The caller is responsible for:
//   - Creating the IntraBlockState with the correct state reader (history or live)
//   - Providing a GasPool with the block's gas limit
//   - Providing the GetHeader function for BLOCKHASH resolution
//
// No caching — callers wrap this with their own caching layer.
func DeriveForRange(
	ctx context.Context,
	cfg *chain.Config,
	engine rules.EngineReader,
	header *types.Header,
	txns types.Transactions,
	fromIdx int,
	toIdx int,
	ibs *state.IntraBlockState,
	gp *protocol.GasPool,
	getHeader GetHeaderFunc,
) (types.Receipts, error) {
	receipts, _, err := deriveForRange(ctx, cfg, engine, header, txns, fromIdx, toIdx, ibs, gp, getHeader)
	return receipts, err
}

func deriveForRange(
	ctx context.Context,
	cfg *chain.Config,
	engine rules.EngineReader,
	header *types.Header,
	txns types.Transactions,
	fromIdx int,
	toIdx int,
	ibs *state.IntraBlockState,
	gp *protocol.GasPool,
	getHeader GetHeaderFunc,
) (types.Receipts, *protocol.GasUsed, error) {
	if fromIdx < 0 {
		fromIdx = 0
	}
	if toIdx > len(txns) {
		toIdx = len(txns)
	}
	if fromIdx >= toIdx {
		return nil, new(protocol.GasUsed), nil
	}

	blockNum := header.Number.Uint64()
	gasUsed := new(protocol.GasUsed)
	noopWriter := state.NewNoopWriter()
	hashFn := protocol.GetHashFn(header, getHeader)
	vmCfg := vm.Config{}

	// If starting mid-block, we need to replay 0..fromIdx-1 first to get
	// cumulative gas and state to the right point. We discard those receipts.
	for i := 0; i < fromIdx; i++ {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		ibs.SetTxContext(blockNum, i)
		evm := protocol.CreateEVM(cfg, hashFn, engine, accounts.NilAddress, ibs, header, vmCfg)
		_, err := protocol.ApplyTransactionWithEVM(cfg, engine, gp, ibs, noopWriter, header, txns[i], gasUsed, vmCfg, evm)
		if err != nil {
			return nil, nil, fmt.Errorf("receipts.DeriveForRange: replay tx %d (warmup): %w", i, err)
		}
	}

	// Now execute the target range and collect receipts.
	receipts := make(types.Receipts, 0, toIdx-fromIdx)
	for i := fromIdx; i < toIdx; i++ {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		ibs.SetTxContext(blockNum, i)
		evm := protocol.CreateEVM(cfg, hashFn, engine, accounts.NilAddress, ibs, header, vmCfg)

		// Cancel watcher: abort mid-opcode if the context is cancelled
		// (e.g. RPC timeout). Without this, a gas-heavy transaction would
		// run to completion even after the caller has given up.
		txDone := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				evm.Cancel()
			case <-txDone:
			}
		}()

		receipt, err := protocol.ApplyTransactionWithEVM(cfg, engine, gp, ibs, noopWriter, header, txns[i], gasUsed, vmCfg, evm)
		close(txDone)
		if err != nil {
			return nil, nil, fmt.Errorf("receipts.DeriveForRange: replay tx %d: %w", i, err)
		}
		if evm.Cancelled() {
			return nil, nil, fmt.Errorf("receipts.DeriveForRange: execution aborted (context cancelled)")
		}
		receipts = append(receipts, receipt)
	}

	return receipts, gasUsed, nil
}

// DeriveBlockReceipts replays all transactions in a block and returns their receipts.
// Convenience wrapper around DeriveForRange(ctx, cfg, engine, header, txns, 0, len(txns), ...).
func DeriveBlockReceipts(
	ctx context.Context,
	cfg *chain.Config,
	engine rules.EngineReader,
	header *types.Header,
	txns types.Transactions,
	ibs *state.IntraBlockState,
	gp *protocol.GasPool,
	getHeader GetHeaderFunc,
) (types.Receipts, error) {
	return DeriveForRange(ctx, cfg, engine, header, txns, 0, len(txns), ibs, gp, getHeader)
}

// DeriveFields populates BlockHash, FirstLogIndexWithinBlock, and (when
// missing) Bloom on each receipt. ApplyTransactionWithEVM sets most receipt
// fields, but the first-log-index needs a second pass once all receipts are
// known, and finalize-produced or cache-read receipts arrive without a bloom.
func DeriveFields(receipts types.Receipts, blockHash common.Hash) {
	for i, receipt := range receipts {
		receipt.BlockHash = blockHash
		if receipt.Bloom.IsEmpty() && len(receipt.Logs) > 0 {
			receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		}
		if len(receipt.Logs) > 0 {
			receipt.FirstLogIndexWithinBlock = uint32(receipt.Logs[0].Index)
		} else if i > 0 {
			receipt.FirstLogIndexWithinBlock = receipts[i-1].FirstLogIndexWithinBlock + uint32(len(receipts[i-1].Logs))
		}
	}
}

func gasUsedFromCachedReceipts(cached types.Receipts, txns types.Transactions) *protocol.GasUsed {
	cumulativeGasUsed := cached.CumulativeGasUsed()
	gasUsed := &protocol.GasUsed{
		Receipt:      cumulativeGasUsed,
		BlockRegular: cumulativeGasUsed,
	}
	for _, txn := range txns {
		gasUsed.Blob += txn.GetBlobGas()
	}
	return gasUsed
}

// DerivePriorReceipts returns receipts for transactions 0..startTxIndex-1,
// together with the block gas totals they accumulated.
// Pre-Amsterdam it uses RCacheV2 when the full prefix is cached. Amsterdam
// blocks are replayed because receipts do not preserve both block-gas dimensions.
//
// Used when execution resumes mid-block from a snapshot boundary and Finalize
// needs the full receipt set for requests hash computation.
func DerivePriorReceipts(
	ctx context.Context,
	cfg *chain.Config,
	engine rules.EngineReader,
	header *types.Header,
	txns types.Transactions,
	startTxIndex int,
	blockStartTxNum uint64,
	tx kv.TemporalTx,
	ibs *state.IntraBlockState,
	gp *protocol.GasPool,
	getHeader GetHeaderFunc,
) (types.Receipts, *protocol.GasUsed, error) {
	if startTxIndex <= 0 {
		return nil, new(protocol.GasUsed), nil
	}
	if startTxIndex > len(txns) {
		return nil, nil, fmt.Errorf("start transaction index %d exceeds transaction count %d", startTxIndex, len(txns))
	}

	if !cfg.IsAmsterdam(header.Time) {
		blockHash := header.Hash()
		blockNum := header.Number.Uint64()
		cached := make(types.Receipts, 0, startTxIndex)
		allCached := true
		for i := range startTxIndex {
			txNum := blockStartTxNum + uint64(i)
			receipt, ok, err := rawdb.ReadReceiptCacheV2(tx, rawdb.RCacheV2Query{
				BlockNum:      blockNum,
				BlockHash:     blockHash,
				TxnHash:       txns[i].Hash(),
				TxNum:         txNum,
				DontCalcBloom: true,
			})
			if err != nil || !ok {
				allCached = false
				break
			}
			cached = append(cached, receipt)
		}
		if allCached && len(cached) == startTxIndex {
			return cached, gasUsedFromCachedReceipts(cached, txns[:startTxIndex]), nil
		}
	}

	return deriveForRange(ctx, cfg, engine, header, txns, 0, startTxIndex, ibs, gp, getHeader)
}
