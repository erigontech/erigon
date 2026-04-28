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
	if fromIdx < 0 {
		fromIdx = 0
	}
	if toIdx > len(txns) {
		toIdx = len(txns)
	}
	if fromIdx >= toIdx {
		return nil, nil
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
			return nil, ctx.Err()
		default:
		}
		ibs.SetTxContext(blockNum, i)
		evm := protocol.CreateEVM(cfg, hashFn, engine, accounts.NilAddress, ibs, header, vmCfg)
		_, err := protocol.ApplyTransactionWithEVM(cfg, engine, gp, ibs, noopWriter, header, txns[i], gasUsed, vmCfg, evm)
		if err != nil {
			return nil, fmt.Errorf("receipts.DeriveForRange: replay tx %d (warmup): %w", i, err)
		}
	}

	// Now execute the target range and collect receipts.
	receipts := make(types.Receipts, 0, toIdx-fromIdx)
	for i := fromIdx; i < toIdx; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
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
			return nil, fmt.Errorf("receipts.DeriveForRange: replay tx %d: %w", i, err)
		}
		if evm.Cancelled() {
			return nil, fmt.Errorf("receipts.DeriveForRange: execution aborted (context cancelled)")
		}
		receipts = append(receipts, receipt)
	}

	return receipts, nil
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

// DeriveFields populates BlockHash and FirstLogIndexWithinBlock on each receipt.
// ApplyTransactionWithEVM sets most receipt fields, but BlockHash and the
// per-receipt first-log-index need a second pass once all receipts are known.
func DeriveFields(receipts types.Receipts, blockHash common.Hash) {
	for i, receipt := range receipts {
		receipt.BlockHash = blockHash
		if len(receipt.Logs) > 0 {
			receipt.FirstLogIndexWithinBlock = uint32(receipt.Logs[0].Index)
		} else if i > 0 {
			receipt.FirstLogIndexWithinBlock = receipts[i-1].FirstLogIndexWithinBlock + uint32(len(receipts[i-1].Logs))
		}
	}
}

// DerivePriorReceipts returns receipts for transactions 0..startTxIndex-1.
// It first tries to read them from RCacheV2 (persistent receipt cache). If all
// prior receipts are cached, no replay is needed. Otherwise falls back to
// replaying via DeriveForRange.
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
) (types.Receipts, error) {
	if startTxIndex <= 0 {
		return nil, nil
	}

	// Try RCacheV2 first — read each prior receipt from the persistent cache.
	blockHash := header.Hash()
	blockNum := header.Number.Uint64()
	cached := make(types.Receipts, 0, startTxIndex)
	allCached := true
	for i := 0; i < startTxIndex && i < len(txns); i++ {
		// blockStartTxNum = firstTask.TxNum - firstTask.TxIndex, which is
		// already the first user tx's txNum (system tx has TxIndex=-1).
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
		return cached, nil
	}

	// Fall back to replay.
	return DeriveForRange(ctx, cfg, engine, header, txns, 0, startTxIndex, ibs, gp, getHeader)
}
