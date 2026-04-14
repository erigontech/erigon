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
		receipt, err := protocol.ApplyTransactionWithEVM(cfg, engine, gp, ibs, noopWriter, header, txns[i], gasUsed, vmCfg, evm)
		if err != nil {
			return nil, fmt.Errorf("receipts.DeriveForRange: replay tx %d: %w", i, err)
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

// DerivePriorReceipts replays transactions 0..startTxIndex-1 and returns their
// receipts. Used when execution resumes mid-block from a snapshot boundary and
// Finalize needs the full receipt set for requests hash computation.
func DerivePriorReceipts(
	ctx context.Context,
	cfg *chain.Config,
	engine rules.EngineReader,
	header *types.Header,
	txns types.Transactions,
	startTxIndex int,
	ibs *state.IntraBlockState,
	gp *protocol.GasPool,
	getHeader GetHeaderFunc,
) (types.Receipts, error) {
	return DeriveForRange(ctx, cfg, engine, header, txns, 0, startTxIndex, ibs, gp, getHeader)
}
