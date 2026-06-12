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

// Package bal regenerates EIP-7928 Block Access Lists for blocks whose stored
// copy has been pruned, by re-executing the block against historical state.
package bal

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// GetHeaderFunc returns a header by hash+number. Used for BLOCKHASH resolution.
type GetHeaderFunc = func(hash common.Hash, number uint64) (*types.Header, error)

// RederiveBlockAccessList re-derives a block's Block Access List by replaying
// the whole block — init system calls, every transaction, and finalize.
//
// The caller provides an ibs whose state reader is positioned before the
// block's init system call (history reader at txIndex -1) and which has a
// VersionMap set so reads are recorded, mirroring the block builder.
func RederiveBlockAccessList(
	ctx context.Context,
	cfg *chain.Config,
	engine rules.Engine,
	chainReader rules.ChainReader,
	stateReader state.StateReader,
	getHeader GetHeaderFunc,
	header *types.Header,
	txns types.Transactions,
	uncles []*types.Header,
	withdrawals types.Withdrawals,
	ibs *state.IntraBlockState,
	logger log.Logger,
) (types.BlockAccessList, error) {
	blockNum := header.Number.Uint64()
	balIO := &state.VersionedIO{}
	noopWriter := state.NewNoopWriter()
	ibs.SetTxContext(blockNum, -1)
	err := protocol.InitializeBlockExecution(engine, chainReader, header, cfg, ibs, noopWriter, logger, nil)
	if err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: initialize block %d: %w", blockNum, err)
	}
	balIO = balIO.Merge(ibs.TxIO())
	ibs.ResetVersionedIO()
	gasUsed := new(protocol.GasUsed)
	gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(cfg.GetMaxBlobGasPerBlock(header.Time))
	hashFn := protocol.GetHashFn(header, getHeader)
	vmCfg := vm.Config{}
	receipts := make(types.Receipts, 0, len(txns))
	for i, txn := range txns {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		ibs.SetTxContext(blockNum, i)
		evm := protocol.CreateEVM(cfg, hashFn, engine, accounts.NilAddress, ibs, header, vmCfg)
		receipt, err := protocol.ApplyTransactionWithEVM(cfg, engine, gp, ibs, noopWriter, header, txn, gasUsed, vmCfg, evm)
		if err != nil {
			return nil, fmt.Errorf("bal.RederiveBlockAccessList: replay tx %d of block %d: %w", i, blockNum, err)
		}
		balIO = balIO.Merge(ibs.TxIO())
		ibs.ResetVersionedIO()
		receipts = append(receipts, receipt)
	}
	ibs.SetTxContext(blockNum, len(txns))
	ibs.ResetVersionedIO()
	_, _, err = protocol.FinalizeBlockExecution(engine, stateReader, header, txns, uncles, noopWriter, cfg, ibs, receipts, withdrawals, chainReader, false, logger, nil)
	if err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: finalize block %d: %w", blockNum, err)
	}
	balIO = balIO.Merge(ibs.TxIO())
	return balIO.AsBlockAccessList(), nil
}
