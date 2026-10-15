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
	receipts, err := ReplayBlockPrefix(ctx, cfg, engine, chainReader, getHeader, header, txns, ibs, balIO, logger)
	if err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: %w", err)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	ibs.SetTxContext(blockNum, len(txns))
	ibs.ResetVersionedIO()
	_, _, err = protocol.FinalizeBlockExecution(engine, header, txns, uncles, state.NewNoopWriter(), cfg, ibs, receipts, withdrawals, chainReader, false, logger)
	if err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: finalize block %d: %w", blockNum, err)
	}
	if err := ibs.StateReadError(); err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: finalize block %d: %w", blockNum, err)
	}
	ibs.MergeTxIOInto(balIO, ibs.FinalizedWrites(blockRules(cfg, engine, header)))
	return balIO.AsBlockAccessList(), nil
}

// ReplayBlockPrefix replays the block-start system calls and txns on ibs,
// folding each phase's reads and writes into io at its access index, and
// returns the receipts of txns. ibs is set up as for RederiveBlockAccessList.
func ReplayBlockPrefix(
	ctx context.Context,
	cfg *chain.Config,
	engine rules.Engine,
	chainReader rules.ChainReader,
	getHeader GetHeaderFunc,
	header *types.Header,
	txns types.Transactions,
	ibs *state.IntraBlockState,
	io *state.VersionedIO,
	logger log.Logger,
) (types.Receipts, error) {
	blockNum := header.Number.Uint64()
	noopWriter := state.NewNoopWriter()
	hashFn := protocol.GetHashFn(header, getHeader)
	chainRules := blockRules(cfg, engine, header)
	// record folds the phase that just ran into io and publishes its writes to
	// the version map, the cross-tx carrier the parallel executor and block
	// builder also use: without it a later tx's balance write (e.g. the
	// accumulating coinbase fee) can't see the running value.
	record := func() error {
		if err := ibs.StateReadError(); err != nil {
			return err
		}
		writes := ibs.FinalizedWrites(chainRules)
		ibs.MergeTxIOInto(io, writes)
		ibs.FlushWritesToVersionMap(writes)
		ibs.ResetVersionedIO()
		return nil
	}
	ibs.SetTxContext(blockNum, -1)
	if err := protocol.InitializeBlockExecution(engine, chainReader, header, cfg, ibs, noopWriter, logger, nil); err != nil {
		return nil, fmt.Errorf("initialize block %d: %w", blockNum, err)
	}
	if err := record(); err != nil {
		return nil, fmt.Errorf("initialize block %d: %w", blockNum, err)
	}
	gasUsed := new(protocol.GasUsed)
	gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(cfg.GetMaxBlobGasPerBlock(header.Time))
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
		stopCancelWatch := context.AfterFunc(ctx, evm.Cancel)
		receipt, err := protocol.ApplyTransactionWithEVM(cfg, engine, gp, ibs, noopWriter, header, txn, gasUsed, vmCfg, evm)
		stopCancelWatch()
		if err != nil {
			return nil, fmt.Errorf("replay tx %d of block %d: %w", i, blockNum, err)
		}
		if evm.Cancelled() {
			return nil, fmt.Errorf("execution aborted replaying tx %d of block %d: %w", i, blockNum, ctx.Err())
		}
		if err := record(); err != nil {
			return nil, fmt.Errorf("replay tx %d of block %d: %w", i, blockNum, err)
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

func blockRules(cfg *chain.Config, engine rules.EngineReader, header *types.Header) *chain.Rules {
	blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, nil), engine, accounts.NilAddress, cfg)
	return blockContext.Rules(cfg)
}
