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
	"errors"
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

// RederiveBlockAccessList replays the whole block — init system calls, every
// transaction, and finalize — and returns the versioned I/O its Block Access
// List is built from.
//
// The caller provides an ibs whose state reader is positioned before the
// block's init system call (history reader at txIndex -1) and which has a
// VersionMap set so reads are recorded, mirroring the block builder. A failure
// of the block wraps rules.ErrInvalidBlock; any other error is the node's.
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
) (*state.VersionedIO, error) {
	blockNum := header.Number.Uint64()
	balIO := &state.VersionedIO{}
	noopWriter := state.NewNoopWriter()
	var headerErr error
	hashFn := protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
		h, err := getHeader(hash, number)
		if err == nil && h == nil {
			err = fmt.Errorf("header %d %x not found", number, hash)
		}
		if err != nil && headerErr == nil {
			headerErr = err
		}
		return h, err
	})
	// phaseErr returns the error of the phase that just ran. The node's own
	// failures come first: the EVM folds failed state reads and header lookups
	// into failed calls, and the executor recovers panics into errors.
	phaseErr := func(err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if readErr := ibs.StateReadError(); readErr != nil {
			return readErr
		}
		if headerErr != nil {
			return headerErr
		}
		if errors.Is(err, protocol.ErrExecPanic) {
			return err
		}
		if err != nil {
			return fmt.Errorf("%w: %w", rules.ErrInvalidBlock, err)
		}
		return nil
	}
	blockContext := protocol.NewEVMBlockContext(header, hashFn, engine, accounts.NilAddress, cfg)
	blockRules := blockContext.Rules(cfg)
	// record folds the phase that just ran into balIO and publishes its writes
	// to the versionMap so the next phase observes them across the reset — the
	// cross-tx carrier the parallel executor and block builder also use. Without
	// it a later tx's balance write (e.g. the accumulating coinbase fee) can't
	// see the running value and the per-tx BAL change is lost.
	record := func() error {
		writes, err := ibs.FinalizedWrites(blockRules)
		if err != nil {
			return err
		}
		ibs.MergeTxIOInto(balIO, writes)
		ibs.FlushWritesToVersionMap(writes)
		ibs.ResetVersionedIO()
		return nil
	}
	ibs.SetTxContext(blockNum, -1)
	err := protocol.InitializeBlockExecution(engine, chainReader, header, cfg, ibs, noopWriter, logger, nil)
	if err = phaseErr(err); err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: initialize block %d: %w", blockNum, err)
	}
	if err := record(); err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: initialize block %d: %w", blockNum, err)
	}
	gasUsed := new(protocol.GasUsed)
	gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(cfg.GetMaxBlobGasPerBlock(header.Time))
	vmCfg := vm.Config{}
	receipts := make(types.Receipts, 0, len(txns))
	for i, txn := range txns {
		// The live executor runs RIP-7560 transactions on their own path.
		if txn.Type() == types.AccountAbstractionTxType {
			return nil, fmt.Errorf("bal.RederiveBlockAccessList: replay tx %d of block %d: account abstraction transactions are not replayed", i, blockNum)
		}
		ibs.SetTxContext(blockNum, i)
		evm := protocol.CreateEVM(cfg, hashFn, engine, accounts.NilAddress, ibs, header, vmCfg)
		stopCancelWatch := context.AfterFunc(ctx, evm.Cancel)
		receipt, err := protocol.ApplyTransactionWithEVM(cfg, engine, gp, ibs, noopWriter, header, txn, gasUsed, vmCfg, evm)
		stopCancelWatch()
		if err = phaseErr(err); err != nil {
			return nil, fmt.Errorf("bal.RederiveBlockAccessList: replay tx %d of block %d: %w", i, blockNum, err)
		}
		if err := record(); err != nil {
			return nil, fmt.Errorf("bal.RederiveBlockAccessList: replay tx %d of block %d: %w", i, blockNum, err)
		}
		receipts = append(receipts, receipt)
	}
	ibs.SetTxContext(blockNum, len(txns))
	ibs.ResetVersionedIO()
	_, _, err = protocol.FinalizeBlockExecution(engine, header, txns, uncles, noopWriter, cfg, ibs, receipts, withdrawals, chainReader, false, logger)
	if err = phaseErr(err); err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: finalize block %d: %w", blockNum, err)
	}
	finalWrites, err := ibs.FinalizedWrites(blockRules)
	if err != nil {
		return nil, fmt.Errorf("bal.RederiveBlockAccessList: finalize block %d: %w", blockNum, err)
	}
	ibs.MergeTxIOInto(balIO, finalWrites)
	return balIO, nil
}
