// Copyright 2025 The Erigon Authors
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

package poc

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// ExecBlockResult holds the proof-of-execution results for one block.
type ExecBlockResult struct {
	BlockNum         uint64
	TxCount          int
	TotalOps         uint64
	ExecHashes       []common.Hash // one per transaction
	TransitionHashes []common.Hash // one per transaction (zero if disabled)
	TxOps            []uint64      // opcodes per transaction
	Elapsed          time.Duration
}

// ExecRunner re-executes historical transactions through the EVM with an
// ExecHasher attached, producing execution hashes for each transaction.
type ExecRunner struct {
	logger      log.Logger
	db          kv.TemporalRoDB
	blockReader services.FullBlockReader
	chainConfig *chain.Config
	engine      rules.EngineReader
}

// NewExecRunner creates a runner for re-executing historical transactions.
// If engine is nil, a minimal engine using misc.Transfer is assumed (suitable
// for post-merge chains where Author() is read from the header coinbase).
func NewExecRunner(
	logger log.Logger,
	db kv.TemporalRoDB,
	blockReader services.FullBlockReader,
	chainConfig *chain.Config,
	engine rules.EngineReader,
) *ExecRunner {
	return &ExecRunner{
		logger:      logger,
		db:          db,
		blockReader: blockReader,
		chainConfig: chainConfig,
		engine:      engine,
	}
}

// RunBlocks re-executes all transactions in [fromBlock, toBlock] and returns
// per-block execution hash results. Each transaction gets its own ExecHasher
// attached to the EVM.
func (r *ExecRunner) RunBlocks(ctx context.Context, fromBlock, toBlock uint64) ([]ExecBlockResult, error) {
	tx, err := r.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tnr := r.blockReader.TxnumReader()
	if toBlock == 0 {
		toBlock, _, err = tnr.Last(tx)
		if err != nil {
			return nil, err
		}
	}

	r.logger.Info("exec runner: starting", "fromBlock", fromBlock, "toBlock", toBlock)

	results := make([]ExecBlockResult, 0, toBlock-fromBlock+1)
	var totalOps uint64

	for block := fromBlock; block <= toBlock; block++ {
		if ctx.Err() != nil {
			return results, ctx.Err()
		}

		br, err := r.processBlock(ctx, tx, tnr, block)
		if err != nil {
			return results, fmt.Errorf("block %d: %w", block, err)
		}
		results = append(results, br)
		totalOps += br.TotalOps

		if block%1000 == 0 || block == toBlock {
			r.logger.Info("exec runner: progress",
				"block", block,
				"txs", br.TxCount,
				"ops", br.TotalOps,
				"totalOps", totalOps,
				"elapsed", br.Elapsed,
			)
		}
	}

	r.logger.Info("exec runner: finished",
		"blocks", len(results),
		"totalOps", totalOps,
	)
	return results, nil
}

// processBlock re-executes all transactions in a single block.
func (r *ExecRunner) processBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	tnr rawdbv3.TxNumsReader,
	blockNum uint64,
) (ExecBlockResult, error) {
	start := time.Now()

	// Read the full block with senders.
	block, err := r.blockReader.BlockByNumber(ctx, tx, blockNum)
	if err != nil {
		return ExecBlockResult{}, fmt.Errorf("read block: %w", err)
	}
	if block == nil {
		return ExecBlockResult{BlockNum: blockNum}, nil // no block at this number
	}

	header := block.HeaderNoCopy()
	txns := block.Transactions()
	if len(txns) == 0 {
		return ExecBlockResult{
			BlockNum: blockNum,
			Elapsed:  time.Since(start),
		}, nil
	}

	// Create state reader positioned before the first transaction in this block.
	stateReader, err := rpchelper.CreateHistoryStateReader(ctx, tx, blockNum, -1, tnr)
	if err != nil {
		return ExecBlockResult{}, fmt.Errorf("create state reader: %w", err)
	}
	ibs := state.New(stateReader)

	// Block context
	getHash := protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
		h, err := r.blockReader.Header(ctx, tx, hash, number)
		return h, err
	})

	blockCtx := protocol.NewEVMBlockContext(header, getHash, r.engine, accounts.NilAddress, r.chainConfig)
	chainRules := blockCtx.Rules(r.chainConfig)
	signer := types.MakeSigner(r.chainConfig, blockNum, header.Time)

	// Create a single EVM and reuse it across transactions in this block.
	evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, r.chainConfig, vm.Config{})

	execHasher := vm.GetExecHasher()
	defer vm.PutExecHasher(execHasher)

	blobGas := r.chainConfig.GetMaxBlobGasPerBlock(header.Time)
	gp := protocol.NewGasPool(header.GasLimit, blobGas)

	result := ExecBlockResult{
		BlockNum:         blockNum,
		ExecHashes:       make([]common.Hash, 0, len(txns)),
		TransitionHashes: make([]common.Hash, 0, len(txns)),
		TxOps:            make([]uint64, 0, len(txns)),
	}

	transitionHasher := vm.GetTransitionHasher()
	defer vm.PutTransitionHasher(transitionHasher)

	noopWriter := state.NewNoopWriter()

	for i, txn := range txns {
		ibs.SetTxContext(blockNum, i)

		msg, err := txn.AsMessage(*signer, header.BaseFee, chainRules)
		if err != nil {
			return result, fmt.Errorf("tx %d: as message: %w", i, err)
		}

		txCtx := protocol.NewEVMTxContext(msg)
		evm.Reset(txCtx, ibs)

		// Attach exec hasher and transition hasher for this transaction.
		execHasher.Reset()
		evm.SetExecHasher(execHasher)
		transitionHasher.Reset()
		evm.SetTransitionHasher(transitionHasher)

		execResult, err := protocol.ApplyMessage(evm, msg, gp, true, false, r.engine)
		if err != nil {
			return result, fmt.Errorf("tx %d: apply: %w", i, err)
		}

		evm.SetExecHasher(nil)
		evm.SetTransitionHasher(nil)

		// Exec hash and transition hash are finalized inside TransitionDb
		// and stored on the ExecutionResult.
		result.ExecHashes = append(result.ExecHashes, execResult.ExecHash)
		result.TransitionHashes = append(result.TransitionHashes, execResult.TransitionHash)
		result.TxOps = append(result.TxOps, execResult.ExecOps)
		result.TotalOps += execResult.ExecOps
		result.TxCount++

		if err := ibs.FinalizeTx(chainRules, noopWriter); err != nil {
			return result, fmt.Errorf("tx %d: finalize: %w", i, err)
		}
	}

	result.Elapsed = time.Since(start)
	return result, nil
}

// nilEngine is a minimal EngineReader for post-merge chains where we don't
// need real consensus logic - just the transfer function and author extraction.
type nilEngine struct{}

func (nilEngine) Author(header *types.Header) (accounts.Address, error) {
	return accounts.InternAddress(common.BytesToAddress(header.Coinbase[:])), nil
}

func (nilEngine) TxDependencies(header *types.Header) [][]int { return nil }

func (nilEngine) IsServiceTransaction(sender accounts.Address, syscall rules.SystemCall) bool {
	return false
}

func (nilEngine) Type() chain.RulesName { return "" }

func (nilEngine) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall rules.SystemCall) ([]rules.Reward, error) {
	return nil, nil
}

func (nilEngine) GetTransferFunc() evmtypes.TransferFunc {
	return misc.Transfer
}

func (nilEngine) GetPostApplyMessageFunc() evmtypes.PostApplyMessageFunc {
	return misc.LogSelfDestructedAccounts
}

func (nilEngine) Close() error { return nil }

// NilEngine returns a minimal EngineReader suitable for re-execution.
func NilEngine() rules.EngineReader { return nilEngine{} }
