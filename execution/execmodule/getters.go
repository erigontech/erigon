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

package execmodule

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

var errNotFound = errors.New("notfound")

func inclusionListGasPools(header *types.Header, maxBlobGas uint64) []*protocol.GasPool {
	limit, used := header.GasLimit, header.GasUsed
	blobLeft := maxBlobGas - *header.BlobGasUsed
	return []*protocol.GasPool{
		protocol.NewBlockGasPool(limit-used, limit-used, blobLeft),
		protocol.NewBlockGasPool(limit-used, limit, blobLeft),
		protocol.NewBlockGasPool(limit, limit-used, blobLeft),
	}
}

func (e *ExecModule) InclusionListSatisfied(ctx context.Context, blockHash common.Hash, blockNumber uint64, il types.Transactions) (*bool, error) {
	if len(il) == 0 {
		satisfied := true
		return &satisfied, nil
	}
	ilHash := exec.InclusionListHash(il)
	if satisfied, ok := e.readAheader.ReadInclusionListResult(blockHash, ilHash); ok {
		return &satisfied, nil
	}

	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	canonical, err := e.isCanonicalHash(ctx, tx, blockHash)
	if err != nil || !canonical {
		return nil, err
	}
	execProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil || execProgress < blockNumber {
		return nil, err
	}
	block, _, err := e.blockReader.BlockWithSenders(ctx, tx, blockHash, blockNumber)
	if err != nil || block == nil {
		return nil, err
	}
	blockEndTxNum, err := e.blockReader.TxnumReader().Max(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}
	blockStartTxNum, err := e.blockReader.TxnumReader().Min(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}
	covered, err := e.stateHistoryCovers(ctx, tx, blockNumber, blockStartTxNum, blockEndTxNum+1)
	if err != nil || !covered {
		return nil, err
	}

	header := block.Header()
	reader := state.NewHistoryReaderV3(tx, blockEndTxNum)
	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		return e.blockReader.Header(ctx, tx, hash, number)
	}
	blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, getHeader), e.engine, accounts.NilAddress, e.config)

	var result *bool
	for _, gasPool := range inclusionListGasPools(header, e.config.GetMaxBlobGasPerBlock(header.Time)) {
		satisfied, err := protocol.CheckInclusionListTransactions(gasPool, block.Transactions(), il, reader, &blockContext, e.config, &vm.Config{})
		if err != nil {
			return nil, err
		}
		if result != nil && *result != satisfied {
			result = nil
			break
		}
		result = &satisfied
	}
	if result == nil {
		gasPool, err := e.replayBlockGasPool(tx, block, blockStartTxNum, getHeader)
		if err != nil {
			return nil, err
		}
		satisfied, err := protocol.CheckInclusionListTransactions(gasPool, block.Transactions(), il, reader, &blockContext, e.config, &vm.Config{})
		if err != nil {
			return nil, err
		}
		result = &satisfied
	}
	e.readAheader.SetInclusionListResult(blockHash, ilHash, *result)
	return result, nil
}

// replayBlockGasPool re-executes block on the state before it to recover the
// per-dimension gas left, which the header's max-of-dimensions GasUsed hides.
func (e *ExecModule) replayBlockGasPool(tx kv.TemporalTx, block *types.Block, blockStartTxNum uint64, getHeader func(common.Hash, uint64) (*types.Header, error)) (*protocol.GasPool, error) {
	header := block.Header()
	ibs := state.New(state.NewHistoryReaderV3(tx, blockStartTxNum))
	defer ibs.Close()
	chainReader := consensuschain.NewReader(e.config, tx, e.blockReader, e.logger)
	if err := protocol.InitializeBlockExecution(e.engine, chainReader, header, e.config, ibs, state.NewNoopWriter(), e.logger, nil); err != nil {
		return nil, err
	}
	gasPool := protocol.NewGasPool(header.GasLimit, e.config.GetMaxBlobGasPerBlock(header.Time))
	blockHashFunc := protocol.GetHashFn(header, getHeader)
	for i, txn := range block.Transactions() {
		ibs.SetTxContext(block.NumberU64(), i)
		if _, err := protocol.ApplyTransaction(e.config, blockHashFunc, e.engine, accounts.NilAddress, gasPool, ibs, state.NewNoopWriter(), header, txn, new(protocol.GasUsed), vm.Config{}); err != nil {
			return nil, err
		}
	}
	return gasPool, nil
}

func (e *ExecModule) stateHistoryCovers(ctx context.Context, tx kv.TemporalTx, blockNumber, fromTxNum, toTxNum uint64) (bool, error) {
	err := state.RequireStateHistory(tx.Debug(), fromTxNum, toTxNum)
	if !errors.Is(err, state.ErrPruned) {
		return err == nil, err
	}
	if !inMemHistoryReads {
		return false, nil
	}
	if overlay, _ := membatchwithdb.ViewOverlay(tx); overlay == nil {
		return false, nil
	}
	dbTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return false, err
	}
	defer dbTx.Rollback()
	committed, err := stages.GetStageProgress(dbTx, stages.Execution)
	if err != nil {
		return false, err
	}
	return blockNumber > committed, nil
}

// beginOverlayOrRo returns a tx that reads from the block overlay (if a
// persistent SharedDomains with an active overlay exists) or a plain DB RO tx.
// When an overlay is active, the returned tx is an OverlayReadView backed by a
// fresh RO tx — each caller gets its own independent DB snapshot, so concurrent
// getters never share MDBX internal state.
// The caller must call the returned cleanup function when done.
func (e *ExecModule) beginOverlayOrRo(ctx context.Context) (kv.TemporalTx, func(), error) {
	if view, cleanup, err := e.beginOverlayView(ctx); cleanup != nil || err != nil {
		return view, cleanup, err
	}

	tx, err := e.db.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, nil, err
	}
	return tx, func() { tx.Rollback() }, nil
}

// beginOverlayView returns a nil cleanup when no overlay is active.
func (e *ExecModule) beginOverlayView(ctx context.Context) (kv.TemporalTx, func(), error) {
	e.lock.RLock()
	defer e.lock.RUnlock()
	sd := e.currentContext
	// Fall back to published SD while an FCU commits.
	if sd == nil && e.publishedSD != nil {
		sd = e.publishedSD()
	}
	if sd == nil {
		return nil, nil, nil
	}
	overlay := sd.BlockOverlay()
	if overlay == nil {
		return nil, nil, nil
	}
	// Open a fresh RO tx while still holding the read lock so that
	// the overlay cannot be closed between our check and the
	// NewReadView call (TOCTOU avoidance).
	roTx, err := e.db.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, nil, err
	}
	ok := false
	defer kv.RollbackUnless(&ok, roTx)
	view := overlay.NewReadView(roTx)
	ok = true
	return view, func() { roTx.Rollback() }, nil
}

// resolveSegment converts optional (blockHash, blockNumber) to a concrete
// (hash, number) pair by looking up the missing value from the database.
func (e *ExecModule) resolveSegment(ctx context.Context, tx kv.Tx, blockHash *common.Hash, blockNumber *uint64) (common.Hash, uint64, error) {
	switch {
	case blockHash != nil && blockNumber == nil:
		// Only hash: resolve number
		numPtr, err := e.blockReader.HeaderNumber(ctx, tx, *blockHash)
		if err != nil {
			return common.Hash{}, 0, err
		}
		if numPtr == nil {
			return common.Hash{}, 0, errNotFound
		}
		return *blockHash, *numPtr, nil

	case blockHash == nil && blockNumber != nil:
		// Only number: resolve canonical hash
		hash, err := e.canonicalHash(ctx, tx, *blockNumber)
		if err != nil {
			return common.Hash{}, 0, errNotFound
		}
		return hash, *blockNumber, nil

	case blockHash != nil && blockNumber != nil:
		return *blockHash, *blockNumber, nil

	default:
		return common.Hash{}, 0, errors.New("at least one of blockHash or blockNumber must be provided")
	}
}

func (e *ExecModule) GetBody(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.RawBody, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: could not begin database tx %w", err)
	}
	defer cleanup()

	hash, number, err := e.resolveSegment(ctx, tx, blockHash, blockNumber)
	if errors.Is(err, errNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: resolveSegment error %w", err)
	}
	body, err := e.getRawBody(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: getRawBody error %w", err)
	}
	return body, nil
}

func (e *ExecModule) GetHeader(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: could not begin database tx %w", err)
	}
	defer cleanup()

	hash, number, err := e.resolveSegment(ctx, tx, blockHash, blockNumber)
	if errors.Is(err, errNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: resolveSegment error %w", err)
	}
	return e.getHeader(ctx, tx, hash, number)
}

func (e *ExecModule) GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: could not begin database tx %w", err)
	}
	defer cleanup()

	bodies := make([]*types.RawBody, 0, len(hashes))
	for _, h := range hashes {
		number, err := e.blockReader.HeaderNumber(ctx, tx, h)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: HeaderNumber error %w", err)
		}
		if number == nil {
			bodies = append(bodies, nil)
			continue
		}
		body, err := e.getRawBody(ctx, tx, h, *number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: getRawBody error %w", err)
		}
		bodies = append(bodies, body)
	}
	return bodies, nil
}

func (e *ExecModule) GetBodiesByRange(ctx context.Context, start, count uint64) ([]*types.RawBody, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: could not begin database tx %w", err)
	}
	defer cleanup()

	bodies := make([]*types.RawBody, 0, count)
	for i := range count {
		hash, err := e.canonicalHash(ctx, tx, start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: ReadCanonicalHash error %w", err)
		}
		if hash == (common.Hash{}) {
			// beyond the last known canonical header
			break
		}
		body, err := e.getRawBody(ctx, tx, hash, start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: getRawBody error %w", err)
		}
		bodies = append(bodies, body)
	}
	// Remove trailing nil values as per spec
	// See point 4 in https://github.com/ethereum/execution-apis/blob/main/src/engine/shanghai.md#specification-4
	for i, body := range slices.Backward(bodies) {
		if body == nil {
			bodies = bodies[:i]
		} else {
			break
		}
	}
	return bodies, nil
}

func (e *ExecModule) GetPayloadBodiesByHash(ctx context.Context, hashes []common.Hash) ([]*PayloadBody, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: could not begin database tx %w", err)
	}
	defer cleanup()

	bodies := make([]*PayloadBody, 0, len(hashes))
	for _, h := range hashes {
		number, err := e.blockReader.HeaderNumber(ctx, tx, h)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: HeaderNumber error %w", err)
		}
		if number == nil {
			bodies = append(bodies, nil)
			continue
		}
		body, err := e.getRawBody(ctx, tx, h, *number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: getRawBody error %w", err)
		}
		if body == nil {
			bodies = append(bodies, nil)
			continue
		}
		balBytes, err := rawdb.ReadBlockAccessListBytes(tx, h, *number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: ReadBlockAccessListBytes error %w", err)
		}
		if len(balBytes) > 0 {
			// GetOne returns mdbx-backed memory; bodies outlive this tx.
			balBytes = bytes.Clone(balBytes)
		} else {
			balBytes, err = e.regenerateBlockAccessList(ctx, tx, h, *number)
			if err != nil {
				return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: regenerateBlockAccessList error %w", err)
			}
		}
		bodies = append(bodies, &PayloadBody{
			Transactions:    body.Transactions,
			Withdrawals:     body.Withdrawals,
			BlockAccessList: balBytes,
		})
	}
	return bodies, nil
}

func (e *ExecModule) GetPayloadBodiesByRange(ctx context.Context, start, count uint64) ([]*PayloadBody, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: could not begin database tx %w", err)
	}
	defer cleanup()

	bodies := make([]*PayloadBody, 0, count)
	for i := range count {
		blockNum := start + i
		hash, err := e.canonicalHash(ctx, tx, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: ReadCanonicalHash error %w", err)
		}
		if hash == (common.Hash{}) {
			break
		}
		body, err := e.getRawBody(ctx, tx, hash, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: getRawBody error %w", err)
		}
		if body == nil {
			bodies = append(bodies, nil)
			continue
		}
		balBytes, err := rawdb.ReadBlockAccessListBytes(tx, hash, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: ReadBlockAccessListBytes error %w", err)
		}
		if len(balBytes) > 0 {
			// GetOne returns mdbx-backed memory; bodies outlive this tx.
			balBytes = bytes.Clone(balBytes)
		} else {
			balBytes, err = e.regenerateBlockAccessList(ctx, tx, hash, blockNum)
			if err != nil {
				return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: regenerateBlockAccessList error %w", err)
			}
		}
		bodies = append(bodies, &PayloadBody{
			Transactions:    body.Transactions,
			Withdrawals:     body.Withdrawals,
			BlockAccessList: balBytes,
		})
	}
	// Remove trailing nil values
	for i, body := range slices.Backward(bodies) {
		if body == nil {
			bodies = bodies[:i]
		} else {
			break
		}
	}
	return bodies, nil
}

// regenerateBlockAccessList re-derives a missing BAL by re-execution. Returns
// nil bytes when the block has no BAL or it cannot be regenerated — the engine
// API then reports null for that block, per spec, rather than failing the
// whole request.
func (e *ExecModule) regenerateBlockAccessList(ctx context.Context, tx kv.TemporalTx, blockHash common.Hash, blockNum uint64) ([]byte, error) {
	encoded, err := e.balRegenerator.GetBlockAccessListBytes(ctx, e.config, tx, blockHash, blockNum, nil)
	if err == nil {
		return encoded, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	if errors.Is(err, state.ErrPruned) {
		e.logger.Debug("regenerateBlockAccessList: history unavailable", "block", blockNum, "hash", blockHash, "err", err)
		return nil, nil
	}
	e.logger.Warn("regenerateBlockAccessList: regeneration failed", "block", blockNum, "hash", blockHash, "err", err)
	return nil, nil
}

func (e *ExecModule) GetHeaderHashNumber(ctx context.Context, blockHash common.Hash) (*uint64, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeaderHashNumber: could not begin database tx %w", err)
	}
	defer cleanup()

	blockNumber, err := e.blockReader.HeaderNumber(ctx, tx, blockHash)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeaderHashNumber: HeaderNumber error %w", err)
	}
	return blockNumber, nil
}

func (e *ExecModule) isCanonicalHash(ctx context.Context, tx kv.Tx, hash common.Hash) (bool, error) {
	blockNumber, err := e.blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: HeaderNumber error %w", err)
	}
	if blockNumber == nil {
		return false, nil
	}

	expectedHash, err := e.canonicalHash(ctx, tx, *blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: could not read canonical hash %w", err)
	}
	td, err := rawdb.ReadTd(tx, hash, *blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: ReadTd error %w", err)
	}
	if td == nil {
		return false, nil
	}
	return expectedHash == hash, nil
}

func (e *ExecModule) IsValidated(blockHash common.Hash) bool {
	return e.forkValidator.IsValidated(blockHash)
}

func (e *ExecModule) IsCanonicalHash(ctx context.Context, blockHash common.Hash) (bool, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.IsCanonicalHash: could not begin database tx %w", err)
	}
	defer cleanup()

	isCanonical, err := e.isCanonicalHash(ctx, tx, blockHash)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.IsCanonicalHash: could not read canonical hash %w", err)
	}
	return isCanonical, nil
}

func (e *ExecModule) CurrentHeader(ctx context.Context) (*types.Header, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: could not begin database tx %w", err)
	}
	defer cleanup()

	hash := rawdb.ReadHeadHeaderHash(tx)
	number, err := e.blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: blockReader.HeaderNumber error %w", err)
	}
	if number == nil {
		return nil, errors.New("ethereumExecutionModule.CurrentHeader: blockReader.HeaderNumber returned nil - probably node not synced yet")
	}
	h, err := e.blockReader.Header(ctx, tx, hash, *number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: blockReader.Header error %w", err)
	}
	if h == nil {
		return nil, errors.New("ethereumExecutionModule.CurrentHeader: no current header yet - probably node not synced yet")
	}
	return h, nil
}

func (e *ExecModule) GetTD(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*uint256.Int, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: could not begin database tx %w", err)
	}
	defer cleanup()

	hash, number, err := e.resolveSegment(ctx, tx, blockHash, blockNumber)
	if errors.Is(err, errNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: resolveSegment error %w", err)
	}
	td, err := e.getTD(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: getTD error %w", err)
	}
	return td, nil
}

func (e *ExecModule) GetForkChoice(ctx context.Context) (ForkChoiceState, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return ForkChoiceState{}, fmt.Errorf("ethereumExecutionModule.GetForkChoice: could not begin database tx %w", err)
	}
	defer cleanup()

	return ForkChoiceState{
		HeadHash:      rawdb.ReadForkchoiceHead(tx),
		FinalizedHash: rawdb.ReadForkchoiceFinalized(tx),
		SafeHash:      rawdb.ReadForkchoiceSafe(tx),
	}, nil
}

func (e *ExecModule) FrozenBlocks(ctx context.Context) (frozenBlocks uint64, hasGap bool, err error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("ethereumExecutionModule.FrozenBlocks: could not begin database tx %w", err)
	}
	defer cleanup()

	firstNonGenesisBlockNumber, ok, err := rawdb.ReadFirstNonGenesisHeaderNumber(tx)
	if err != nil {
		return 0, false, err
	}
	gap := false
	if ok {
		gap = e.blockReader.Snapshots().SegmentsMax()+1 < firstNonGenesisBlockNumber
	}
	return e.blockReader.FrozenBlocks(), gap, nil
}
