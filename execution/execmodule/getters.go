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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
)

// bodyToRawBody converts a parsed Body to a RawBody using MarshalBinary
// (canonical binary encoding) for transactions. This differs from
// Body.RawBody() which uses rlp.EncodeToBytes and wraps typed transactions
// in an extra RLP string header — incorrect for the engine API which expects
// raw binary tx format (type prefix + RLP payload, no outer wrapper).
func bodyToRawBody(body *types.Body) (*types.RawBody, error) {
	txs, err := types.MarshalTransactionsBinary(body.Transactions)
	if err != nil {
		return nil, err
	}
	return &types.RawBody{
		Transactions: txs,
		Uncles:       body.Uncles,
		Withdrawals:  body.Withdrawals,
	}, nil
}

// beginOverlayOrRo returns a tx that reads from the block overlay (if a
// persistent SharedDomains with an active overlay exists) or a plain DB RO tx.
// When an overlay is active, the returned tx is an OverlayReadView backed by a
// fresh RO tx — each caller gets its own independent DB snapshot, so concurrent
// getters never share MDBX internal state.
// The caller must call the returned cleanup function when done.
func (e *ExecModule) beginOverlayOrRo(ctx context.Context) (kv.TemporalTx, func(), error) {
	e.lock.RLock()
	sd := e.currentContext
	// Fall back to published SD while an FCU commits.
	if sd == nil && e.publishedSD != nil {
		sd = e.publishedSD()
	}
	if sd != nil {
		if overlay := sd.BlockOverlay(); overlay != nil {
			// Open a fresh RO tx while still holding the read lock so that
			// the overlay cannot be closed between our check and the
			// NewReadView call (TOCTOU avoidance).
			roTx, err := e.db.BeginTemporalRo(ctx) //nolint:gocritic
			if err != nil {
				e.lock.RUnlock()
				return nil, nil, err
			}
			view := overlay.NewReadView(roTx)
			e.lock.RUnlock()
			return view, func() { roTx.Rollback() }, nil
		}
	}
	e.lock.RUnlock()

	tx, err := e.db.BeginTemporalRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, nil, err
	}
	return tx, func() { tx.Rollback() }, nil
}

// resolveSegment converts optional (blockHash, blockNumber) to a concrete
// (hash, number) pair by looking up the missing value from the database.
func (e *ExecModule) resolveSegment(ctx context.Context, tx kv.Tx, blockHash *common.Hash, blockNumber *uint64) (common.Hash, uint64, bool, error) {
	switch {
	case blockHash != nil && blockNumber == nil:
		// Only hash: resolve number
		number, ok, err := e.blockReader.HeaderNumber(ctx, tx, *blockHash)
		if err != nil {
			return common.Hash{}, 0, false, err
		}
		if !ok {
			return common.Hash{}, 0, false, nil
		}
		return *blockHash, number, true, nil

	case blockHash == nil && blockNumber != nil:
		// Only number: resolve canonical hash
		hash, ok, err := e.canonicalHash(ctx, tx, *blockNumber)
		if err != nil {
			return common.Hash{}, 0, false, err
		}
		return hash, *blockNumber, ok, nil

	case blockHash != nil && blockNumber != nil:
		return *blockHash, *blockNumber, true, nil

	default:
		return common.Hash{}, 0, false, errors.New("at least one of blockHash or blockNumber must be provided")
	}
}

func (e *ExecModule) GetBody(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.RawBody, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: could not begin database tx %w", err)
	}
	defer cleanup()

	hash, number, ok, err := e.resolveSegment(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: resolveSegment error %w", err)
	}
	if !ok {
		return nil, nil
	}
	body, ok, err := e.getBody(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: getBody error %w", err)
	}
	if !ok {
		return nil, nil
	}
	return bodyToRawBody(body)
}

func (e *ExecModule) GetHeader(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*types.Header, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: could not begin database tx %w", err)
	}
	defer cleanup()

	hash, number, ok, err := e.resolveSegment(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeader: resolveSegment error %w", err)
	}
	if !ok {
		return nil, nil
	}
	header, ok, err := e.getHeader(ctx, tx, hash, number)
	if err != nil || !ok {
		return nil, err
	}
	return header, nil
}

func (e *ExecModule) GetBodiesByHashes(ctx context.Context, hashes []common.Hash) ([]*types.RawBody, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: could not begin database tx %w", err)
	}
	defer cleanup()

	bodies := make([]*types.RawBody, 0, len(hashes))
	for _, h := range hashes {
		number, ok, err := e.blockReader.HeaderNumber(ctx, tx, h)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: HeaderNumber error %w", err)
		}
		if !ok {
			bodies = append(bodies, nil)
			continue
		}
		body, ok, err := e.getBody(ctx, tx, h, number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: getBody error %w", err)
		}
		if !ok {
			bodies = append(bodies, nil)
			continue
		}
		rb, err := bodyToRawBody(body)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: MarshalTransactionsBinary error %w", err)
		}
		bodies = append(bodies, rb)
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
		hash, ok, err := e.canonicalHash(ctx, tx, start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: ReadCanonicalHash error %w", err)
		}
		if !ok {
			// beyond the last known canonical header
			break
		}
		body, ok, err := e.getBody(ctx, tx, hash, start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: getBody error %w", err)
		}
		if !ok {
			bodies = append(bodies, nil)
			continue
		}
		rb, err := bodyToRawBody(body)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: MarshalTransactionsBinary error %w", err)
		}
		bodies = append(bodies, rb)
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
		number, ok, err := e.blockReader.HeaderNumber(ctx, tx, h)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: HeaderNumber error %w", err)
		}
		if !ok {
			bodies = append(bodies, nil)
			continue
		}
		body, ok, err := e.getBody(ctx, tx, h, number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: getBody error %w", err)
		}
		if !ok {
			bodies = append(bodies, nil)
			continue
		}
		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: MarshalTransactionsBinary error %w", err)
		}
		balBytes, balFound, err := rawdb.ReadBlockAccessListBytes(tx, h, number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: ReadBlockAccessListBytes error %w", err)
		}
		if balFound {
			// GetOne returns mdbx-backed memory; bodies outlive this tx.
			balBytes = bytes.Clone(balBytes)
		} else {
			balBytes, err = e.regenerateBlockAccessList(ctx, tx, h, number)
			if err != nil {
				return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: regenerateBlockAccessList error %w", err)
			}
		}
		bodies = append(bodies, &PayloadBody{
			Transactions:    txs,
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
		hash, ok, err := e.canonicalHash(ctx, tx, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: ReadCanonicalHash error %w", err)
		}
		if !ok {
			break
		}
		body, ok, err := e.getBody(ctx, tx, hash, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: getBody error %w", err)
		}
		if !ok {
			bodies = append(bodies, nil)
			continue
		}
		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: MarshalTransactionsBinary error %w", err)
		}
		balBytes, balFound, err := rawdb.ReadBlockAccessListBytes(tx, hash, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: ReadBlockAccessListBytes error %w", err)
		}
		if balFound {
			// GetOne returns mdbx-backed memory; bodies outlive this tx.
			balBytes = bytes.Clone(balBytes)
		} else {
			balBytes, err = e.regenerateBlockAccessList(ctx, tx, hash, blockNum)
			if err != nil {
				return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: regenerateBlockAccessList error %w", err)
			}
		}
		bodies = append(bodies, &PayloadBody{
			Transactions:    txs,
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
	encoded, err := e.balRegenerator.GetBlockAccessListBytes(ctx, e.config, tx, blockHash, blockNum)
	if err == nil {
		return encoded, nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}
	if errors.Is(err, state.PrunedError) {
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

	blockNumber, ok, err := e.blockReader.HeaderNumber(ctx, tx, blockHash)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetHeaderHashNumber: HeaderNumber error %w", err)
	}
	if !ok {
		return nil, nil
	}
	return &blockNumber, nil
}

func (e *ExecModule) isCanonicalHash(ctx context.Context, tx kv.Tx, hash common.Hash) (bool, error) {
	blockNumber, ok, err := e.blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: HeaderNumber error %w", err)
	}
	if !ok {
		return false, nil
	}

	expectedHash, ok, err := e.canonicalHash(ctx, tx, blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: could not read canonical hash %w", err)
	}
	if !ok {
		return false, nil
	}
	_, ok, err = rawdb.ReadTd(tx, hash, blockNumber)
	if err != nil {
		return false, fmt.Errorf("ethereumExecutionModule.isCanonicalHash: ReadTd error %w", err)
	}
	if !ok {
		return false, nil
	}
	return expectedHash == hash, nil
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

	hash, ok, err := rawdb.ReadHeadHeaderHash(tx)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: ReadHeadHeaderHash error %w", err)
	}
	if !ok {
		return nil, errors.New("ethereumExecutionModule.CurrentHeader: no head header hash - probably node not synced yet")
	}
	number, ok, err := e.blockReader.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: blockReader.HeaderNumber error %w", err)
	}
	if !ok {
		return nil, errors.New("ethereumExecutionModule.CurrentHeader: blockReader.HeaderNumber did not find a block - probably node not synced yet")
	}
	h, ok, err := e.blockReader.Header(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.CurrentHeader: blockReader.Header error %w", err)
	}
	if !ok {
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

	hash, number, ok, err := e.resolveSegment(ctx, tx, blockHash, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: resolveSegment error %w", err)
	}
	if !ok {
		return nil, nil
	}
	td, found, err := e.getTD(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetTD: getTD error %w", err)
	}
	if !found {
		return nil, nil
	}
	return td, nil
}

func (e *ExecModule) GetForkChoice(ctx context.Context) (ForkChoiceState, error) {
	tx, cleanup, err := e.beginOverlayOrRo(ctx)
	if err != nil {
		return ForkChoiceState{}, fmt.Errorf("ethereumExecutionModule.GetForkChoice: could not begin database tx %w", err)
	}
	defer cleanup()

	headHash, _, err := rawdb.ReadForkchoiceHead(tx)
	if err != nil {
		return ForkChoiceState{}, err
	}
	finalizedHash, _, err := rawdb.ReadForkchoiceFinalized(tx)
	if err != nil {
		return ForkChoiceState{}, err
	}
	safeHash, _, err := rawdb.ReadForkchoiceSafe(tx)
	if err != nil {
		return ForkChoiceState{}, err
	}
	return ForkChoiceState{HeadHash: headHash, FinalizedHash: finalizedHash, SafeHash: safeHash}, nil
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
