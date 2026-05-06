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
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
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

var errNotFound = errors.New("notfound")

// beginOverlayOrRo returns a tx that reads from the block overlay (if a
// persistent SharedDomains with an active overlay exists) or a plain DB RO tx.
// When an overlay is active, the returned tx is an OverlayReadView backed by a
// fresh RO tx — each caller gets its own independent DB snapshot, so concurrent
// getters never share MDBX internal state.
// The caller must call the returned cleanup function when done.
func (e *ExecModule) beginOverlayOrRo(ctx context.Context) (kv.Tx, func(), error) {
	e.lock.RLock()
	sd := e.currentContext
	// Fall back to published SD during background commit.
	if sd == nil && e.publishedSD != nil {
		sd = e.publishedSD()
	}
	if sd != nil {
		if overlay := sd.BlockOverlay(); overlay != nil {
			// Open a fresh RO tx while still holding the read lock so that
			// the overlay cannot be closed between our check and the
			// NewReadView call (TOCTOU avoidance).
			roTx, err := e.db.BeginRo(ctx) //nolint:gocritic
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

	tx, err := e.db.BeginRo(ctx) //nolint:gocritic
	if err != nil {
		return nil, nil, err
	}
	return tx, func() { tx.Rollback() }, nil
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
	body, err := e.getBody(ctx, tx, hash, number)
	if err != nil {
		return nil, fmt.Errorf("ethereumExecutionModule.GetBody: getBody error %w", err)
	}
	if body == nil {
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
		body, err := e.getBody(ctx, tx, h, *number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByHashes: getBody error %w", err)
		}
		if body == nil {
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
	for i := uint64(0); i < count; i++ {
		hash, err := e.canonicalHash(ctx, tx, start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: ReadCanonicalHash error %w", err)
		}
		if hash == (common.Hash{}) {
			// beyond the last known canonical header
			break
		}
		body, err := e.getBody(ctx, tx, hash, start+i)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetBodiesByRange: getBody error %w", err)
		}
		if body == nil {
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
	for i := len(bodies) - 1; i >= 0; i-- {
		if bodies[i] == nil {
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
		body, err := e.getBody(ctx, tx, h, *number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: getBody error %w", err)
		}
		if body == nil {
			bodies = append(bodies, nil)
			continue
		}
		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: MarshalTransactionsBinary error %w", err)
		}
		balBytes, err := rawdb.ReadBlockAccessListBytes(tx, h, *number)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByHash: ReadBlockAccessListBytes error %w", err)
		}
		var bal []byte
		if len(balBytes) > 0 {
			bal = bytes.Clone(balBytes)
		}
		bodies = append(bodies, &PayloadBody{
			Transactions:    txs,
			Withdrawals:     body.Withdrawals,
			BlockAccessList: bal,
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
	for i := uint64(0); i < count; i++ {
		blockNum := start + i
		hash, err := e.canonicalHash(ctx, tx, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: ReadCanonicalHash error %w", err)
		}
		if hash == (common.Hash{}) {
			break
		}
		body, err := e.getBody(ctx, tx, hash, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: getBody error %w", err)
		}
		if body == nil {
			bodies = append(bodies, nil)
			continue
		}
		txs, err := types.MarshalTransactionsBinary(body.Transactions)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: MarshalTransactionsBinary error %w", err)
		}
		balBytes, err := rawdb.ReadBlockAccessListBytes(tx, hash, blockNum)
		if err != nil {
			return nil, fmt.Errorf("ethereumExecutionModule.GetPayloadBodiesByRange: ReadBlockAccessListBytes error %w", err)
		}
		var bal []byte
		if len(balBytes) > 0 {
			bal = bytes.Clone(balBytes)
		}
		bodies = append(bodies, &PayloadBody{
			Transactions:    txs,
			Withdrawals:     body.Withdrawals,
			BlockAccessList: bal,
		})
	}
	// Remove trailing nil values
	for i := len(bodies) - 1; i >= 0; i-- {
		if bodies[i] == nil {
			bodies = bodies[:i]
		} else {
			break
		}
	}
	return bodies, nil
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

func (e *ExecModule) GetTD(ctx context.Context, blockHash *common.Hash, blockNumber *uint64) (*big.Int, error) {
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
