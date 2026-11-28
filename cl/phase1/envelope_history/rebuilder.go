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

package envelope_history

import (
	"context"
	"errors"
	"math"
	"slices"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

var ErrRebuildBusy = errors.New("execution payload envelope rebuild is busy")

type ExecutionEngine interface {
	GetHeaderByHash(context.Context, common.Hash) (*types.Header, error)
	GetPayloadBodiesByHash(context.Context, []common.Hash) ([]*engine_types.ExecutionPayloadBodyV2, error)
}

type Rebuilder struct {
	beaconCfg   *clparams.BeaconChainConfig
	beaconDB    freezeblocks.BeaconSnapshotReader
	engine      ExecutionEngine
	rebuildSlot *semaphore.Weighted
}

type PreparedRebuild struct {
	blockRoot         common.Hash
	parentRoot        common.Hash
	bid               *cltypes.ExecutionPayloadBid
	signature         common.Bytes96
	executionRequests *cltypes.ExecutionRequests
}

func NewRebuilder(beaconCfg *clparams.BeaconChainConfig, beaconDB freezeblocks.BeaconSnapshotReader, engine ExecutionEngine) *Rebuilder {
	return &Rebuilder{beaconCfg: beaconCfg, beaconDB: beaconDB, engine: engine, rebuildSlot: semaphore.NewWeighted(1)}
}

func (r *Rebuilder) Prepare(ctx context.Context, tx kv.Tx, slot uint64, blockRoot common.Hash) (*PreparedRebuild, error) {
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return nil, err
	}
	if canonicalRoot != blockRoot {
		r.unavailable(blockRoot, "block is not canonical")
		return nil, nil
	}

	block, err := r.beaconDB.ReadBlockByRoot(ctx, tx, blockRoot)
	if err != nil {
		return nil, err
	}
	prepared, err := r.prepareParent(tx, slot, blockRoot, block)
	if err != nil || prepared == nil {
		return nil, err
	}
	var childRoot common.Hash
	var childSlot uint64
	err = beacon_indicies.RangeBlockRoots(tx, slot+1, math.MaxUint64, func(nextSlot uint64, root common.Hash) bool {
		childSlot = nextSlot
		childRoot = root
		return false
	})
	if err != nil {
		return nil, err
	}
	if childRoot == (common.Hash{}) {
		r.unavailable(blockRoot, "canonical child is unavailable")
		return nil, nil
	}
	child, err := r.beaconDB.ReadBlockByRoot(ctx, tx, childRoot)
	if err != nil {
		return nil, err
	}
	return r.prepareChild(prepared, block, childSlot, child), nil
}

// PrepareFromBlocks expects blockRoot to be canonical at slot and child to be the next canonical block, stored at childSlot.
func (r *Rebuilder) PrepareFromBlocks(tx kv.Tx, slot uint64, blockRoot common.Hash, block *cltypes.SignedBeaconBlock, childSlot uint64, child *cltypes.SignedBeaconBlock) (*PreparedRebuild, error) {
	prepared, err := r.prepareParent(tx, slot, blockRoot, block)
	if err != nil || prepared == nil {
		return nil, err
	}
	return r.prepareChild(prepared, block, childSlot, child), nil
}

func (r *Rebuilder) prepareParent(tx kv.Tx, slot uint64, blockRoot common.Hash, block *cltypes.SignedBeaconBlock) (*PreparedRebuild, error) {
	if block == nil || block.Block == nil || block.Block.Body == nil || block.Block.Slot != slot || block.Version() < clparams.GloasVersion {
		r.unavailable(blockRoot, "block is unavailable or predates Gloas")
		return nil, nil
	}
	signedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if signedBid == nil || signedBid.Message == nil {
		r.unavailable(blockRoot, "execution payload bid is unavailable")
		return nil, nil
	}
	bid := signedBid.Message

	signature, found, err := beacon_indicies.ReadExecutionPayloadEnvelopeSignature(tx, slot, blockRoot)
	if err != nil {
		return nil, err
	}
	if !found {
		r.unavailable(blockRoot, "signature is unavailable")
		return nil, nil
	}
	return &PreparedRebuild{
		blockRoot:  blockRoot,
		parentRoot: block.Block.ParentRoot,
		bid:        bid,
		signature:  signature,
	}, nil
}

func (r *Rebuilder) prepareChild(prepared *PreparedRebuild, block *cltypes.SignedBeaconBlock, childSlot uint64, child *cltypes.SignedBeaconBlock) *PreparedRebuild {
	if child == nil || child.Block == nil || child.Block.Body == nil || child.Block.Slot != childSlot || child.Block.ParentRoot != prepared.blockRoot {
		r.unavailable(prepared.blockRoot, "canonical child is invalid")
		return nil
	}
	if forkchoice.ParentPayloadStatusFromBids(block, child.Block) != cltypes.PayloadStatusFull {
		r.unavailable(prepared.blockRoot, "payload status is not full")
		return nil
	}
	executionRequests := child.Block.Body.GetParentExecutionRequests()
	if executionRequests == nil {
		r.unavailable(prepared.blockRoot, "parent execution requests are unavailable")
		return nil
	}
	prepared.executionRequests = executionRequests
	return prepared
}

func (r *Rebuilder) Rebuild(ctx context.Context, prepared *PreparedRebuild) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if err := r.rebuildSlot.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer r.rebuildSlot.Release(1)
	return r.rebuild(ctx, prepared)
}

func (r *Rebuilder) TryRebuild(ctx context.Context, prepared *PreparedRebuild) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !r.rebuildSlot.TryAcquire(1) {
		return nil, ErrRebuildBusy
	}
	defer r.rebuildSlot.Release(1)
	return r.rebuild(ctx, prepared)
}

func (r *Rebuilder) rebuild(ctx context.Context, prepared *PreparedRebuild) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	header, err := r.engine.GetHeaderByHash(ctx, prepared.bid.BlockHash)
	if err != nil {
		return nil, err
	}
	if header == nil || header.BaseFee == nil || header.SlotNumber == nil {
		r.unavailable(prepared.blockRoot, "execution header is unavailable")
		return nil, nil
	}
	bodies, err := r.engine.GetPayloadBodiesByHash(ctx, []common.Hash{prepared.bid.BlockHash})
	if err != nil {
		return nil, err
	}
	if len(bodies) == 0 || bodies[0] == nil || bodies[0].BlockAccessList == nil || len(*bodies[0].BlockAccessList) == 0 {
		r.unavailable(prepared.blockRoot, "execution body is unavailable")
		return nil, nil
	}
	body := bodies[0]
	if slices.Contains(body.Withdrawals, nil) {
		r.unavailable(prepared.blockRoot, "execution body is invalid")
		return nil, nil
	}
	rawBody := &types.RawBody{Transactions: make([][]byte, len(body.Transactions)), Withdrawals: body.Withdrawals}
	for i := range body.Transactions {
		rawBody.Transactions[i] = body.Transactions[i]
	}
	payload := cltypes.NewEth1BlockFromHeaderAndBody(header, rawBody, r.beaconCfg)
	if payload.BlockAccessList == nil || payload.BlockAccessList.SetBytes(*body.BlockAccessList) != nil {
		r.unavailable(prepared.blockRoot, "block access list is invalid")
		return nil, nil
	}
	message := cltypes.NewExecutionPayloadEnvelope(r.beaconCfg)
	message.Payload = payload
	message.ExecutionRequests = prepared.executionRequests
	message.BuilderIndex = prepared.bid.BuilderIndex
	message.BeaconBlockRoot = prepared.blockRoot
	message.ParentBeaconBlockRoot = prepared.parentRoot
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: message, Signature: prepared.signature}
	if err := cltypes.ValidateGloasEnvelopeAgainstBid(r.beaconCfg, prepared.blockRoot, prepared.bid, envelope); err != nil {
		log.Warn("rebuilt execution payload envelope failed verification", "blockRoot", prepared.blockRoot, "err", err)
		return nil, nil
	}
	return envelope, nil
}

func (r *Rebuilder) unavailable(blockRoot common.Hash, reason string) {
	log.Debug("execution payload envelope is unavailable", "blockRoot", blockRoot, "reason", reason)
}
