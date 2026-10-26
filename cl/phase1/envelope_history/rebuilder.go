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
	"math"
	"slices"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

const MaxRebuildsPerRequest = 32

type ExecutionEngine interface {
	GetHeaderByHash(context.Context, common.Hash) (*types.Header, error)
	GetPayloadBodiesByHash(context.Context, []common.Hash) ([]*engine_types.ExecutionPayloadBodyV2, error)
}

type Rebuilder struct {
	beaconCfg   *clparams.BeaconChainConfig
	beaconDB    freezeblocks.BeaconSnapshotReader
	engine      ExecutionEngine
	rebuildSlot chan struct{}
}

func NewRebuilder(beaconCfg *clparams.BeaconChainConfig, beaconDB freezeblocks.BeaconSnapshotReader, engine ExecutionEngine) *Rebuilder {
	return &Rebuilder{beaconCfg: beaconCfg, beaconDB: beaconDB, engine: engine, rebuildSlot: make(chan struct{}, 1)}
}

func (r *Rebuilder) Rebuild(ctx context.Context, tx kv.Tx, slot uint64, blockRoot common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return nil, err
	}
	if canonicalRoot != blockRoot {
		return r.unavailable(blockRoot, "block is not canonical"), nil
	}

	block, err := r.beaconDB.ReadBlockByRoot(ctx, tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if block == nil || block.Block == nil || block.Block.Body == nil || block.Block.Slot != slot || block.Version() < clparams.GloasVersion {
		return r.unavailable(blockRoot, "block is unavailable or predates Gloas"), nil
	}
	signedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if signedBid == nil || signedBid.Message == nil {
		return r.unavailable(blockRoot, "execution payload bid is unavailable"), nil
	}
	bid := signedBid.Message

	signature, found, err := beacon_indicies.ReadExecutionPayloadEnvelopeSignature(tx, slot, blockRoot)
	if err != nil {
		return nil, err
	}
	if !found {
		return r.unavailable(blockRoot, "signature is unavailable"), nil
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
		return r.unavailable(blockRoot, "canonical child is unavailable"), nil
	}
	child, err := r.beaconDB.ReadBlockByRoot(ctx, tx, childRoot)
	if err != nil {
		return nil, err
	}
	if child == nil || child.Block == nil || child.Block.Body == nil || child.Block.Slot != childSlot || child.Block.ParentRoot != blockRoot {
		return r.unavailable(blockRoot, "canonical child is invalid"), nil
	}
	if forkchoice.ParentPayloadStatusFromBids(block, child.Block) != cltypes.PayloadStatusFull {
		return r.unavailable(blockRoot, "payload status is not full"), nil
	}
	executionRequests := child.Block.Body.GetParentExecutionRequests()
	if executionRequests == nil {
		return r.unavailable(blockRoot, "parent execution requests are unavailable"), nil
	}

	select {
	case r.rebuildSlot <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if ctx.Err() != nil {
		<-r.rebuildSlot
		return nil, ctx.Err()
	}
	defer func() { <-r.rebuildSlot }()

	header, err := r.engine.GetHeaderByHash(ctx, bid.BlockHash)
	if err != nil {
		return nil, err
	}
	if header == nil || header.BaseFee == nil || header.SlotNumber == nil {
		return r.unavailable(blockRoot, "execution header is unavailable"), nil
	}
	bodies, err := r.engine.GetPayloadBodiesByHash(ctx, []common.Hash{bid.BlockHash})
	if err != nil {
		return nil, err
	}
	if len(bodies) == 0 || bodies[0] == nil || bodies[0].BlockAccessList == nil || len(*bodies[0].BlockAccessList) == 0 {
		return r.unavailable(blockRoot, "execution body is unavailable"), nil
	}
	body := bodies[0]
	if slices.Contains(body.Withdrawals, nil) {
		return r.unavailable(blockRoot, "execution body is invalid"), nil
	}
	rawBody := &types.RawBody{Transactions: make([][]byte, len(body.Transactions)), Withdrawals: body.Withdrawals}
	for i := range body.Transactions {
		rawBody.Transactions[i] = body.Transactions[i]
	}
	payload := cltypes.NewEth1BlockFromHeaderAndBody(header, rawBody, r.beaconCfg)
	if payload.BlockAccessList == nil || payload.BlockAccessList.SetBytes(*body.BlockAccessList) != nil {
		return r.unavailable(blockRoot, "block access list is invalid"), nil
	}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload:               payload,
			ExecutionRequests:     executionRequests,
			BuilderIndex:          bid.BuilderIndex,
			BeaconBlockRoot:       blockRoot,
			ParentBeaconBlockRoot: block.Block.ParentRoot,
		},
		Signature: signature,
	}
	if err := network.ValidateGloasEnvelopeAgainstBid(r.beaconCfg, blockRoot, bid, envelope); err != nil {
		log.Warn("rebuilt execution payload envelope failed verification", "blockRoot", blockRoot, "err", err)
		return nil, nil
	}
	return envelope, nil
}

func (r *Rebuilder) unavailable(blockRoot common.Hash, reason string) *cltypes.SignedExecutionPayloadEnvelope {
	log.Debug("execution payload envelope is unavailable", "blockRoot", blockRoot, "reason", reason)
	return nil
}
