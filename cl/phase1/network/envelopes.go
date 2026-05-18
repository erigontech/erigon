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

package network

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

var requestEnvelopeBatchExpiration = 30 * time.Second

// RequestEnvelopesFrantically requests execution payload envelopes from the network for the given beacon block roots.
// It first tries by-root, then falls back to by-range using the slot range of fullBlocks.
// EMPTY blocks will not have envelopes on the network, so timeout is non-fatal.
// Returns a map of beacon block root -> envelope for all received envelopes.
func RequestEnvelopesFrantically(ctx context.Context, r *rpc.BeaconRpcP2P, roots [][32]byte, fullBlocks ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
	received := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(roots))
	needed := make([][32]byte, len(roots))
	copy(needed, roots)

	// Build a set of requested roots so we can reject unsolicited envelopes from peers.
	requestedRoots := make(map[common.Hash]struct{}, len(roots))
	for _, root := range roots {
		requestedRoots[common.Hash(root)] = struct{}{}
	}

	timer := time.NewTimer(requestEnvelopeBatchExpiration)
	defer timer.Stop()

	byRootAttempts := 0
	byRangeAttempted := false
	for len(needed) > 0 {
		select {
		case <-ctx.Done():
			return received, ctx.Err()
		case <-timer.C:
			log.Debug("RequestEnvelopesFrantically: timeout, some envelopes not received", "missing", len(needed))
			return received, nil
		default:
		}

		// Try by-range once after initial by-root attempts as a supplement.
		// Don't keep retrying by-range — on devnets most peers register the
		// protocol but return EOF, wasting the entire 30s timeout budget.
		if byRootAttempts >= 3 && len(fullBlocks) > 0 && !byRangeAttempted {
			byRangeAttempted = true
			requestEnvelopesByRange(ctx, r, fullBlocks, received)
			needed = filterReceived(needed, received)
			if len(needed) == 0 {
				break
			}
		}

		responses, _, err := r.SendExecutionPayloadEnvelopesByRootReq(ctx, needed)
		if err != nil {
			log.Trace("RequestEnvelopesFrantically: by-root error", "err", err)
			byRootAttempts++
			time.Sleep(300 * time.Millisecond)
			continue
		}
		for _, env := range responses {
			if env.Message == nil {
				continue
			}
			if _, ok := requestedRoots[env.Message.BeaconBlockRoot]; !ok {
				log.Debug("RequestEnvelopesFrantically: ignoring unsolicited envelope", "root", env.Message.BeaconBlockRoot)
				continue
			}
			received[env.Message.BeaconBlockRoot] = env
		}
		needed = filterReceived(needed, received)
		if len(needed) > 0 {
			byRootAttempts++
			time.Sleep(300 * time.Millisecond)
		}
	}
	return received, nil
}

func filterReceived(needed [][32]byte, received map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) [][32]byte {
	var remaining [][32]byte
	for _, root := range needed {
		if _, ok := received[common.Hash(root)]; !ok {
			remaining = append(remaining, root)
		}
	}
	return remaining
}

func requestEnvelopesByRange(ctx context.Context, r *rpc.BeaconRpcP2P, blocks []*cltypes.SignedBeaconBlock, received map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) {
	if len(blocks) == 0 {
		return
	}
	startSlot := blocks[0].Block.Slot
	endSlot := blocks[len(blocks)-1].Block.Slot
	count := endSlot - startSlot + 1
	log.Debug("envelope fetch: falling back to by-range", "startSlot", startSlot, "count", count)

	// Build a set of valid block roots from the blocks we actually need envelopes for.
	validRoots := make(map[common.Hash]struct{}, len(blocks))
	for _, blk := range blocks {
		root, err := blk.Block.HashSSZ()
		if err != nil {
			continue
		}
		validRoots[root] = struct{}{}
	}

	envelopes, _, err := r.SendExecutionPayloadEnvelopesByRangeReq(ctx, startSlot, count)
	if err != nil {
		log.Debug("envelope fetch: by-range error", "err", err)
		return
	}
	for _, env := range envelopes {
		if env.Message == nil {
			continue
		}
		// Only accept envelopes whose BeaconBlockRoot matches one of the blocks we requested.
		// A malicious peer could respond with envelopes for arbitrary roots.
		if _, ok := validRoots[env.Message.BeaconBlockRoot]; !ok {
			log.Debug("requestEnvelopesByRange: ignoring unsolicited envelope", "root", env.Message.BeaconBlockRoot)
			continue
		}
		received[env.Message.BeaconBlockRoot] = env
	}
}
