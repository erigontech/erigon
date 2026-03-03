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

var requestEnvelopeBatchExpiration = 15 * time.Second

// RequestEnvelopesFrantically requests execution payload envelopes from the network for the given beacon block roots.
// It retries until all envelopes are received or a timeout occurs.
// EMPTY blocks will not have envelopes on the network, so timeout is non-fatal.
// Returns a map of beacon block root -> envelope for all received envelopes.
func RequestEnvelopesFrantically(ctx context.Context, r *rpc.BeaconRpcP2P, roots [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
	received := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(roots))
	needed := make([][32]byte, len(roots))
	copy(needed, roots)

	timer := time.NewTimer(requestEnvelopeBatchExpiration)
	defer timer.Stop()
	for len(needed) > 0 {
		select {
		case <-ctx.Done():
			return received, ctx.Err()
		case <-timer.C:
			log.Debug("RequestEnvelopesFrantically: timeout, some envelopes not received (may be EMPTY blocks)", "missing", len(needed))
			return received, nil
		default:
		}

		responses, _, err := r.SendExecutionPayloadEnvelopesByRootReq(ctx, needed)
		if err != nil {
			log.Trace("RequestEnvelopesFrantically: error", "err", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		for _, env := range responses {
			if env.Message == nil {
				continue
			}
			received[env.Message.BeaconBlockRoot] = env
		}
		// Filter out received roots
		var remaining [][32]byte
		for _, root := range needed {
			if _, ok := received[common.Hash(root)]; !ok {
				remaining = append(remaining, root)
			}
		}
		needed = remaining
		if len(needed) > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return received, nil
}
