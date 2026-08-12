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

package network

import (
	"context"
	"errors"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common/log/v3"
)

var ErrTimeout = errors.New("timeout")

var requestBlobBatchExpiration = 15 * time.Second

const (
	initialBlobRequestBackoff = 100 * time.Millisecond
	maxBlobRequestBackoff     = 2 * time.Second
	maxConcurrentBlobRequests = 2
)

// This is just a bunch of functions to handle blobs

// BlobsIdentifiersFromBlocks returns a list of blob identifiers from a list of blocks, which should then be forwarded to the network.
func BlobsIdentifiersFromBlocks(blocks []*cltypes.SignedBeaconBlock, cfg *clparams.BeaconChainConfig) (*solid.ListSSZ[*cltypes.BlobIdentifier], error) {
	ids := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	for _, block := range blocks {
		if block.Version() < clparams.DenebVersion {
			continue
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		commitments := block.Block.Body.GetBlobKzgCommitments()
		if commitments == nil {
			// [New in Gloas:EIP7732] EMPTY block: no ExecutionPayloadBid so no commitments.
			log.Debug("[BlobsIdentifiers] skipping block with nil kzg commitments", "slot", block.Block.Slot, "version", block.Version())
			continue
		}
		kzgCommitments := commitments.Len()
		if ids.Len()+kzgCommitments > cfg.MaxRequestBlobSidecarsByVersion(block.Version()) {
			break
		}
		for i := range kzgCommitments {
			ids.Append(&cltypes.BlobIdentifier{
				BlockRoot: blockRoot,
				Index:     uint64(i),
			})
		}
	}
	return ids, nil
}

type PeerAndSidecars struct {
	Peer      string
	Responses []*cltypes.BlobSidecar
}

type BlobPeerClient interface {
	Peers() (uint64, error)
	SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)
}

// RequestBlobsFrantically requests blobs from the network frantically.
func RequestBlobsFrantically(ctx context.Context, r BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
	type requestResult struct {
		responses []*cltypes.BlobSidecar
		peer      string
		err       error
	}
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	expiration := time.NewTimer(requestBlobBatchExpiration)
	defer expiration.Stop()
	retry := time.NewTimer(0)
	defer retry.Stop()
	retryC := retry.C
	results := make(chan requestResult, maxConcurrentBlobRequests)
	inFlight := 0
	backoff := initialBlobRequestBackoff
	resetRetry := func(delay time.Duration) {
		if !retry.Stop() {
			select {
			case <-retry.C:
			default:
			}
		}
		retry.Reset(delay)
		retryC = retry.C
	}
	launch := func() {
		inFlight++
		go func() {
			responses, peer, err := r.SendBlobsSidecarByIdentifierReq(attemptCtx, req)
			results <- requestResult{responses: responses, peer: peer, err: err}
		}()
	}
	for {
		select {
		case <-retryC:
			launch()
			if inFlight < maxConcurrentBlobRequests {
				resetRetry(initialBlobRequestBackoff)
			} else {
				retryC = nil
			}
		case result := <-results:
			inFlight--
			if result.err == nil && len(result.responses) > 0 {
				return &PeerAndSidecars{Peer: result.peer, Responses: result.responses}, nil
			}
			if result.err != nil {
				log.Trace("RequestBlobsFrantically: error", "err", result.err, "peer", result.peer)
			} else {
				log.Trace("RequestBlobsFrantically: response is empty", "peer", result.peer)
			}
			backoff = min(backoff*2, maxBlobRequestBackoff)
			resetRetry(backoff)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-expiration.C:
			log.Trace("RequestBlobsFrantically: timeout")
			return nil, ErrTimeout
		}
	}
}
