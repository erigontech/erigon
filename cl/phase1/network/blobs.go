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
var requestBlobRetryInterval = 100 * time.Millisecond

const (
	maxConcurrentBlobRequests = 2
	requestBlobMaxBackoff     = time.Second
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

type blobRequester interface {
	SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)
}

type blobRequestResult struct {
	peer      string
	responses []*cltypes.BlobSidecar
	err       error
}

// RequestBlobsFrantically requests blobs from the network frantically.
func RequestBlobsFrantically(ctx context.Context, r blobRequester, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
	return requestBlobsFrantically(ctx, r, req, nil)
}

func requestBlobsFrantically(ctx context.Context, r blobRequester, req *solid.ListSSZ[*cltypes.BlobIdentifier], accept func(context.Context, *PeerAndSidecars) error) (*PeerAndSidecars, error) {
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.NewTimer(requestBlobBatchExpiration)
	defer timer.Stop()
	reqInterval := time.NewTicker(requestBlobRetryInterval)
	defer reqInterval.Stop()
	results := make(chan blobRequestResult, maxConcurrentBlobRequests)
	validationResults := make(chan error, 1)
	inFlight := 0
	var candidate *PeerAndSidecars
	var resultC <-chan blobRequestResult = results
	var validationC <-chan error
	backoff := requestBlobRetryInterval
	nextRequest := time.Time{}
	for {
		select {
		case now := <-reqInterval.C:
			if inFlight >= cap(results) || now.Before(nextRequest) {
				continue
			}
			inFlight++
			go func() {
				responses, peer, err := r.SendBlobsSidecarByIdentifierReq(requestCtx, req)
				results <- blobRequestResult{peer: peer, responses: responses, err: err}
			}()
		case result := <-resultC:
			inFlight--
			if result.err != nil {
				log.Trace("RequestBlobsFrantically: error", "err", result.err, "peer", result.peer)
				backoff = min(backoff*2, requestBlobMaxBackoff)
				nextRequest = time.Now().Add(backoff)
				continue
			}
			backoff = requestBlobRetryInterval
			nextRequest = time.Time{}
			if len(result.responses) == 0 {
				log.Trace("RequestBlobsFrantically: response is empty", "peer", result.peer)
				continue
			}
			candidate = &PeerAndSidecars{Peer: result.peer, Responses: result.responses}
			if accept != nil {
				resultC = nil
				validationC = validationResults
				go func() { validationResults <- accept(requestCtx, candidate) }()
				continue
			}
			return candidate, nil
		case err := <-validationC:
			if err == nil {
				return candidate, nil
			}
			log.Trace("RequestBlobsFrantically: rejected response", "err", err, "peer", candidate.Peer)
			backoff = min(backoff*2, requestBlobMaxBackoff)
			nextRequest = time.Now().Add(backoff)
			candidate = nil
			validationC = nil
			resultC = results
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			log.Trace("RequestBlobsFrantically: timeout")
			return nil, ErrTimeout
		}
	}
}
