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
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common/log/v3"
)

func validateBlobResponseCandidate(req *solid.ListSSZ[*cltypes.BlobIdentifier], responses []*cltypes.BlobSidecar) error {
	type identity struct {
		root  [32]byte
		index uint64
	}
	requested := make(map[identity]struct{}, req.Len())
	req.Range(func(_ int, value *cltypes.BlobIdentifier, _ int) bool {
		requested[identity{root: value.BlockRoot, index: value.Index}] = struct{}{}
		return true
	})
	seen := make(map[identity]struct{}, len(responses))
	for _, sidecar := range responses {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil {
			return errors.New("blob response contains incomplete sidecar")
		}
		root, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return err
		}
		key := identity{root: root, index: sidecar.Index}
		if _, ok := requested[key]; !ok {
			return fmt.Errorf("blob response contains unrequested identity %x:%d", root, sidecar.Index)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("blob response contains duplicate identity %x:%d", root, sidecar.Index)
		}
		seen[key] = struct{}{}
	}
	return blob_storage.VerifyBlobSidecars(responses, nil)
}

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

type BlobPeerClient interface {
	Peers() (uint64, error)
	SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)
}

type blobBackfillPeerClient interface {
	SendBlobsSidecarByIdentifierReqForBackfill(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)
}

type blobPeerRejecter interface {
	BanPeer(string)
}

type blobRequestResult struct {
	peer      string
	responses []*cltypes.BlobSidecar
	err       error
}

type blobRequestPacing struct {
	backoff     time.Duration
	nextRequest time.Time
}

func newBlobRequestPacing() blobRequestPacing {
	return blobRequestPacing{backoff: requestBlobRetryInterval}
}

func (p *blobRequestPacing) ready(now time.Time) bool {
	return !now.Before(p.nextRequest)
}

func (p *blobRequestPacing) failed(now time.Time) {
	p.backoff = min(p.backoff*2, requestBlobMaxBackoff)
	p.nextRequest = now.Add(p.backoff)
}

func (p *blobRequestPacing) reset() {
	p.backoff = requestBlobRetryInterval
	p.nextRequest = time.Time{}
}

func (p *blobRequestPacing) complete(now time.Time, err error) {
	if err != nil {
		p.failed(now)
		return
	}
	p.reset()
}

// RequestBlobsFrantically requests blobs from the network frantically.
func RequestBlobsFrantically(ctx context.Context, r BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
	return requestBlobsFrantically(ctx, req, r.SendBlobsSidecarByIdentifierReq, blobPeerRejecterFor(r))
}

func requestBlobsFranticallyForBackfill(ctx context.Context, r blobBackfillPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
	return requestBlobsFrantically(ctx, req, r.SendBlobsSidecarByIdentifierReqForBackfill, blobPeerRejecterFor(r))
}

func blobPeerRejecterFor(client any) func(string) {
	if rejecter, ok := client.(blobPeerRejecter); ok {
		return rejecter.BanPeer
	}
	return nil
}

func requestBlobsFrantically(ctx context.Context, req *solid.ListSSZ[*cltypes.BlobIdentifier], send func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error), rejectPeer func(string)) (*PeerAndSidecars, error) {
	return requestBlobsFranticallyValidated(ctx, req, send, rejectPeer, nil)
}

func requestBlobsFranticallyValidated(ctx context.Context, req *solid.ListSSZ[*cltypes.BlobIdentifier], send func(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error), rejectPeer func(string), validate func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
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
	pacing := newBlobRequestPacing()
	launch := func() {
		inFlight++
		go func() {
			responses, peer, err := send(requestCtx, req)
			select {
			case results <- blobRequestResult{peer: peer, responses: responses, err: err}:
			case <-requestCtx.Done():
			}
		}()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	launch()
	for {
		select {
		case now := <-reqInterval.C:
			if inFlight >= cap(results) || !pacing.ready(now) {
				continue
			}
			launch()
		case result := <-resultC:
			inFlight--
			if result.err != nil {
				pacing.failed(time.Now())
				log.Trace("RequestBlobsFrantically: error", "err", result.err, "peer", result.peer)
				continue
			}
			if len(result.responses) == 0 {
				pacing.reset()
				log.Trace("RequestBlobsFrantically: response is empty", "peer", result.peer)
				continue
			}
			candidate = &PeerAndSidecars{Peer: result.peer, Responses: result.responses}
			resultC = nil
			validationC = validationResults
			go func(candidate *PeerAndSidecars) {
				err := validateBlobResponseCandidate(req, candidate.Responses)
				if err == nil && validate != nil {
					err = validate(candidate.Responses)
				}
				select {
				case validationResults <- err:
				case <-requestCtx.Done():
				}
			}(candidate)
		case err := <-validationC:
			if err == nil {
				return candidate, nil
			}
			if rejectPeer != nil && candidate.Peer != "" {
				rejectPeer(candidate.Peer)
			}
			log.Trace("RequestBlobsFrantically: rejected response", "err", err, "peer", candidate.Peer)
			pacing.failed(time.Now())
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
