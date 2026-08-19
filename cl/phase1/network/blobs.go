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
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/common/log/v3"
)

var ErrTimeout = errors.New("timeout")

const (
	requestBlobBatchExpiration       = 15 * time.Second
	requestBlobRetryInterval         = 100 * time.Millisecond
	requestBlobMaxBackoff            = 2 * time.Second
	maxConcurrentBlobBackfillRequest = 2
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
	requested *solid.ListSSZ[*cltypes.BlobIdentifier]
}

type blobRequester interface {
	SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)
}

type blobRequestResult struct {
	peer      string
	responses []*cltypes.BlobSidecar
	requested *solid.ListSSZ[*cltypes.BlobIdentifier]
	err       error
}

type blobBackfillRequestPacing struct {
	backoff     time.Duration
	nextRequest time.Time
}

func newBlobBackfillRequestPacing() blobBackfillRequestPacing {
	return blobBackfillRequestPacing{backoff: requestBlobRetryInterval}
}

func (p *blobBackfillRequestPacing) ready(now time.Time) bool {
	return !now.Before(p.nextRequest)
}

func (p *blobBackfillRequestPacing) failed(now time.Time) {
	p.nextRequest = now.Add(p.backoff)
	p.backoff = min(p.backoff*2, requestBlobMaxBackoff)
}

func (p *blobBackfillRequestPacing) reset(now time.Time) {
	p.backoff = requestBlobRetryInterval
	p.nextRequest = now.Add(requestBlobRetryInterval)
}

func (p *blobBackfillRequestPacing) recordValidation(now time.Time, progress bool, err error) {
	if progress && err == nil {
		p.reset(now)
		return
	}
	p.failed(now)
}

type blobBackfillRequestSchedule struct {
	ticks           <-chan time.Time
	expires         <-chan time.Time
	now             func() time.Time
	validationReady func()
}

type blobBackfillCandidateAcceptor func(context.Context, *PeerAndSidecars) (progress, complete bool, err error)
type blobBackfillRequestFactory func() *solid.ListSSZ[*cltypes.BlobIdentifier]

func requestBlobsForBackfill(ctx context.Context, r blobRequester, req blobBackfillRequestFactory, accept blobBackfillCandidateAcceptor) (*PeerAndSidecars, error) {
	ticker := time.NewTicker(requestBlobRetryInterval)
	defer ticker.Stop()
	timer := time.NewTimer(requestBlobBatchExpiration)
	defer timer.Stop()
	return requestBlobsForBackfillWithSchedule(ctx, r, req, accept, blobBackfillRequestSchedule{
		ticks:   ticker.C,
		expires: timer.C,
		now:     time.Now,
	})
}

type blobValidationResult struct {
	candidate *PeerAndSidecars
	progress  bool
	complete  bool
	err       error
}

func requestBlobsForBackfillWithSchedule(ctx context.Context, r blobRequester, req blobBackfillRequestFactory, acceptCandidate blobBackfillCandidateAcceptor, schedule blobBackfillRequestSchedule) (*PeerAndSidecars, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithCancel(ctx)
	var requestWorkers sync.WaitGroup
	defer func() {
		cancel()
		requestWorkers.Wait()
	}()
	results := make(chan blobRequestResult, maxConcurrentBlobBackfillRequest)
	validationResults := make(chan blobValidationResult, 1)
	inFlight := 0
	validating := false
	var retryCandidate *PeerAndSidecars
	pacing := newBlobBackfillRequestPacing()
	launch := func() {
		inFlight++
		requested := req()
		requestWorkers.Go(func() {
			responses, peer, err := r.SendBlobsSidecarByIdentifierReq(requestCtx, requested)
			select {
			case results <- blobRequestResult{peer: peer, responses: responses, requested: requested, err: err}:
			case <-requestCtx.Done():
			}
		})
	}
	startValidation := func(candidate *PeerAndSidecars) {
		validating = true
		go func() {
			progress, complete, err := acceptCandidate(requestCtx, candidate)
			validationResults <- blobValidationResult{candidate: candidate, progress: progress, complete: complete, err: err}
			if schedule.validationReady != nil {
				schedule.validationReady()
			}
		}()
	}
	waitForValidation := func() {
		if validating {
			<-validationResults
		}
	}
	handleRequest := func(result blobRequestResult) {
		inFlight--
		if result.err != nil {
			pacing.failed(schedule.now())
			log.Trace("requestBlobsForBackfill: error", "err", result.err, "peer", result.peer)
			return
		}
		startValidation(&PeerAndSidecars{Peer: result.peer, Responses: result.responses, requested: result.requested})
	}
	handleValidation := func(result blobValidationResult) *PeerAndSidecars {
		validating = false
		pacing.recordValidation(schedule.now(), result.progress, result.err)
		if result.err != nil || !result.progress {
			if result.err != nil && result.progress {
				retryCandidate = result.candidate
			}
			log.Trace("requestBlobsForBackfill: candidate rejected", "err", result.err, "peer", result.candidate.Peer)
			return nil
		}
		if result.complete {
			return result.candidate
		}
		return nil
	}
	launch()
	for {
		requestResults := (<-chan blobRequestResult)(results)
		if validating {
			requestResults = nil
		}
		select {
		case now := <-schedule.ticks:
			select {
			case result := <-validationResults:
				if response := handleValidation(result); response != nil {
					return response, nil
				}
			default:
			}
			if !validating && pacing.ready(now) {
				if retryCandidate != nil {
					candidate := retryCandidate
					retryCandidate = nil
					startValidation(candidate)
				} else if inFlight < maxConcurrentBlobBackfillRequest {
					launch()
				}
			}
		case result := <-requestResults:
			handleRequest(result)
		case result := <-validationResults:
			if response := handleValidation(result); response != nil {
				return response, nil
			}
		case <-ctx.Done():
			cancel()
			waitForValidation()
			return nil, ctx.Err()
		case <-schedule.expires:
			select {
			case result := <-validationResults:
				if response := handleValidation(result); response != nil {
					return response, nil
				}
			default:
			}
			cancel()
			waitForValidation()
			log.Trace("requestBlobsForBackfill: timeout")
			return nil, ErrTimeout
		}
	}
}

// RequestBlobsFrantically requests blobs from the network frantically.
func RequestBlobsFrantically(ctx context.Context, r *rpc.BeaconRpcP2P, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error) {
	var atomicResp atomic.Value

	atomicResp.Store(&PeerAndSidecars{})
	timer := time.NewTimer(requestBlobBatchExpiration)
	defer timer.Stop()
	reqInterval := time.NewTicker(100 * time.Millisecond)
	defer reqInterval.Stop()
Loop:
	for {
		select {
		case <-reqInterval.C:
			go func() {
				if len(atomicResp.Load().(*PeerAndSidecars).Responses) > 0 {
					return
				}
				// this is so we do not get stuck on a side-fork
				responses, pid, err := r.SendBlobsSidecarByIdentifierReq(ctx, req)
				if err != nil {
					log.Trace("RequestBlobsFrantically: error", "err", err, "peer", pid)
					return
				}
				if responses == nil {
					log.Trace("RequestBlobsFrantically: response is nil", "peer", pid)
					return
				}
				if len(atomicResp.Load().(*PeerAndSidecars).Responses) > 0 {
					return
				}
				atomicResp.Store(&PeerAndSidecars{
					Peer:      pid,
					Responses: responses,
				})
			}()
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			log.Trace("RequestBlobsFrantically: timeout")
			return nil, ErrTimeout
		default:
			if len(atomicResp.Load().(*PeerAndSidecars).Responses) > 0 {
				break Loop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return atomicResp.Load().(*PeerAndSidecars), nil
}
