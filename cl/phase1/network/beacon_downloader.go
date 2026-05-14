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
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// Input: the currently highest slot processed, the list of blocks we want to process,
// and a map of beacon block root -> envelope for GLOAS FULL blocks.
// Output: the new last new highest slot processed and an error possibly?
type ProcessFn func(
	highestSlotProcessed uint64,
	blocks []*cltypes.SignedBeaconBlock,
	envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (
	newHighestSlotProcessed uint64,
	err error)

type ForwardBeaconDownloader struct {
	ctx                   context.Context
	highestSlotProcessed  uint64
	highestSlotUpdateTime time.Time
	minSlot               uint64 // earliest requestable slot (e.g. checkpoint anchor)
	rpc                   *rpc.BeaconRpcP2P
	process               ProcessFn
	beaconCfg             *clparams.BeaconChainConfig
	httpFallbackURL       string      // beacon API base URL for HTTP fallback when P2P fails
	httpPreferred         atomic.Bool // set after first HTTP fallback success; skips P2P probing

	mu sync.Mutex
}

func NewForwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P, beaconCfg *clparams.BeaconChainConfig) *ForwardBeaconDownloader {
	return &ForwardBeaconDownloader{
		ctx:       ctx,
		rpc:       rpc,
		beaconCfg: beaconCfg,
	}
}

// SetProcessFunction sets the function used to process segments.
func (f *ForwardBeaconDownloader) SetProcessFunction(fn ProcessFn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.process = fn
}

// SetHTTPFallbackURL sets the beacon API base URL for HTTP-based block fetching
// when P2P blocks_by_range requests fail. Derived from the checkpoint sync URL.
func (f *ForwardBeaconDownloader) SetHTTPFallbackURL(checkpointSyncURL string) {
	if checkpointSyncURL == "" {
		return
	}
	idx := strings.Index(checkpointSyncURL, "/eth/")
	if idx >= 0 {
		f.httpFallbackURL = checkpointSyncURL[:idx]
	} else {
		// Accept bare base URL (no /eth/ path).
		f.httpFallbackURL = strings.TrimRight(checkpointSyncURL, "/")
	}
}

// SetMinSlot sets the earliest slot the downloader may request.
// After checkpoint sync the state only exists from the anchor slot onward,
// so the overlap (highestSlotProcessed-2) must not reach below this bound.
func (f *ForwardBeaconDownloader) SetMinSlot(slot uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.minSlot = slot
}

// SetHighestProcessedSlot sets the highest processed slot so far.
func (f *ForwardBeaconDownloader) SetHighestProcessedSlot(highestSlotProcessed uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if highestSlotProcessed > f.highestSlotProcessed {
		f.highestSlotProcessed = highestSlotProcessed
		f.highestSlotUpdateTime = time.Now()
	}
}

type peerAndBlocks struct {
	peerId string
	blocks []*cltypes.SignedBeaconBlock
}

func (f *ForwardBeaconDownloader) RequestMore(ctx context.Context) {
	count := uint64(32)
	var atomicResp atomic.Value
	atomicResp.Store(peerAndBlocks{})

	// Fast path: when HTTP has been working, skip P2P probing entirely.
	if f.httpPreferred.Load() && f.httpFallbackURL != "" {
		httpStart := f.highestSlotProcessed + 1
		httpBlocks, httpErr := fetchBlocksFromBeaconAPI(ctx, f.httpFallbackURL, httpStart, count+10, f.beaconCfg)
		if httpErr == nil && len(httpBlocks) > 0 {
			atomicResp.Store(peerAndBlocks{"http-fallback", httpBlocks})
		} else {
			// HTTP failed — fall back to P2P probing.
			f.httpPreferred.Store(false)
		}
		if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
			goto Process
		}
	}

	{
		// Start with a base interval; backoff increases it on repeated failures.
		baseInterval := 300 * time.Millisecond
		var consecutiveFailures atomic.Int32
		reqInterval := time.NewTicker(baseInterval)
		defer reqInterval.Stop()
		// Timeout: if no blocks received within this duration, return to let the caller
		// run stale detection and potentially switch to ChainTipSync.
		requestTimeout := time.NewTimer(30 * time.Second)
		defer requestTimeout.Stop()

	Loop:
		for {
			select {
			case <-reqInterval.C:
				go func() {
					if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
						return
					}
					var reqSlot uint64
					if f.highestSlotProcessed > 2 {
						reqSlot = f.highestSlotProcessed - 2
					}
					if reqSlot < f.minSlot {
						reqSlot = f.minSlot
					}
					// Request one extra block beyond the batch for GLOAS lookahead:
					// the extra block lets determineFullGloasRoots check whether the
					// last batch block is FULL or EMPTY, instead of guessing FULL.
					reqCount := count + 1

					// Cap the request at the next fork epoch boundary. The Eth2 spec
					// says peers SHOULD NOT serve blocks across fork boundaries in a
					// single BeaconBlocksByRange response.
					if f.beaconCfg != nil {
						reqSlot, reqCount = f.capAtForkBoundary(reqSlot, reqCount)
					}

					// leave a warning if we are stuck for more than 90 seconds
					if time.Since(f.highestSlotUpdateTime) > 90*time.Second {
						log.Trace("Forward beacon downloader gets stuck", "time", time.Since(f.highestSlotUpdateTime).Seconds(), "highestSlotProcessed", f.highestSlotProcessed)
					}
					responses, peerId, err := f.rpc.SendBeaconBlocksByRangeReq(ctx, reqSlot, reqCount)
					if err != nil {
						if errors.Is(err, peers.ErrNoPeers) {
							log.Debug("[Caplin] no peers available for beacon blocks by range request", "slot", reqSlot, "reqCount", reqCount)
						} else {
							// Peer returned an error response (e.g. rate limited, invalid request).
							// Do NOT ban — apply backoff instead.
							log.Debug("Beacon blocks by range request failed", "err", err, "peer", peerId, "slot", reqSlot, "reqCount", reqCount)
						}
						// Exponential backoff: 300ms, 600ms, 1.2s, 2.4s, capped at 5s
						failures := int(consecutiveFailures.Add(1))

						// HTTP fallback: after many consecutive P2P failures, try beacon API.
						// Start from highestSlotProcessed+1 (not reqSlot which includes overlap
						// before the anchor that would fail with ErrMissingSegment).
						// Request extra slots beyond count for GLOAS lookahead (sparse slots may
						// leave no block at exactly count+1; extra range ensures the lookahead).
						if failures >= 5 && f.httpFallbackURL != "" {
							if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
								return
							}
							httpStart := f.highestSlotProcessed + 1
							httpBlocks, httpErr := fetchBlocksFromBeaconAPI(ctx, f.httpFallbackURL, httpStart, count+10, f.beaconCfg)
							if httpErr == nil && len(httpBlocks) > 0 {
								log.Debug("[ForwardBeaconDownloader] P2P failed, fetched blocks from beacon API",
									"fromSlot", httpStart, "count", len(httpBlocks))
								consecutiveFailures.Store(0)
								f.httpPreferred.Store(true)
								atomicResp.Store(peerAndBlocks{"http-fallback", httpBlocks})
								return
							}
							if httpErr != nil {
								log.Debug("[ForwardBeaconDownloader] HTTP fallback also failed", "err", httpErr)
							}
						}

						backoff := baseInterval * time.Duration(1<<uint(min(failures, 4)))
						if backoff > 5*time.Second {
							backoff = 5 * time.Second
						}
						reqInterval.Reset(backoff)
						return
					}
					if responses == nil {
						return
					}
					if len(responses) == 0 {
						// Empty response: no blocks in this slot range.
						// Advance past the requested range so we don't get stuck requesting the same empty range.
						f.mu.Lock()
						newSlot := reqSlot + count
						if newSlot > f.highestSlotProcessed {
							log.Debug("Empty block range response, advancing past gap", "from", f.highestSlotProcessed, "to", newSlot, "peer", peerId)
							f.highestSlotProcessed = newSlot
							f.highestSlotUpdateTime = time.Now()
						}
						f.mu.Unlock()
						return
					}
					// Success: reset backoff
					consecutiveFailures.Store(0)
					reqInterval.Reset(baseInterval)
					if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
						return
					}
					atomicResp.Store(peerAndBlocks{peerId, responses})
				}()
			case <-ctx.Done():
				return
			case <-requestTimeout.C:
				// No blocks received in time — return to let stale detection run.
				return
			default:
				if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
					break Loop
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	} // end P2P probing block

Process:
	resp := atomicResp.Load().(peerAndBlocks)
	processBlocks := resp.blocks
	pid := resp.peerId

	sort.Slice(processBlocks, func(i, j int) bool {
		return processBlocks[i].Block.Slot < processBlocks[j].Block.Slot
	})

	// For GLOAS blocks, fetch envelopes only for FULL blocks (whose payload was delivered).
	// EMPTY blocks never have envelopes on the network, so requesting them causes a 30s stall.
	// We determine FULL/EMPTY by comparing consecutive blocks' bids:
	// block[i+1].bid.ParentBlockHash == block[i].bid.BlockHash → block[i] is FULL.
	//
	// We requested count+1 blocks so the extra lookahead block lets us determine the
	// last batch block's FULL/EMPTY status accurately. Use all blocks for determination,
	// then trim to `count` before processing.
	var envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	if anyGloasBlock(processBlocks) {
		// Always keep at least 1 block as lookahead so the last processed
		// block's FULL/EMPTY status is determined from the actual next block
		// rather than guessed as EMPTY.  Without this, a FULL block at the
		// batch boundary has its envelope skipped, and the next batch's first
		// block fails with ErrParentEnvelopePending.
		processCount := min(int(count), len(processBlocks)-1)
		if processCount < 1 {
			processCount = len(processBlocks) // single block: process it (best-effort)
		}
		fullRoots := determineFullGloasRoots(processBlocks, processCount)
		processBlocks = processBlocks[:processCount]
		if len(fullRoots) > 0 {
			// When blocks came from HTTP fallback, P2P is known-broken for this
			// batch — skip the 30s P2P envelope timeout and fetch directly via HTTP.
			if pid == "http-fallback" && f.httpFallbackURL != "" {
				envelopes = make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
				httpEnvs := fetchEnvelopesFromBeaconAPI(ctx, f.httpFallbackURL, processBlocks, fullRoots, envelopes, f.beaconCfg)
				if httpEnvs > 0 {
					log.Debug("[ForwardBeaconDownloader] fetched envelopes from beacon API", "count", httpEnvs)
				}
			} else {
				var envErr error
				envelopes, envErr = RequestEnvelopesFrantically(ctx, f.rpc, fullRoots, processBlocks...)
				if envErr != nil {
					log.Debug("[ForwardBeaconDownloader] failed to get envelopes via P2P", "err", envErr)
				}
				// HTTP fallback for envelopes when P2P returned incomplete results
				if f.httpFallbackURL != "" && len(envelopes) < len(fullRoots) {
					if envelopes == nil {
						envelopes = make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
					}
					httpEnvs := fetchEnvelopesFromBeaconAPI(ctx, f.httpFallbackURL, processBlocks, fullRoots, envelopes, f.beaconCfg)
					if httpEnvs > 0 {
						log.Debug("[ForwardBeaconDownloader] fetched envelopes from beacon API", "count", httpEnvs)
					}
				}
			}
			log.Debug("[ForwardBeaconDownloader] envelope fetch result",
				"fullRoots", len(fullRoots), "received", len(envelopes),
				"batchBlocks", len(processBlocks),
				"firstSlot", processBlocks[0].Block.Slot,
				"lastSlot", processBlocks[len(processBlocks)-1].Block.Slot)
		}
	} else if uint64(len(processBlocks)) > count {
		// Non-GLOAS: still trim the extra lookahead block.
		processBlocks = processBlocks[:count]
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	var highestSlotProcessed uint64
	var err error
	if highestSlotProcessed, err = f.process(f.highestSlotProcessed, processBlocks, envelopes); err != nil {
		if pid != "http-fallback" {
			f.rpc.BanPeer(pid)
		}
		return
	}
	if highestSlotProcessed > f.highestSlotProcessed {
		f.highestSlotProcessed = highestSlotProcessed
		f.highestSlotUpdateTime = time.Now()
	}
}

// anyGloasBlock returns true if any block in the list is GLOAS version or later.
func anyGloasBlock(blocks []*cltypes.SignedBeaconBlock) bool {
	for _, block := range blocks {
		if block.Version() >= clparams.GloasVersion {
			return true
		}
	}
	return false
}

// determineFullGloasRoots uses consecutive blocks in a sorted batch to determine which
// GLOAS blocks are FULL (payload was delivered). A block[i] is FULL when:
//
//	block[i+1].bid.ParentBlockHash == block[i].bid.BlockHash
//
// processCount is the number of blocks to return roots for. blocks may contain one extra
// lookahead block beyond processCount to determine the last batch block's FULL/EMPTY status.
// Only roots for blocks[:processCount] are returned; the lookahead block's root is never included.
func determineFullGloasRoots(blocks []*cltypes.SignedBeaconBlock, processCount int) [][32]byte {
	var roots [][32]byte
	for i := 0; i < processCount && i < len(blocks); i++ {
		block := blocks[i]
		if block.Version() < clparams.GloasVersion {
			continue
		}
		bid := block.Block.Body.GetSignedExecutionPayloadBid()
		if bid == nil || bid.Message == nil {
			continue
		}

		isFull := false
		if i+1 < len(blocks) {
			nextBlock := blocks[i+1]
			if nextBlock.Version() >= clparams.GloasVersion {
				nextBid := nextBlock.Block.Body.GetSignedExecutionPayloadBid()
				if nextBid != nil && nextBid.Message != nil {
					isFull = nextBid.Message.ParentBlockHash == bid.Message.BlockHash
				}
			}
		}

		if isFull {
			root, err := block.Block.HashSSZ()
			if err == nil {
				roots = append(roots, root)
			}
		}
	}
	return roots
}

// capAtForkBoundary ensures the range [reqSlot, reqSlot+reqCount) does not
// cross a fork epoch boundary. Peers reject cross-fork range requests with
// error code 3 (resource unavailable).
//
// When a boundary falls within the overlap region (reqSlot..highestSlotProcessed],
// the overlap slots are already processed, so we advance reqSlot past the
// boundary instead of capping — otherwise the downloader re-requests the same
// already-processed slots and never makes progress.
func (f *ForwardBeaconDownloader) capAtForkBoundary(reqSlot, reqCount uint64) (uint64, uint64) {
	slotsPerEpoch := f.beaconCfg.SlotsPerEpoch
	forkEpochs := []uint64{
		f.beaconCfg.AltairForkEpoch,
		f.beaconCfg.BellatrixForkEpoch,
		f.beaconCfg.CapellaForkEpoch,
		f.beaconCfg.DenebForkEpoch,
		f.beaconCfg.ElectraForkEpoch,
		f.beaconCfg.FuluForkEpoch,
		f.beaconCfg.GloasForkEpoch,
	}

	var boundaries []uint64
	for _, epoch := range forkEpochs {
		if epoch == 0 || epoch == math.MaxUint64 {
			continue
		}
		boundaries = append(boundaries, epoch*slotsPerEpoch)
	}
	sort.Slice(boundaries, func(i, j int) bool { return boundaries[i] < boundaries[j] })

	endSlot := reqSlot + reqCount
	for _, boundarySlot := range boundaries {
		if boundarySlot <= reqSlot {
			continue
		}
		if boundarySlot >= endSlot {
			break
		}
		// boundarySlot is in (reqSlot, endSlot).
		if boundarySlot <= f.highestSlotProcessed+1 {
			// Already processed past this boundary — skip the pre-boundary
			// overlap and start from the boundary so the request stays
			// within a single fork.
			reqSlot = boundarySlot
		} else {
			// Haven't reached this boundary yet — cap the request here.
			reqCount = boundarySlot - reqSlot
			return reqSlot, reqCount
		}
	}
	reqCount = endSlot - reqSlot
	return reqSlot, reqCount
}

// fetchBlocksFromBeaconAPI fetches blocks from a beacon API endpoint as a fallback
// when P2P blocks_by_range requests fail. Skipped slots (404) are silently ignored.
func fetchBlocksFromBeaconAPI(ctx context.Context, baseURL string, startSlot, count uint64, beaconCfg *clparams.BeaconChainConfig) ([]*cltypes.SignedBeaconBlock, error) {
	type slotResult struct {
		slot  uint64
		block *cltypes.SignedBeaconBlock
		err   error
	}

	results := make([]slotResult, count)
	client := &http.Client{Timeout: 10 * time.Second}
	sem := make(chan struct{}, 8) // limit concurrent requests
	var wg sync.WaitGroup

	for i := uint64(0); i < count; i++ {
		slot := startSlot + i
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			results[idx].slot = slot
			reqURL := fmt.Sprintf("%s/eth/v2/beacon/blocks/%d", baseURL, slot)
			req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
			if err != nil {
				results[idx].err = err
				return
			}
			req.Header.Set("Accept", "application/octet-stream")

			resp, err := client.Do(req)
			if err != nil {
				results[idx].err = fmt.Errorf("HTTP block fetch slot %d: %w", slot, err)
				return
			}
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				results[idx].err = fmt.Errorf("HTTP block read slot %d: %w", slot, readErr)
				return
			}
			if resp.StatusCode == http.StatusNotFound {
				return // Skipped slot — block stays nil
			}
			if resp.StatusCode != http.StatusOK {
				results[idx].err = fmt.Errorf("HTTP block fetch slot %d: status %d", slot, resp.StatusCode)
				return
			}

			version := httpConsensusVersion(resp.Header.Get("Eth-Consensus-Version"))
			block := cltypes.NewSignedBeaconBlock(beaconCfg, version)
			if err := block.DecodeSSZ(body, int(version)); err != nil {
				results[idx].err = fmt.Errorf("HTTP block decode slot %d: %w", slot, err)
				return
			}
			results[idx].block = block
		}()
	}
	wg.Wait()

	var blocks []*cltypes.SignedBeaconBlock
	for _, r := range results {
		if r.err != nil {
			return blocks, r.err
		}
		if r.block != nil {
			blocks = append(blocks, r.block)
		}
	}
	return blocks, nil
}

// httpConsensusVersion maps the Eth-Consensus-Version header to a StateVersion.
func httpConsensusVersion(header string) clparams.StateVersion {
	switch strings.ToLower(header) {
	case "phase0":
		return clparams.Phase0Version
	case "altair":
		return clparams.AltairVersion
	case "bellatrix":
		return clparams.BellatrixVersion
	case "capella":
		return clparams.CapellaVersion
	case "deneb":
		return clparams.DenebVersion
	case "electra":
		return clparams.ElectraVersion
	case "fulu":
		return clparams.FuluVersion
	case "gloas", "glamsterdam":
		return clparams.GloasVersion
	default:
		return clparams.GloasVersion
	}
}

// fetchEnvelopesFromBeaconAPI fetches execution payload envelopes from the beacon API
// for FULL blocks whose envelopes were not received via P2P.
func fetchEnvelopesFromBeaconAPI(
	ctx context.Context,
	baseURL string,
	blocks []*cltypes.SignedBeaconBlock,
	fullRoots [][32]byte,
	received map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope,
	beaconCfg *clparams.BeaconChainConfig,
) int {
	// Build root-to-slot mapping from blocks
	rootToSlot := make(map[common.Hash]uint64, len(blocks))
	for _, blk := range blocks {
		root, err := blk.Block.HashSSZ()
		if err == nil {
			rootToSlot[root] = blk.Block.Slot
		}
	}

	type envResult struct {
		hash     common.Hash
		envelope *cltypes.SignedExecutionPayloadEnvelope
	}

	// Filter roots that need fetching
	var toFetch []struct {
		root [32]byte
		slot uint64
	}
	for _, root := range fullRoots {
		h := common.Hash(root)
		if _, ok := received[h]; ok {
			continue
		}
		slot, ok := rootToSlot[h]
		if !ok {
			continue
		}
		toFetch = append(toFetch, struct {
			root [32]byte
			slot uint64
		}{root, slot})
	}

	if len(toFetch) == 0 {
		return 0
	}

	results := make([]envResult, len(toFetch))
	client := &http.Client{Timeout: 10 * time.Second}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup

	for i, item := range toFetch {
		idx := i
		slot := item.slot
		root := item.root
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			reqURL := fmt.Sprintf("%s/eth/v1/beacon/execution_payload_envelope/%d", baseURL, slot)
			req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
			if err != nil {
				return
			}
			req.Header.Set("Accept", "application/octet-stream")

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != http.StatusOK {
				return
			}

			envelope := &cltypes.SignedExecutionPayloadEnvelope{
				Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg),
			}
			if err := envelope.DecodeSSZ(body, int(clparams.GloasVersion)); err != nil {
				log.Debug("[ForwardBeaconDownloader] HTTP envelope decode failed", "slot", slot, "err", err)
				return
			}
			results[idx] = envResult{hash: common.Hash(root), envelope: envelope}
		}()
	}
	wg.Wait()

	fetched := 0
	for _, r := range results {
		if r.envelope != nil {
			received[r.hash] = r.envelope
			fetched++
		}
	}
	return fetched
}

// GetHighestProcessedSlot retrieve the highest processed slot we accumulated.
func (f *ForwardBeaconDownloader) GetHighestProcessedSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestSlotProcessed
}

func (f *ForwardBeaconDownloader) Peers() (uint64, error) {
	return f.rpc.Peers()
}
