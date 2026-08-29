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
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
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

var ErrUnattributableProcess = errors.New("unattributable process error")

type ForwardBeaconDownloader struct {
	ctx                   context.Context
	highestSlotProcessed  uint64
	highestSlotUpdateTime time.Time
	minSlot               uint64 // earliest requestable slot (e.g. checkpoint anchor)
	rpc                   *rpc.BeaconRpcP2P
	requestBlocksByRange  func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error)
	banPeer               func(string)
	process               ProcessFn
	currentSlot           func() uint64
	beaconCfg             *clparams.BeaconChainConfig
	httpFallbackURL       string      // beacon API base URL for HTTP fallback when P2P fails
	httpPreferred         atomic.Bool // set after first HTTP fallback success; skips P2P probing
	onRequestComplete     func(response *peerAndBlocks, empty bool)

	mu                 sync.Mutex
	gloasLookahead     *cltypes.SignedBeaconBlock
	gloasNextUnscanned uint64
}

const forwardRequestRetryInterval = 300 * time.Millisecond

// SetCurrentSlotSampler limits range requests to slots that can already exist.
func (f *ForwardBeaconDownloader) SetCurrentSlotSampler(currentSlot func() uint64) {
	f.currentSlot = currentSlot
}

func NewForwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P, beaconCfg *clparams.BeaconChainConfig) *ForwardBeaconDownloader {
	f := &ForwardBeaconDownloader{
		ctx:       ctx,
		rpc:       rpc,
		beaconCfg: beaconCfg,
	}
	if rpc != nil {
		f.requestBlocksByRange = rpc.SendBeaconBlocksByRangeReq
		f.banPeer = rpc.BanPeer
	}
	return f
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
	before, _, found := strings.Cut(checkpointSyncURL, "/eth/")
	if found {
		f.httpFallbackURL = before
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
	if f.gloasLookahead != nil && f.gloasLookahead.Block.Slot < slot {
		f.clearGloasScan()
	}
}

// SetHighestProcessedSlot sets the highest processed slot so far.
func (f *ForwardBeaconDownloader) SetHighestProcessedSlot(highestSlotProcessed uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if highestSlotProcessed > f.highestSlotProcessed {
		f.highestSlotProcessed = highestSlotProcessed
		f.highestSlotUpdateTime = time.Now()
		if f.gloasLookahead != nil && f.gloasLookahead.Block.Slot <= highestSlotProcessed {
			f.clearGloasScan()
		}
	}
}

func (f *ForwardBeaconDownloader) progressSnapshot() (uint64, time.Time, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestSlotProcessed, f.highestSlotUpdateTime, f.minSlot
}

type peerAndBlocks struct {
	peerId                 string
	blocks                 []*cltypes.SignedBeaconBlock
	httpSampledHighestSlot uint64
	fromHTTP               bool
	rangeStart             uint64
	rangeCount             uint64
	hadGloasPending        bool
}

var (
	forwardBeaconRequestInterval = 300 * time.Millisecond
	forwardBeaconRequestTimeout  = 30 * time.Second
	forwardBeaconProbeTimeout    = 21 * time.Second
	forwardBeaconFallbackDelay   = 5 * time.Second
	forwardBeaconResponsePoll    = 10 * time.Millisecond
)

const maxConcurrentForwardBeaconRequests = 2

func (f *ForwardBeaconDownloader) RequestMore(ctx context.Context) {
	requestCtx, cancelRequests := context.WithTimeout(ctx, forwardBeaconRequestTimeout)
	defer cancelRequests()
	f.restartGloasScanAtHead()

	count := uint64(32)
	requestStart, _ := f.nextRequestStart(true)
	if f.capAtCurrentSlot(requestStart, 1) == 0 {
		waitForForwardRequestRetry(ctx)
		return
	}
	var atomicResp atomic.Value
	atomicResp.Store(peerAndBlocks{})
	commitHTTPBlocks := func(sampledHighestSlot, httpStart, httpCount uint64, hadGloasPending bool, blocks []*cltypes.SignedBeaconBlock) bool {
		if len(blocks) == 0 {
			return false
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.highestSlotProcessed != sampledHighestSlot || len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
			return false
		}
		log.Debug("[ForwardBeaconDownloader] fetched blocks from beacon API",
			"fromSlot", httpStart, "count", len(blocks))
		atomicResp.Store(peerAndBlocks{
			peerId:                 "http-fallback",
			blocks:                 blocks,
			httpSampledHighestSlot: sampledHighestSlot,
			fromHTTP:               true,
			rangeStart:             httpStart,
			rangeCount:             httpCount,
			hadGloasPending:        hadGloasPending,
		})
		return true
	}

	// Fast path: when HTTP has been working, skip P2P probing entirely.
	if f.httpPreferred.Load() && f.httpFallbackURL != "" {
		httpStart, hadGloasPending := f.nextRequestStart(false)
		httpCount := capRequestCount(httpStart, count+10)
		httpCount, completedHTTPCount := f.requestCountsAtCurrentSlot(httpStart, httpCount)
		if httpCount == 0 {
			waitForForwardRequestRetry(ctx)
			return
		}
		highestSlotProcessed, _, _ := f.progressSnapshot()
		httpBlocks, httpErr := fetchBlocksFromBeaconAPI(requestCtx, f.httpFallbackURL, httpStart, httpCount, f.beaconCfg)
		switch {
		case httpErr == nil && len(httpBlocks) > 0:
			if !commitHTTPBlocks(highestSlotProcessed, httpStart, httpCount, hadGloasPending, httpBlocks) {
				f.httpPreferred.Store(false)
			}
		case httpErr == nil && hadGloasPending:
			f.recordEmptyRange(httpStart, completedHTTPCount, true, "http-fallback")
			return
		default:
			// HTTP failed — fall back to P2P probing.
			f.httpPreferred.Store(false)
		}
		if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
			goto Process
		}
		if requestCtx.Err() != nil {
			return
		}
	}
	if f.requestBlocksByRange == nil {
		waitForForwardRequestRetry(ctx)
		return
	}

	{
		probeCtx, cancelProbes := context.WithCancel(requestCtx)
		var probeWG sync.WaitGroup
		probeSlots := make(chan struct{}, maxConcurrentForwardBeaconRequests)
		var httpFallbackRunning atomic.Bool
		noRequestableRange := make(chan struct{}, 1)
		stopProbes := func() {
			cancelProbes()
			probeWG.Wait()
		}
		defer stopProbes()
		var requestsMu sync.Mutex
		inFlightRequests := 0
		responseAccepted := false
		type emptyRangeResult struct {
			lastSlot uint64
			apply    func()
		}
		var pendingEmpty *emptyRangeResult
		beginRequest := func() bool {
			requestsMu.Lock()
			defer requestsMu.Unlock()
			if responseAccepted || pendingEmpty != nil {
				return false
			}
			inFlightRequests++
			return true
		}
		completeRequest := func(response *peerAndBlocks, empty *emptyRangeResult) {
			requestsMu.Lock()
			if response != nil && !responseAccepted {
				responseAccepted = true
				atomicResp.Store(*response)
			}
			if empty != nil && (pendingEmpty == nil || empty.lastSlot > pendingEmpty.lastSlot) {
				pendingEmpty = empty
			}
			inFlightRequests--
			if inFlightRequests == 0 {
				if !responseAccepted && pendingEmpty != nil {
					pendingEmpty.apply()
				}
				pendingEmpty = nil
			}
			requestsMu.Unlock()
			if f.onRequestComplete != nil {
				f.onRequestComplete(response, empty != nil)
			}
		}
		startHTTPFallback := func() {
			if f.httpFallbackURL == "" || !httpFallbackRunning.CompareAndSwap(false, true) {
				return
			}
			if !beginRequest() {
				httpFallbackRunning.Store(false)
				return
			}
			probeWG.Go(func() {
				var acceptedResponse *peerAndBlocks
				var emptyResponse *emptyRangeResult
				defer func() {
					httpFallbackRunning.Store(false)
					completeRequest(acceptedResponse, emptyResponse)
				}()

				latestHighestSlotProcessed, _, _ := f.progressSnapshot()
				httpStart, hadGloasPending := f.nextRequestStart(false)
				httpCount := capRequestCount(httpStart, count+10)
				httpCount, completedHTTPCount := f.requestCountsAtCurrentSlot(httpStart, httpCount)
				if httpCount == 0 {
					select {
					case noRequestableRange <- struct{}{}:
					default:
					}
					return
				}
				httpBlocks, httpErr := fetchBlocksFromBeaconAPI(probeCtx, f.httpFallbackURL, httpStart, httpCount, f.beaconCfg)
				if probeCtx.Err() != nil {
					return
				}
				if httpErr == nil && len(httpBlocks) > 0 {
					currentHighestSlotProcessed, _, _ := f.progressSnapshot()
					if currentHighestSlotProcessed == latestHighestSlotProcessed {
						response := peerAndBlocks{
							peerId:                 "http-fallback",
							blocks:                 httpBlocks,
							httpSampledHighestSlot: latestHighestSlotProcessed,
							fromHTTP:               true,
							rangeStart:             httpStart,
							rangeCount:             httpCount,
							hadGloasPending:        hadGloasPending,
						}
						acceptedResponse = &response
					}
					return
				}
				if httpErr == nil && hadGloasPending && completedHTTPCount > 0 {
					emptyResponse = &emptyRangeResult{
						lastSlot: lastSlotInRange(httpStart, completedHTTPCount),
						apply:    func() { f.recordEmptyRange(httpStart, completedHTTPCount, true, "http-fallback") },
					}
					return
				}
				if httpErr != nil {
					log.Debug("[ForwardBeaconDownloader] HTTP fallback also failed", "err", httpErr)
				}
			})
		}

		// Start with a base interval; backoff increases it on repeated failures.
		baseInterval := forwardBeaconRequestInterval
		var consecutiveFailures atomic.Int32
		reqInterval := time.NewTicker(baseInterval)
		defer reqInterval.Stop()
		var fallbackTimer *time.Timer
		var fallbackTimerC <-chan time.Time
		if f.httpFallbackURL != "" {
			fallbackTimer = time.NewTimer(forwardBeaconFallbackDelay)
			fallbackTimerC = fallbackTimer.C
			defer fallbackTimer.Stop()
		}

	Loop:
		for {
			select {
			case <-reqInterval.C:
				select {
				case probeSlots <- struct{}{}:
				default:
					continue
				}
				if !beginRequest() {
					<-probeSlots
					continue
				}
				probeWG.Go(func() {
					defer func() { <-probeSlots }()
					var acceptedResponse *peerAndBlocks
					var emptyResponse *emptyRangeResult
					defer func() { completeRequest(acceptedResponse, emptyResponse) }()
					if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
						return
					}
					highestSlotProcessed, highestSlotUpdateTime, _ := f.progressSnapshot()
					reqSlot, hadGloasPending := f.nextRequestStart(true)
					// Request one extra block beyond the batch for GLOAS lookahead:
					// the extra block lets determineFullGloasRoots check whether the
					// last batch block is FULL or EMPTY, instead of guessing FULL.
					reqCount := capRequestCount(reqSlot, count+1)

					// Cap the request at the next fork epoch boundary. The Eth2 spec
					// says peers SHOULD NOT serve blocks across fork boundaries in a
					// single BeaconBlocksByRange response.
					if f.beaconCfg != nil {
						reqSlot, reqCount = f.capAtForkBoundary(reqSlot, reqCount, highestSlotProcessed)
					}
					reqCount, completedReqCount := f.requestCountsAtCurrentSlot(reqSlot, reqCount)
					if reqCount == 0 {
						select {
						case noRequestableRange <- struct{}{}:
						default:
						}
						return
					}

					// leave a warning if we are stuck for more than 90 seconds
					if time.Since(highestSlotUpdateTime) > 90*time.Second {
						log.Trace("Forward beacon downloader gets stuck", "time", time.Since(highestSlotUpdateTime).Seconds(), "highestSlotProcessed", highestSlotProcessed)
					}
					attemptCtx, cancelAttempt := context.WithTimeout(probeCtx, forwardBeaconProbeTimeout)
					responses, peerId, err := f.requestBlocksByRange(attemptCtx, reqSlot, reqCount)
					cancelAttempt()
					if probeCtx.Err() != nil {
						return
					}
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
							startHTTPFallback()
						}

						backoff := min(baseInterval*time.Duration(1<<uint(min(failures, 4))), 5*time.Second)
						reqInterval.Reset(backoff)
						return
					}
					if responses == nil {
						return
					}
					if len(responses) == 0 {
						if hadGloasPending && completedReqCount > 0 {
							emptyResponse = &emptyRangeResult{
								lastSlot: lastSlotInRange(reqSlot, completedReqCount),
								apply:    func() { f.recordEmptyRange(reqSlot, completedReqCount, true, peerId) },
							}
							return
						}
						if hadGloasPending {
							return
						}
						failures := int(consecutiveFailures.Add(1))
						if failures >= 5 && f.httpFallbackURL != "" {
							startHTTPFallback()
						}
						backoff := min(baseInterval*time.Duration(1<<uint(min(failures, 4))), 5*time.Second)
						reqInterval.Reset(backoff)
						return
					}
					// Success: reset backoff
					consecutiveFailures.Store(0)
					reqInterval.Reset(baseInterval)
					response := peerAndBlocks{
						peerId:          peerId,
						blocks:          responses,
						rangeStart:      reqSlot,
						rangeCount:      reqCount,
						hadGloasPending: hadGloasPending,
					}
					acceptedResponse = &response
				})
			case <-noRequestableRange:
				waitForForwardRequestRetry(ctx)
				return
			case <-requestCtx.Done():
				// No blocks received in time — return to let stale detection run.
				stopProbes()
				return
			case <-fallbackTimerC:
				startHTTPFallback()
				fallbackTimerC = nil
			default:
				if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
					break Loop
				}
				time.Sleep(forwardBeaconResponsePoll)
			}
		}
		stopProbes()
	} // end P2P probing block

Process:
	resp := atomicResp.Load().(peerAndBlocks)
	processBlocks := resp.blocks
	pid := resp.peerId
	if resp.fromHTTP {
		highestSlotProcessed, _, _ := f.progressSnapshot()
		if highestSlotProcessed != resp.httpSampledHighestSlot {
			f.httpPreferred.Store(false)
			return
		}
	}
	slices.SortFunc(processBlocks, func(a, b *cltypes.SignedBeaconBlock) int {
		return cmp.Compare(a.Block.Slot, b.Block.Slot)
	})
	if anyGloasBlock(processBlocks) && !connectedGloasBlocks(processBlocks) {
		f.rejectInvalidGloasResponse(pid)
		return
	}
	f.mu.Lock()
	lookahead := f.gloasLookahead
	f.mu.Unlock()
	if lookahead != nil {
		processBlocks = mergeGloasLookahead(processBlocks, lookahead)
	}

	slices.SortFunc(processBlocks, func(a, b *cltypes.SignedBeaconBlock) int {
		return cmp.Compare(a.Block.Slot, b.Block.Slot)
	})
	hasGloasBlocks := anyGloasBlock(processBlocks)
	if hasGloasBlocks && !connectedGloasBlocks(processBlocks) {
		f.mu.Lock()
		if lookahead != nil && f.gloasLookahead == lookahead {
			f.clearGloasScan()
		}
		f.mu.Unlock()
		return
	}

	var envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	var nextGloasLookahead *cltypes.SignedBeaconBlock
	nextGloasCursor := nextSlotAfterRange(resp.rangeStart, resp.rangeCount)
	if hasGloasBlocks {
		// A Gloas block needs its successor to determine whether its envelope exists.
		processCount := min(int(count), len(processBlocks)-1)
		nextGloasLookahead = processBlocks[processCount]
		if pid != "http-fallback" || processCount+1 < len(processBlocks) {
			nextGloasCursor = saturatingIncrement(nextGloasLookahead.Block.Slot)
		}
		fullRoots := determineFullGloasRoots(processBlocks, processCount)
		processBlocks = processBlocks[:processCount]
		if len(fullRoots) > 0 {
			// When blocks came from HTTP fallback, P2P is known-broken for this
			// batch — skip the 30s P2P envelope timeout and fetch directly via HTTP.
			if pid == "http-fallback" && f.httpFallbackURL != "" {
				envelopes = make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
				httpResult := fetchEnvelopesFromBeaconAPI(ctx, f.httpFallbackURL, processBlocks, fullRoots, envelopes, f.beaconCfg)
				if httpResult.fetched > 0 {
					log.Debug("[ForwardBeaconDownloader] fetched envelopes from beacon API", "count", httpResult.fetched)
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
					httpResult := fetchEnvelopesFromBeaconAPI(ctx, f.httpFallbackURL, processBlocks, fullRoots, envelopes, f.beaconCfg)
					if httpResult.fetched > 0 {
						log.Debug("[ForwardBeaconDownloader] fetched envelopes from beacon API", "count", httpResult.fetched)
					}
				}
			}
			log.Debug("[ForwardBeaconDownloader] envelope fetch result",
				"fullRoots", len(fullRoots), "received", len(envelopes),
				"batchBlocks", len(processBlocks),
				"firstSlot", processBlocks[0].Block.Slot,
				"lastSlot", processBlocks[len(processBlocks)-1].Block.Slot)
			retained := retainBlocksBeforeMissingGloasEnvelope(processBlocks, fullRoots, envelopes)
			if len(retained) < len(processBlocks) {
				log.Debug("[ForwardBeaconDownloader] retaining frontier before missing GLOAS envelope",
					"retainedBlocks", len(retained), "batchBlocks", len(processBlocks))
				nextGloasLookahead = processBlocks[len(retained)]
				nextGloasCursor = saturatingIncrement(nextGloasLookahead.Block.Slot)
				processBlocks = retained
			}
		}
	} else if uint64(len(processBlocks)) > count {
		// Non-GLOAS: still trim the extra lookahead block.
		processBlocks = processBlocks[:count]
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if resp.fromHTTP && f.highestSlotProcessed != resp.httpSampledHighestSlot {
		f.httpPreferred.Store(false)
		return
	}
	if f.process == nil {
		return
	}

	previousHighestSlotProcessed := f.highestSlotProcessed
	highestSlotProcessed, err := f.process(previousHighestSlotProcessed, processBlocks, envelopes)
	if err != nil {
		if resp.fromHTTP {
			f.httpPreferred.Store(false)
		}
		if lookahead != nil && f.gloasLookahead == lookahead {
			f.clearGloasScan()
		}
		if lookahead == nil && shouldBanProcessPeer(pid, err) && f.banPeer != nil {
			f.banPeer(pid)
		}
		return
	}
	attemptedNewSlot := slices.ContainsFunc(processBlocks, func(block *cltypes.SignedBeaconBlock) bool {
		return block.Block.Slot > f.highestSlotProcessed
	})
	if attemptedNewSlot && nextGloasLookahead != nil && highestSlotProcessed <= f.highestSlotProcessed {
		if resp.fromHTTP {
			f.httpPreferred.Store(false)
		}
		if lookahead != nil && f.gloasLookahead == lookahead {
			f.clearGloasScan()
		}
		return
	}
	if resp.fromHTTP {
		f.httpPreferred.Store(highestSlotProcessed > previousHighestSlotProcessed || nextGloasLookahead != nil)
	}
	if highestSlotProcessed > f.highestSlotProcessed {
		f.highestSlotProcessed = highestSlotProcessed
		f.highestSlotUpdateTime = time.Now()
	}
	if nextGloasLookahead != nil {
		for _, block := range processBlocks {
			if block.Block.Slot > f.highestSlotProcessed {
				nextGloasLookahead = block
				nextGloasCursor = saturatingIncrement(block.Block.Slot)
				break
			}
		}
	}
	if nextGloasLookahead != nil && nextGloasLookahead.Block.Slot > f.highestSlotProcessed {
		f.gloasLookahead = nextGloasLookahead
		f.gloasNextUnscanned = max(f.gloasNextUnscanned, nextGloasCursor, saturatingIncrement(nextGloasLookahead.Block.Slot))
	} else {
		f.clearGloasScan()
	}
}

func waitForForwardRequestRetry(ctx context.Context) {
	timer := time.NewTimer(forwardRequestRetryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (f *ForwardBeaconDownloader) capAtCurrentSlot(start, count uint64) uint64 {
	requestCount, _ := f.requestCountsAtCurrentSlot(start, count)
	return requestCount
}

func (f *ForwardBeaconDownloader) requestCountsAtCurrentSlot(start, count uint64) (requestCount, completedCount uint64) {
	if count == 0 || f.currentSlot == nil {
		return count, count
	}
	currentSlot := f.currentSlot()
	if start > currentSlot {
		return 0, 0
	}
	requestCount = count
	if count-1 > currentSlot-start {
		requestCount = currentSlot - start + 1
	}
	if start >= currentSlot {
		return requestCount, 0
	}
	completedCount = min(count, currentSlot-start)
	return requestCount, completedCount
}

func (f *ForwardBeaconDownloader) nextRequestStart(overlap bool) (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gloasLookahead != nil {
		return f.gloasNextUnscanned, true
	}
	start := saturatingIncrement(f.highestSlotProcessed)
	if overlap && f.highestSlotProcessed > 2 {
		start = f.highestSlotProcessed - 2
	}
	if start < f.minSlot {
		start = f.minSlot
	}
	return start, false
}

func (f *ForwardBeaconDownloader) restartGloasScanAtHead() {
	if f.currentSlot == nil {
		return
	}
	currentSlot := f.currentSlot()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.gloasLookahead != nil && f.gloasNextUnscanned >= currentSlot {
		f.gloasNextUnscanned = saturatingIncrement(f.gloasLookahead.Block.Slot)
	}
}

func (f *ForwardBeaconDownloader) recordEmptyRange(start, count uint64, hadGloasPending bool, peerID string) {
	if count == 0 {
		return
	}
	lastSlot := lastSlotInRange(start, count)
	f.mu.Lock()
	defer f.mu.Unlock()
	if hadGloasPending || f.gloasLookahead != nil {
		if f.gloasLookahead != nil {
			f.gloasNextUnscanned = max(f.gloasNextUnscanned, saturatingIncrement(lastSlot))
		}
		return
	}
	if lastSlot > f.highestSlotProcessed {
		log.Debug("Empty block range response, advancing past gap", "from", f.highestSlotProcessed, "to", lastSlot, "peer", peerID)
		f.highestSlotProcessed = lastSlot
		f.highestSlotUpdateTime = time.Now()
	}
}

func (f *ForwardBeaconDownloader) clearGloasScan() {
	f.gloasLookahead = nil
	f.gloasNextUnscanned = 0
}

func mergeGloasLookahead(blocks []*cltypes.SignedBeaconBlock, lookahead *cltypes.SignedBeaconBlock) []*cltypes.SignedBeaconBlock {
	lookaheadRoot, lookaheadErr := lookahead.Block.HashSSZ()
	merged := make([]*cltypes.SignedBeaconBlock, 0, len(blocks)+1)
	merged = append(merged, lookahead)
	for _, block := range blocks {
		if lookaheadErr == nil {
			root, err := block.Block.HashSSZ()
			if err == nil && root == lookaheadRoot {
				continue
			}
		}
		merged = append(merged, block)
	}
	return merged
}

func connectedGloasBlocks(blocks []*cltypes.SignedBeaconBlock) bool {
	for i := 1; i < len(blocks); i++ {
		root, err := blocks[i-1].Block.HashSSZ()
		if err != nil || blocks[i].Block.ParentRoot != root {
			return false
		}
	}
	return true
}

const (
	banInvalidGloasResponse         = "ban-peer"
	disableHTTPInvalidGloasResponse = "disable-http"
)

func invalidGloasResponseAction(pid string) string {
	if pid == "http-fallback" {
		return disableHTTPInvalidGloasResponse
	}
	return banInvalidGloasResponse
}

func (f *ForwardBeaconDownloader) rejectInvalidGloasResponse(pid string) {
	switch invalidGloasResponseAction(pid) {
	case banInvalidGloasResponse:
		if f.banPeer != nil {
			f.banPeer(pid)
		}
	case disableHTTPInvalidGloasResponse:
		f.httpPreferred.Store(false)
	}
}

func capRequestCount(start, count uint64) uint64 {
	if count == 0 || count-1 <= math.MaxUint64-start {
		return count
	}
	return math.MaxUint64 - start + 1
}

func lastSlotInRange(start, count uint64) uint64 {
	if count == 0 {
		return start
	}
	return start + capRequestCount(start, count) - 1
}

func nextSlotAfterRange(start, count uint64) uint64 {
	return saturatingIncrement(lastSlotInRange(start, count))
}

func saturatingIncrement(slot uint64) uint64 {
	if slot == math.MaxUint64 {
		return slot
	}
	return slot + 1
}

func retainBlocksBeforeMissingGloasEnvelope(blocks []*cltypes.SignedBeaconBlock, fullRoots [][32]byte, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) []*cltypes.SignedBeaconBlock {
	for _, root := range fullRoots {
		if envelopes[common.Hash(root)] != nil {
			continue
		}
		for i, block := range blocks {
			blockRoot, err := block.Block.HashSSZ()
			if err != nil || blockRoot == root {
				return blocks[:i]
			}
		}
		return nil
	}
	return blocks
}

func shouldBanProcessPeer(pid string, err error) bool {
	return pid != "http-fallback" && !errors.Is(err, ErrUnattributableProcess)
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
func (f *ForwardBeaconDownloader) capAtForkBoundary(reqSlot, reqCount, highestSlotProcessed uint64) (uint64, uint64) {
	reqCount = capRequestCount(reqSlot, reqCount)
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
	slices.Sort(boundaries)

	for _, boundarySlot := range boundaries {
		if boundarySlot <= reqSlot {
			continue
		}
		distance := boundarySlot - reqSlot
		if distance >= reqCount {
			break
		}
		// boundarySlot is in (reqSlot, endSlot).
		if boundarySlot <= saturatingIncrement(highestSlotProcessed) {
			// Already processed past this boundary — skip the pre-boundary
			// overlap and start from the boundary so the request stays
			// within a single fork.
			reqSlot = boundarySlot
			reqCount -= distance
		} else {
			// Haven't reached this boundary yet — cap the request here.
			reqCount = distance
			return reqSlot, reqCount
		}
	}
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
	requestCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for i := range count {
		slot := startSlot + i
		idx := i
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-requestCtx.Done():
				results[idx].err = requestCtx.Err()
				return
			}
			defer func() { <-sem }()

			results[idx].slot = slot
			reqURL := fmt.Sprintf("%s/eth/v2/beacon/blocks/%d", baseURL, slot)
			req, err := http.NewRequestWithContext(requestCtx, "GET", reqURL, nil)
			if err != nil {
				results[idx].err = err
				cancel()
				return
			}
			req.Header.Set("Accept", "application/octet-stream")

			resp, err := client.Do(req)
			if err != nil {
				results[idx].err = fmt.Errorf("HTTP block fetch slot %d: %w", slot, err)
				cancel()
				return
			}
			body, readErr := readBeaconAPIResponseBody(resp)
			if readErr != nil {
				results[idx].err = fmt.Errorf("HTTP block read slot %d: %w", slot, readErr)
				cancel()
				return
			}
			if resp.StatusCode == http.StatusNotFound {
				return // Skipped slot — block stays nil
			}
			if resp.StatusCode != http.StatusOK {
				results[idx].err = fmt.Errorf("HTTP block fetch slot %d: status %d", slot, resp.StatusCode)
				cancel()
				return
			}

			version, err := httpConsensusVersion(resp.Header.Get("Eth-Consensus-Version"))
			if err != nil {
				results[idx].err = fmt.Errorf("HTTP block version slot %d: %w", slot, err)
				cancel()
				return
			}
			if err := validateHTTPBlockVersion(beaconCfg, slot, version); err != nil {
				results[idx].err = err
				cancel()
				return
			}
			block := cltypes.NewSignedBeaconBlock(beaconCfg, version)
			if err := block.DecodeSSZStrict(body, int(version)); err != nil {
				results[idx].err = fmt.Errorf("HTTP block decode slot %d: %w", slot, err)
				cancel()
				return
			}
			if block.Block == nil {
				results[idx].err = fmt.Errorf("HTTP block slot %d has no message", slot)
				cancel()
				return
			}
			if block.Block.Slot != slot {
				results[idx].err = fmt.Errorf("HTTP block slot mismatch: requested %d, received %d", slot, block.Block.Slot)
				cancel()
				return
			}
			results[idx].block = block
		})
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

func readBeaconAPIResponseBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(clparams.MaxChunkSize)+1))
	if err != nil {
		return nil, err
	}
	if uint64(len(body)) > clparams.MaxChunkSize {
		return nil, fmt.Errorf("response body exceeds %d bytes", clparams.MaxChunkSize)
	}
	return body, nil
}

func httpConsensusVersion(header string) (clparams.StateVersion, error) {
	header = strings.ToLower(strings.TrimSpace(header))
	if header == "" {
		return 0, errors.New("missing Eth-Consensus-Version header")
	}
	return clparams.StringToClVersion(header)
}

func validateHTTPBlockVersion(beaconCfg *clparams.BeaconChainConfig, slot uint64, version clparams.StateVersion) error {
	if beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 {
		return errors.New("invalid beacon chain config")
	}
	expected := beaconCfg.GetCurrentStateVersion(slot / beaconCfg.SlotsPerEpoch)
	if version != expected {
		return fmt.Errorf("HTTP block version mismatch at slot %d: expected %s, received %s", slot, expected, version)
	}
	return nil
}

type fetchEnvelopeHTTPResult struct {
	fetched int
}

// fetchEnvelopesFromBeaconAPI fetches execution payload envelopes from the beacon API for FULL blocks missing from P2P.
func fetchEnvelopesFromBeaconAPI(
	ctx context.Context,
	baseURL string,
	blocks []*cltypes.SignedBeaconBlock,
	fullRoots [][32]byte,
	received map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope,
	beaconCfg *clparams.BeaconChainConfig,
) fetchEnvelopeHTTPResult {
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
		return fetchEnvelopeHTTPResult{}
	}

	results := make([]envResult, len(toFetch))
	client := &http.Client{Timeout: 10 * time.Second}
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup

	for i, item := range toFetch {
		idx := i
		slot := item.slot
		root := item.root
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			reqURL := fmt.Sprintf("%s/eth/v1/beacon/execution_payload_envelopes/0x%x", baseURL, root)
			req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
			if err != nil {
				return
			}
			req.Header.Set("Accept", "application/octet-stream")

			resp, err := client.Do(req)
			if err != nil {
				return
			}
			body, err := readBeaconAPIResponseBody(resp)
			if err != nil {
				return
			}
			if resp.StatusCode == http.StatusNotFound {
				return
			}
			if resp.StatusCode != http.StatusOK {
				return
			}
			version, err := httpConsensusVersion(resp.Header.Get("Eth-Consensus-Version"))
			if err != nil || version != clparams.GloasVersion || validateHTTPBlockVersion(beaconCfg, slot, version) != nil {
				return
			}

			envelope := &cltypes.SignedExecutionPayloadEnvelope{
				Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg),
			}
			if err := envelope.DecodeSSZStrict(body, int(clparams.GloasVersion)); err != nil {
				log.Debug("[ForwardBeaconDownloader] HTTP envelope decode failed", "root", common.Hash(root), "err", err)
				return
			}
			if envelope.Message == nil || envelope.Message.BeaconBlockRoot != common.Hash(root) {
				return
			}
			results[idx] = envResult{hash: common.Hash(root), envelope: envelope}
		})
	}
	wg.Wait()

	result := fetchEnvelopeHTTPResult{}
	for _, r := range results {
		if r.envelope != nil {
			received[r.hash] = r.envelope
			result.fetched++
		}
	}
	return result
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
