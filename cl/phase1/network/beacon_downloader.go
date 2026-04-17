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
	"sort"
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
	rpc                   *rpc.BeaconRpcP2P
	process               ProcessFn

	mu sync.Mutex
}

func NewForwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P) *ForwardBeaconDownloader {
	return &ForwardBeaconDownloader{
		ctx: ctx,
		rpc: rpc,
	}
}

// SetProcessFunction sets the function used to process segments.
func (f *ForwardBeaconDownloader) SetProcessFunction(fn ProcessFn) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.process = fn
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
				// Request one extra block beyond the batch for GLOAS lookahead:
				// the extra block lets determineFullGloasRoots check whether the
				// last batch block is FULL or EMPTY, instead of guessing FULL.
				reqCount := count + 1

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
		fullRoots := determineFullGloasRoots(processBlocks, int(count))
		// Trim the lookahead block before envelope fetch and processing.
		if uint64(len(processBlocks)) > count {
			processBlocks = processBlocks[:count]
		}
		if len(fullRoots) > 0 {
			var envErr error
			envelopes, envErr = RequestEnvelopesFrantically(ctx, f.rpc, fullRoots, processBlocks...)
			if envErr != nil {
				log.Debug("[ForwardBeaconDownloader] failed to get envelopes", "err", envErr)
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
		f.rpc.BanPeer(pid)
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

// GetHighestProcessedSlot retrieve the highest processed slot we accumulated.
func (f *ForwardBeaconDownloader) GetHighestProcessedSlot() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.highestSlotProcessed
}

func (f *ForwardBeaconDownloader) Peers() (uint64, error) {
	return f.rpc.Peers()
}
