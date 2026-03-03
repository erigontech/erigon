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
	reqInterval := time.NewTicker(300 * time.Millisecond)
	defer reqInterval.Stop()

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
				// double the request count every 10 seconds. This is inspired by the mekong network, which has many consecutive missing blocks.
				reqCount := count
				// NEED TO COMMENT THIS BC IT CAUSES ISSUES ON MAINNET

				// if !f.highestSlotUpdateTime.IsZero() {
				// 	multiplier := int(time.Since(f.highestSlotUpdateTime).Seconds()) / 10
				// 	multiplier = min(multiplier, 6)
				// 	reqCount *= uint64(1 << uint(multiplier))
				// }

				// leave a warning if we are stuck for more than 90 seconds
				if time.Since(f.highestSlotUpdateTime) > 90*time.Second {
					log.Trace("Forward beacon downloader gets stuck", "time", time.Since(f.highestSlotUpdateTime).Seconds(), "highestSlotProcessed", f.highestSlotProcessed)
				}
				// Request count+1 blocks: the extra block is used as a lookahead to determine
				// whether the last block in the batch is GLOAS FULL or EMPTY.
				responses, peerId, err := f.rpc.SendBeaconBlocksByRangeReq(ctx, reqSlot, reqCount+1)
				if err != nil {
					if errors.Is(err, peers.ErrNoPeers) {
						log.Trace("No peers available for beacon blocks by range request", "err", err, "peer", peerId, "slot", reqSlot, "reqCount", reqCount)
					} else {
						log.Debug("Failed to send beacon blocks by range request", "err", err, "peer", peerId, "slot", reqSlot, "reqCount", reqCount)
					}
					return
				}
				if responses == nil {
					return
				}
				if len(responses) == 0 {
					f.rpc.BanPeer(peerId)
					return
				}
				if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
					return
				}
				atomicResp.Store(peerAndBlocks{peerId, responses})
			}()
		case <-ctx.Done():
			return
		default:
			if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
				break Loop
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	resp := atomicResp.Load().(peerAndBlocks)
	blocks := resp.blocks
	pid := resp.peerId

	// Sort by slot so count+1 lookahead is correct.
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	// Trim to count; the extra block is only used as a lookahead for FULL/EMPTY detection.
	processBlocks := blocks
	var extraBlock *cltypes.SignedBeaconBlock
	if uint64(len(blocks)) > count {
		processBlocks = blocks[:count]
		extraBlock = blocks[count]
	}

	// For GLOAS blocks, determine which are FULL and request their envelopes before locking.
	var envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	if anyGloasBlock(processBlocks) {
		if fullRoots := determineFullGloasRoots(processBlocks, extraBlock); len(fullRoots) > 0 {
			var envErr error
			envelopes, envErr = RequestEnvelopesFrantically(ctx, f.rpc, fullRoots)
			if envErr != nil {
				log.Debug("[ForwardBeaconDownloader] failed to get envelopes", "err", envErr)
				// Non-fatal: blocks without envelopes will be treated as EMPTY.
			}
		}
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

// determineFullGloasRoots uses the count+1 trick to identify which GLOAS blocks are FULL.
// A block is FULL if the next block's bid.ParentBlockHash == this block's bid.BlockHash,
// meaning the EL chain continued from this block's payload.
// extraBlock is the (count+1)-th block used as a lookahead for the last block in the batch.
func determineFullGloasRoots(blocks []*cltypes.SignedBeaconBlock, extraBlock *cltypes.SignedBeaconBlock) [][32]byte {
	var fullRoots [][32]byte
	for i, block := range blocks {
		if block.Version() < clparams.GloasVersion {
			continue
		}
		bid := block.Block.Body.GetSignedExecutionPayloadBid()
		if bid == nil || bid.Message == nil {
			continue
		}
		// Get lookahead block
		var nextBlock *cltypes.SignedBeaconBlock
		if i+1 < len(blocks) {
			nextBlock = blocks[i+1]
		} else {
			nextBlock = extraBlock
		}
		if nextBlock == nil {
			// No lookahead: optimistically request the envelope; timeout means EMPTY.
			root, err := block.Block.HashSSZ()
			if err == nil {
				fullRoots = append(fullRoots, root)
			}
			continue
		}
		nextBid := nextBlock.Block.Body.GetSignedExecutionPayloadBid()
		if nextBid != nil && nextBid.Message != nil && nextBid.Message.ParentBlockHash == bid.Message.BlockHash {
			root, err := block.Block.HashSSZ()
			if err == nil {
				fullRoots = append(fullRoots, root)
			}
		}
	}
	return fullRoots
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
