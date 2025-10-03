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
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
)

// Input: the currently highest slot processed and the list of blocks we want to know process
// Output: the new last new highest slot processed and an error possibly?
type ProcessFn func(
	highestSlotProcessed uint64,
	blocks []*cltypes.SignedBeaconBlock) (
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
				// this is so we do not get stuck on a side-fork
				responses, peerId, err := f.rpc.SendBeaconBlocksByRangeReq(ctx, reqSlot, reqCount)
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

	f.mu.Lock()
	defer f.mu.Unlock()

	var highestSlotProcessed uint64
	var err error
	blocks := atomicResp.Load().(peerAndBlocks).blocks
	pid := atomicResp.Load().(peerAndBlocks).peerId
	if highestSlotProcessed, err = f.process(f.highestSlotProcessed, blocks); err != nil {
		f.rpc.BanPeer(pid)
		return
	}
	if highestSlotProcessed > f.highestSlotProcessed {
		f.highestSlotProcessed = highestSlotProcessed
		f.highestSlotUpdateTime = time.Now()
	}
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
