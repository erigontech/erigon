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
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/rpc"
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
	f.highestSlotProcessed = highestSlotProcessed
	f.highestSlotUpdateTime = time.Now()
}

type peerAndBlocks struct {
	peerId string
	blocks []*cltypes.SignedBeaconBlock
}

func (f *ForwardBeaconDownloader) RequestMore(ctx context.Context) {
	log.Info("Requesting more beacon blocks")
	count := uint64(16)
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
				reqCount := count
				if time.Since(f.highestSlotUpdateTime) > 30*time.Second {
					reqCount *= 2
				} else if time.Since(f.highestSlotUpdateTime) > time.Minute {
					reqCount *= 4
				}
				if time.Since(f.highestSlotUpdateTime) > 90*time.Second {
					log.Warn("Forward beacon downloader gets stuck for %v seconds", time.Since(f.highestSlotUpdateTime).Seconds(), "highestSlotProcessed", f.highestSlotProcessed)
				}
				// this is so we do not get stuck on a side-fork
				log.Info("Requesting beacon blocks by range", "slot", reqSlot, "count", count)
				responses, peerId, err := f.rpc.SendBeaconBlocksByRangeReq(ctx, reqSlot, count)
				if err != nil {
					log.Warn("Failed to send beacon blocks by range request", "err", err, "peer", peerId, "slot", reqSlot, "count", count)
					return
				}
				if responses == nil {
					log.Warn("response is nil", "peer", peerId, "slot", reqSlot, "count", count)
					return
				}
				if len(responses) == 0 {
					log.Warn("response is empty", "peer", peerId, "slot", reqSlot, "count", count)
					f.rpc.BanPeer(peerId)
					return
				}
				if len(atomicResp.Load().(peerAndBlocks).blocks) > 0 {
					log.Info("Already received blocks", "peer", peerId, "slot", reqSlot, "count", count)
					return
				}
				atomicResp.Store(peerAndBlocks{peerId, responses})
				log.Info("Received beacon blocks by range", "peer", peerId, "slot", reqSlot, "count", count)
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

	log.Info("before acquiring lock")
	f.mu.Lock()
	defer f.mu.Unlock()
	log.Info("after acquiring lock")

	var highestSlotProcessed uint64
	var err error
	blocks := atomicResp.Load().(peerAndBlocks).blocks
	pid := atomicResp.Load().(peerAndBlocks).peerId
	if highestSlotProcessed, err = f.process(f.highestSlotProcessed, blocks); err != nil {
		log.Info("Failed to process downloaded blocks", "err", err)
		f.rpc.BanPeer(pid)
		log.Info("Banned peer", "peer", pid)
		return
	}
	f.highestSlotProcessed = highestSlotProcessed
	f.highestSlotUpdateTime = time.Now()
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
