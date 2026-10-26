// Copyright 2026 The Erigon Authors
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
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

func denebChain(t *testing.T, slots []uint64) []*cltypes.SignedBeaconBlock {
	t.Helper()
	blocks := make([]*cltypes.SignedBeaconBlock, len(slots))
	for i, slot := range slots {
		blocks[i] = makeDenebBlock(slot)
	}
	linkBeaconBlocks(t, blocks...)
	return blocks
}

func slotsUpTo(n uint64, skip func(uint64) bool) []uint64 {
	var slots []uint64
	for slot := range n {
		if !skip(slot) {
			slots = append(slots, slot)
		}
	}
	return slots
}

type slowRangeServer struct {
	blocks      []*cltypes.SignedBeaconBlock
	forked      []*cltypes.SignedBeaconBlock
	forkBelow   uint64
	delay       time.Duration
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	mu          sync.Mutex
	calls       map[uint64]int
}

func (s *slowRangeServer) requestBlocksByRange(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for m := s.maxInFlight.Load(); n > m && !s.maxInFlight.CompareAndSwap(m, n); m = s.maxInFlight.Load() {
	}
	select {
	case <-time.After(s.delay):
	case <-ctx.Done():
		return nil, "", ctx.Err()
	}
	s.mu.Lock()
	s.calls[start]++
	firstCall := s.calls[start] == 1
	s.mu.Unlock()
	served := s.blocks
	if firstCall && start < s.forkBelow {
		served = s.forked
	}
	var out []*cltypes.SignedBeaconBlock
	for _, blk := range served {
		if blk.Block.Slot >= start && blk.Block.Slot < start+count {
			out = append(out, blk)
		}
	}
	return out, "peer", nil
}

func walkBackwardFromTop(t *testing.T, blocks []*cltypes.SignedBeaconBlock, server *slowRangeServer, httpFallbackURL string) []uint64 {
	t.Helper()
	top := blocks[len(blocks)-1]
	topRoot, err := top.Block.HashSSZ()
	require.NoError(t, err)
	downloader := &BackwardBeaconDownloader{
		expectedRoot:         topRoot,
		beaconCfg:            &clparams.MainnetBeaconConfig,
		reqInterval:          time.NewTicker(time.Millisecond),
		baseInterval:         time.Millisecond,
		requestBlocksByRange: server.requestBlocksByRange,
		httpFallbackURL:      httpFallbackURL,
	}
	defer downloader.reqInterval.Stop()
	downloader.slotToDownload.Store(top.Block.Slot)
	var processed []uint64
	downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		processed = append(processed, blk.Block.Slot)
		return false, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for !downloader.Finished() {
		require.NoError(t, downloader.RequestMore(ctx))
	}
	return processed
}

func descending(blocks []*cltypes.SignedBeaconBlock) []uint64 {
	slots := make([]uint64, 0, len(blocks))
	for _, blk := range slices.Backward(blocks) {
		slots = append(slots, blk.Block.Slot)
	}
	return slots
}

func TestBackwardBeaconDownloaderKeepsSeveralRangesInFlight(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(640, func(uint64) bool { return false }))
	server := &slowRangeServer{blocks: blocks, delay: 20 * time.Millisecond, calls: map[uint64]int{}}

	require.Equal(t, descending(blocks), walkBackwardFromTop(t, blocks, server, ""))
	require.Greater(t, server.maxInFlight.Load(), int32(1))
}

func TestBackwardBeaconDownloaderRefetchesPrefetchedRangeFromAnotherFork(t *testing.T) {
	slots := slotsUpTo(640, func(uint64) bool { return false })
	blocks := denebChain(t, slots)
	forked := denebChain(t, slots)
	forked[0].Block.ParentRoot = hash(0xff)
	linkBeaconBlocks(t, forked...)
	server := &slowRangeServer{blocks: blocks, forked: forked, forkBelow: 576, delay: 20 * time.Millisecond, calls: map[uint64]int{}}
	var httpRequests atomic.Int32
	beaconAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		httpRequests.Add(1)
		http.NotFound(w, r)
	}))
	defer beaconAPI.Close()

	require.Equal(t, descending(blocks), walkBackwardFromTop(t, blocks, server, beaconAPI.URL))
	require.Zero(t, httpRequests.Load())
}

func TestBackwardBeaconDownloaderPrefetchesAcrossEmptySlots(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(700, func(slot uint64) bool {
		return slot%13 == 5 || slot == 575 || slot == 576 || (slot >= 300 && slot < 350)
	}))
	server := &slowRangeServer{blocks: blocks, delay: 5 * time.Millisecond, calls: map[uint64]int{}}

	require.Equal(t, descending(blocks), walkBackwardFromTop(t, blocks, server, ""))
	require.Greater(t, server.maxInFlight.Load(), int32(1))
}
