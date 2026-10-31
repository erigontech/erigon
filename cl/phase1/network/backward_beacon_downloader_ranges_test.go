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
	"errors"
	"fmt"
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
	held        func(start uint64) bool
	badPeer     string
	badStart    uint64
	sent        atomic.Int32
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	mu          sync.Mutex
	calls       map[uint64]int
	cancelled   []uint64
}

func (s *slowRangeServer) requestBlocksByRange(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
	s.sent.Add(1)
	n := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for m := s.maxInFlight.Load(); n > m && !s.maxInFlight.CompareAndSwap(m, n); m = s.maxInFlight.Load() {
	}
	delay := s.delay
	if s.held != nil && s.held(start) {
		delay = time.Hour
	}
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		s.mu.Lock()
		s.cancelled = append(s.cancelled, start)
		s.mu.Unlock()
		return nil, "", ctx.Err()
	}
	s.mu.Lock()
	s.calls[start]++
	firstCall := s.calls[start] == 1
	s.mu.Unlock()
	if firstCall && s.badPeer != "" && start == s.badStart {
		return nil, s.badPeer, errors.New("bad response")
	}
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

func (s *slowRangeServer) requestBlocksByRoot(_ context.Context, roots [][32]byte) ([]*cltypes.SignedBeaconBlock, string, error) {
	var out []*cltypes.SignedBeaconBlock
	for _, blk := range s.blocks {
		root, err := blk.Block.HashSSZ()
		if err != nil {
			return nil, "", err
		}
		if slices.Contains(roots, root) {
			out = append(out, blk)
		}
	}
	return out, "peer", nil
}

func newRangeDownloader(t *testing.T, top *cltypes.SignedBeaconBlock, server *slowRangeServer) *BackwardBeaconDownloader {
	t.Helper()
	topRoot, err := top.Block.HashSSZ()
	require.NoError(t, err)
	downloader := &BackwardBeaconDownloader{
		expectedRoot:         topRoot,
		beaconCfg:            &clparams.MainnetBeaconConfig,
		reqInterval:          time.NewTicker(time.Millisecond),
		baseInterval:         time.Millisecond,
		requestBlocksByRange: server.requestBlocksByRange,
		requestBlocksByRoot:  server.requestBlocksByRoot,
	}
	t.Cleanup(downloader.reqInterval.Stop)
	downloader.slotToDownload.Store(top.Block.Slot)
	return downloader
}

func walkBackwardFromTop(t *testing.T, blocks []*cltypes.SignedBeaconBlock, server *slowRangeServer, httpFallbackURL string) []uint64 {
	t.Helper()
	downloader := newRangeDownloader(t, blocks[len(blocks)-1], server)
	downloader.httpFallbackURL = httpFallbackURL
	return walkBackward(t, downloader)
}

func walkBackward(t *testing.T, downloader *BackwardBeaconDownloader) []uint64 {
	t.Helper()
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
	require.LessOrEqual(t, server.maxInFlight.Load(), int32(maxPrefetchedRanges+1))
}

func TestBackwardBeaconDownloaderBansPeerOnPrefetchError(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(640, func(uint64) bool { return false }))
	server := &slowRangeServer{blocks: blocks, delay: 20 * time.Millisecond, badPeer: "bad", badStart: 512, calls: map[uint64]int{}}
	sentinel := &emptyResponseBanRecordingSentinel{banned: make(chan string, 1)}
	downloader := newRangeDownloader(t, blocks[len(blocks)-1], server)
	downloader.rpc, _ = newContextBlockingBeaconRPC(t.Context(), sentinel)

	require.Equal(t, descending(blocks), walkBackward(t, downloader))
	select {
	case pid := <-sentinel.banned:
		require.Equal(t, "bad", pid)
	default:
		t.Fatal("peer that failed a prefetched range was not banned")
	}
}

func TestBackwardBeaconDownloaderPrefetchHitResetsEmptyBackoff(t *testing.T) {
	block := makeDenebBlock(10)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	downloader := &BackwardBeaconDownloader{
		expectedRoot: root,
		reqInterval:  time.NewTicker(time.Hour),
		baseInterval: time.Hour,
	}
	defer downloader.reqInterval.Stop()
	downloader.slotToDownload.Store(10)
	downloader.emptyResponses.Store(3)
	ready := make(chan []*cltypes.SignedBeaconBlock, 1)
	ready <- []*cltypes.SignedBeaconBlock{block}
	downloader.prefetched = []prefetchedRange{{lo: 0, hi: 63, blocks: ready, cancel: func() {}}}

	blocks, err := downloader.fetchBlockRange(t.Context())
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Zero(t, downloader.emptyResponses.Load())
}

func TestBackwardBeaconDownloaderTakesPrefetchBelowEmptyRangeBottom(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(640, func(slot uint64) bool { return slot == 512 }))
	server := &slowRangeServer{blocks: blocks, delay: 20 * time.Millisecond, calls: map[uint64]int{}}

	require.Equal(t, descending(blocks), walkBackwardFromTop(t, blocks, server, ""))
	want := map[uint64]int{}
	for start := uint64(0); start < 640; start += slotsPerRange {
		want[start] = 1
	}
	require.Equal(t, want, server.calls)
}

func TestBackwardBeaconDownloaderKeepsPrefetchesWhenFirstResponseIsPartial(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(640, func(uint64) bool { return false }))
	// The first response for start 576 holds only slots 576..599.
	server := &slowRangeServer{blocks: blocks, forked: blocks[:600], forkBelow: 577, delay: 20 * time.Millisecond, calls: map[uint64]int{}}

	require.Equal(t, descending(blocks), walkBackwardFromTop(t, blocks, server, ""))
	require.LessOrEqual(t, server.sent.Load(), int32(11))
}

func TestBackwardBeaconDownloaderKeepsPrefetchesWhenProcessingStopsAboveThem(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(640, func(uint64) bool { return false }))
	server := &slowRangeServer{blocks: blocks, delay: 20 * time.Millisecond, calls: map[uint64]int{}}
	downloader := newRangeDownloader(t, blocks[len(blocks)-1], server)
	failed := false
	var processed []uint64
	downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		if blk.Block.Slot == 600 && !failed {
			failed = true
			return false, errors.New("transient failure")
		}
		processed = append(processed, blk.Block.Slot)
		return false, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for !downloader.Finished() {
		require.NoError(t, downloader.RequestMore(ctx))
	}

	require.Equal(t, descending(blocks), processed)
	require.LessOrEqual(t, server.sent.Load(), int32(11))
}

func TestBackwardBeaconDownloaderCancelsDroppedPrefetches(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(640, func(uint64) bool { return false }))
	server := &slowRangeServer{blocks: blocks, delay: 20 * time.Millisecond, held: func(start uint64) bool { return start > 0 && start < 576 }, calls: map[uint64]int{}}
	downloader := newRangeDownloader(t, blocks[len(blocks)-1], server)
	downloader.SetOnNewBlock(func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		return false, nil
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	require.NoError(t, downloader.RequestMore(ctx))
	require.NotEmpty(t, downloader.prefetched)
	var dropped []uint64
	for _, r := range downloader.prefetched {
		dropped = append(dropped, r.lo)
	}
	slices.Sort(dropped)
	root, err := blocks[63].Block.HashSSZ()
	require.NoError(t, err)
	downloader.SetSlotToDownload(63)
	downloader.SetExpectedRoot(root)

	require.NoError(t, downloader.RequestMore(ctx))
	require.True(t, downloader.Finished())
	require.Eventually(t, func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return slices.Equal(dropped, slices.Sorted(slices.Values(server.cancelled)))
	}, 5*time.Second, time.Millisecond)
}

func TestBackwardBeaconDownloaderCancelsPrefetchesWhenFinished(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(1280, func(uint64) bool { return false }))
	server := &slowRangeServer{blocks: blocks, delay: 5 * time.Millisecond, held: func(start uint64) bool { return start < 640 }, calls: map[uint64]int{}}
	downloader := newRangeDownloader(t, blocks[len(blocks)-1], server)
	// The range holding the finishing block is answered only once the queue below it is full.
	queueFull := make(chan struct{})
	var queued atomic.Int32
	downloader.requestBlocksByRange = func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
		if start < 640 && queued.Add(1) == maxPrefetchedRanges {
			close(queueFull)
		}
		if start == 640 {
			select {
			case <-queueFull:
			case <-ctx.Done():
				return nil, "", ctx.Err()
			}
		}
		return server.requestBlocksByRange(ctx, start, count)
	}
	downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock, _ *cltypes.SignedExecutionPayloadEnvelope) (bool, error) {
		return blk.Block.Slot == 640, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for !downloader.Finished() {
		require.NoError(t, downloader.RequestMore(ctx))
	}

	require.Empty(t, downloader.prefetched)
	require.Eventually(t, func() bool {
		server.mu.Lock()
		defer server.mu.Unlock()
		return len(server.cancelled) == maxPrefetchedRanges
	}, 5*time.Second, time.Millisecond)
}

func singleBlockBeaconAPI(t *testing.T, slot uint64) (string, *clparams.BeaconChainConfig) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.InitializeForkSchedule()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	block.Block.Slot = slot
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	beaconAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != fmt.Sprintf("/eth/v2/beacon/blocks/%d", slot) {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Eth-Consensus-Version", "phase0")
		_, _ = w.Write(encoded)
	}))
	t.Cleanup(beaconAPI.Close)
	return beaconAPI.URL, &cfg
}

func TestBackwardBeaconDownloaderHTTPFastPathCancelsPrefetches(t *testing.T) {
	url, cfg := singleBlockBeaconAPI(t, 11)
	downloader := &BackwardBeaconDownloader{
		reqInterval:     time.NewTicker(time.Hour),
		httpFallbackURL: url,
		beaconCfg:       cfg,
	}
	defer downloader.reqInterval.Stop()
	downloader.slotToDownload.Store(63)
	downloader.httpPreferred.Store(true)
	prefetchCtx, cancelPrefetch := context.WithCancel(t.Context())
	defer cancelPrefetch()
	downloader.prefetched = []prefetchedRange{{lo: 0, hi: 63, blocks: make(chan []*cltypes.SignedBeaconBlock, 1), cancel: cancelPrefetch}}

	blocks, err := downloader.fetchBlockRange(t.Context())
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.Empty(t, downloader.prefetched)
	require.ErrorIs(t, prefetchCtx.Err(), context.Canceled)
}

func TestBackwardBeaconDownloaderHTTPFallbackCancelsPrefetches(t *testing.T) {
	url, cfg := singleBlockBeaconAPI(t, 100)
	downloader := &BackwardBeaconDownloader{
		reqInterval:     time.NewTicker(time.Hour),
		httpFallbackURL: url,
		beaconCfg:       cfg,
	}
	defer downloader.reqInterval.Stop()
	downloader.slotToDownload.Store(127)
	takenCtx, cancelTaken := context.WithCancel(t.Context())
	defer cancelTaken()
	queuedCtx, cancelQueued := context.WithCancel(t.Context())
	defer cancelQueued()
	downloader.prefetched = []prefetchedRange{
		{lo: 64, hi: 127, blocks: make(chan []*cltypes.SignedBeaconBlock, 1), cancel: cancelTaken},
		{lo: 0, hi: 63, blocks: make(chan []*cltypes.SignedBeaconBlock, 1), cancel: cancelQueued},
	}

	blocks, err := downloader.fetchBlockRange(t.Context())
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	require.ErrorIs(t, takenCtx.Err(), context.Canceled)
	require.ErrorIs(t, queuedCtx.Err(), context.Canceled)
	require.Empty(t, downloader.prefetched)
}

func TestBackwardBeaconDownloaderBansPeerSendingUnrequestedRoot(t *testing.T) {
	requested, err := makeDenebBlock(10).Block.HashSSZ()
	require.NoError(t, err)
	sentinel := &emptyResponseBanRecordingSentinel{banned: make(chan string, 1)}
	downloader := &BackwardBeaconDownloader{
		requestBlocksByRoot: func(context.Context, [][32]byte) ([]*cltypes.SignedBeaconBlock, string, error) {
			return []*cltypes.SignedBeaconBlock{makeDenebBlock(11)}, "liar", nil
		},
	}
	downloader.rpc, _ = newContextBlockingBeaconRPC(t.Context(), sentinel)

	require.Nil(t, downloader.blockByRoot(t.Context(), requested, 0, slotsPerRange))
	select {
	case pid := <-sentinel.banned:
		require.Equal(t, "liar", pid)
	default:
		t.Fatal("peer that answered a root request with another block was not banned")
	}
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

func TestBackwardBeaconDownloaderFetchesParentByRootBelowEmptyRange(t *testing.T) {
	blocks := denebChain(t, slotsUpTo(701, func(slot uint64) bool { return slot >= 500 && slot < 700 }))
	server := &slowRangeServer{blocks: blocks, delay: 5 * time.Millisecond, calls: map[uint64]int{}}

	require.Equal(t, descending(blocks), walkBackwardFromTop(t, blocks, server, ""))
}

func TestBlockWithRootReturnsFirstMatch(t *testing.T) {
	first := makeDenebBlock(7)
	second := makeDenebBlock(7)
	second.Signature[0] = 1
	root, err := first.Block.HashSSZ()
	require.NoError(t, err)

	require.Same(t, first, blockWithRoot([]*cltypes.SignedBeaconBlock{first, second}, root))
}
