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

package p2p

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// ErrBadBALResponse is returned when a peer sends a BAL whose keccak256 does not
// match the hash the block header commits to, or otherwise violates EIP-8159.
// The peer has been penalised before this error is returned.
var ErrBadBALResponse = errors.New("bal: peer returned invalid block access list")

// BALRequest carries the header fields needed to validate an EIP-8159 response.
type BALRequest struct {
	Hash         common.Hash
	Number       uint64
	GasLimit     uint64
	ExpectedHash common.Hash
}

// BALFetcher fetches EIP-7928 block access lists over eth/71 (EIP-8159). Results
// are best-effort: a block whose BAL no peer holds is simply absent from the map.
type BALFetcher interface {
	// Fetch queries peerId and fallbackPeers (up to balFetchParallelism concurrently,
	// disjoint shards per round), taking the first BAL per block that validates
	// against its header commitment; misses are absent. A hash mismatch or protocol
	// violation penalises that peer. batchTimeout bounds the whole call across all
	// rounds; requestTimeout bounds each single request.
	Fetch(ctx context.Context, reqs []BALRequest, peerId *PeerId, fallbackPeers []PeerId, batchTimeout time.Duration, requestTimeout time.Duration) map[common.Hash]*types.BlockAccessListSidecar
}

// balFetchParallelism bounds how many peers Fetch queries concurrently for a batch's BALs.
const balFetchParallelism = 8

func NewBALFetcher(logger log.Logger, ml *MessageListener, ms *MessageSender, penalizer *PeerPenalizer, peerTracker *PeerTracker) BALFetcher {
	return &balFetcher{
		logger:          logger,
		messageListener: ml,
		messageSender:   ms,
		peerPenalizer:   penalizer,
		peerTracker:     peerTracker,
	}
}

type balFetcher struct {
	logger          log.Logger
	messageListener *MessageListener
	messageSender   *MessageSender
	peerPenalizer   *PeerPenalizer
	peerTracker     *PeerTracker
}

func (f *balFetcher) Fetch(ctx context.Context, reqs []BALRequest, peerId *PeerId, fallbackPeers []PeerId, batchTimeout time.Duration, requestTimeout time.Duration) map[common.Hash]*types.BlockAccessListSidecar {
	if len(reqs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, batchTimeout)
	defer cancel()
	fetch := func(ctx context.Context, rs []BALRequest, p *PeerId) (map[common.Hash]*types.BlockAccessListSidecar, int) {
		got, retryFrom, err := f.fetchFromPeer(ctx, rs, p, requestTimeout)
		if err != nil {
			f.logger.Debug("[p2p.bal] peer did not serve BALs", "peerId", p, "err", err)
		}
		return got, retryFrom
	}
	allPeers := append([]PeerId{*peerId}, fallbackPeers...)
	var maxNum uint64
	for _, r := range reqs {
		maxNum = max(maxNum, r.Number)
	}
	plausible := make([]PeerId, 0, len(allPeers))
	for _, p := range allPeers {
		if f.peerTracker.PeerMayHaveBALNum(&p, maxNum) {
			plausible = append(plausible, p)
		}
	}
	if len(plausible) == 0 {
		plausible = allPeers
	}
	return fetchAcrossPeers(ctx, reqs, plausible, balFetchParallelism, fetch)
}

// peerFetchFunc fetches BALs from a single peer, injected so fetchAcrossPeers is
// unit-testable without the network. retryFrom indexes the unanswered suffix;
// terminal failures use len(reqs) to stop retries for that peer.
type peerFetchFunc func(ctx context.Context, reqs []BALRequest, peerId *PeerId) (bals map[common.Hash]*types.BlockAccessListSidecar, retryFrom int)

// Pace requests across concurrent batches so truncated replies cannot cause
// a tight retry loop.
const balFetchRequestInterval = 500 * time.Millisecond

// balFetchShardingThreshold is the request-set size above which the first
// round shards; at or below it the dedup savings are negligible and coverage
// matters more, so every peer is asked for everything from the start.
const balFetchShardingThreshold = 16

// fetchAcrossPeers first splits large batches into disjoint per-peer shards.
// Later rounds ask each peer for its remaining requests, excluding BALs already
// returned by any peer. Answered entries leave that peer's queue even when
// unavailable; unanswered suffixes remain eligible for retry within the batch deadline.
func fetchAcrossPeers(ctx context.Context, reqs []BALRequest, peerIds []PeerId, maxParallel int, fetch peerFetchFunc) map[common.Hash]*types.BlockAccessListSidecar {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	out := make(map[common.Hash]*types.BlockAccessListSidecar, len(reqs))
	if len(peerIds) == 0 {
		return out
	}
	remaining := make([][]BALRequest, len(peerIds))
	for i := range remaining {
		remaining[i] = slices.Clone(reqs)
	}
	for round := 0; ctx.Err() == nil; round++ {
		shardedFetch := round == 0 && len(reqs) > balFetchShardingThreshold
		shards := min(len(peerIds), maxParallel, len(reqs))
		workers := len(peerIds)
		if shardedFetch {
			workers = shards
		}
		var eg errgroup.Group
		eg.SetLimit(maxParallel)
		for i := 0; i < workers && ctx.Err() == nil; i++ {
			peerIndex := (i + round) % len(peerIds)
			pending := remaining[peerIndex]
			start, end := 0, len(pending)
			if shardedFetch {
				start, end = i*len(reqs)/shards, (i+1)*len(reqs)/shards
			}
			slice := pending[start:end]
			if len(slice) == 0 {
				continue
			}
			peerId := peerIds[peerIndex]
			eg.Go(func() error {
				if ctx.Err() != nil {
					return nil
				}
				got, retryFrom := fetch(ctx, slice, &peerId)
				remaining[peerIndex] = slices.Delete(pending, start, start+retryFrom)
				mu.Lock()
				defer mu.Unlock()
				for hash, bal := range got {
					if _, ok := out[hash]; !ok {
						out[hash] = bal
					}
				}
				if len(out) == len(reqs) {
					cancel()
				}
				return nil
			})
		}
		_ = eg.Wait()
		var pending bool
		for i := range remaining {
			remaining[i] = slices.DeleteFunc(remaining[i], func(r BALRequest) bool {
				_, ok := out[r.Hash]
				return ok
			})
			pending = pending || len(remaining[i]) > 0
		}
		if !pending {
			break
		}
	}
	return out
}

func (f *balFetcher) fetchFromPeer(ctx context.Context, reqs []BALRequest, peerId *PeerId, timeout time.Duration) (map[common.Hash]*types.BlockAccessListSidecar, int, error) {
	if len(reqs) == 0 {
		return nil, 0, nil
	}
	response, err := f.fetchOnce(ctx, reqs, peerId, timeout)
	if err != nil {
		return nil, len(reqs), err
	}
	out, badPeer, err := validateBALResponse(reqs, response)
	retryFrom := len(response)
	if badPeer {
		f.logger.Debug("[p2p.bal] penalizing peer for bad BAL response", "peerId", peerId, "err", err)
		f.penalize(ctx, peerId)
		retryFrom = len(reqs)
	}
	// An answered-but-undelivered entry (explicit 0x80 or a skipped violation)
	// means the peer does not have that BAL; entries beyond the response length
	// are only truncation and say nothing.
	for i := 0; i < len(response) && i < len(reqs); i++ {
		if _, ok := out[reqs[i].Hash]; !ok {
			f.peerTracker.BALNumMissing(peerId, reqs[i].Number)
		}
	}
	return out, retryFrom, err
}

// validateBALResponse maps a positional EIP-8159 BlockAccessLists response onto
// a hash-keyed result. badPeer is true when the peer must be penalised: an
// over-long response, a 0xc0 "empty" claim for a block whose header commits to a
// non-empty BAL, or a payload whose keccak256 does not match the committed hash.
// A violating entry is skipped while the remaining valid entries are kept —
// pruned peers answering 0xc0 must not cost the rest of the response.
func validateBALResponse(reqs []BALRequest, response []rlp.RawValue) (map[common.Hash]*types.BlockAccessListSidecar, bool, error) {
	if len(response) > len(reqs) {
		return nil, true, fmt.Errorf("%w: peer returned %d entries for %d requests", ErrBadBALResponse, len(response), len(reqs))
	}
	var badPeer bool
	var responseErr error
	out := make(map[common.Hash]*types.BlockAccessListSidecar, len(reqs))
	for i := range response {
		entry := response[i]
		expected := reqs[i].ExpectedHash
		// EIP-8159: 0x80 = "not available", 0xc0 = "genuinely empty BAL",
		// anything else = BAL bytes that must keccak256 to the committed hash.
		if len(entry) == 0 || (len(entry) == 1 && entry[0] == 0x80) {
			continue
		}
		if len(entry) == 1 && entry[0] == 0xc0 {
			if expected != empty.BlockAccessListHash {
				badPeer = true
				responseErr = fmt.Errorf("%w: req %d wanted non-empty BAL %x, peer returned empty", ErrBadBALResponse, i, expected)
				continue
			}
		}
		bal, err := types.DecodeBlockAccessListSidecarOwned(entry)
		if err != nil {
			badPeer = true
			responseErr = fmt.Errorf("%w: req %d returned malformed BAL: %w", ErrBadBALResponse, i, err)
			continue
		}
		hash, err := bal.Hash()
		if err != nil {
			badPeer = true
			responseErr = fmt.Errorf("%w: req %d could not hash BAL: %w", ErrBadBALResponse, i, err)
			continue
		}
		if hash != expected {
			badPeer = true
			responseErr = fmt.Errorf("%w: req %d wanted %x got %x", ErrBadBALResponse, i, expected, hash)
			continue
		}
		if err = bal.ValidateForBlock(reqs[i].GasLimit); err != nil {
			badPeer = true
			responseErr = fmt.Errorf("%w: req %d returned invalid BAL: %w", ErrBadBALResponse, i, err)
			continue
		}
		out[reqs[i].Hash] = bal
	}
	return out, badPeer, responseErr
}

func (f *balFetcher) fetchOnce(ctx context.Context, reqs []BALRequest, peerId *PeerId, timeout time.Duration) ([]rlp.RawValue, error) {
	if err := f.peerTracker.waitForBALRequest(ctx, peerId); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	messages := make(chan *DecodedInboundMessage[*eth.BlockAccessListsPacket66])
	observer := func(message *DecodedInboundMessage[*eth.BlockAccessListsPacket66]) {
		select {
		case <-ctx.Done():
		case messages <- message:
		}
	}
	unregister := f.messageListener.RegisterBlockAccessListsObserver(observer)
	defer unregister()
	requestId := rand.Uint64() //nolint:gosec // request id does not need crypto-grade randomness
	hashes := make([]common.Hash, len(reqs))
	for i, r := range reqs {
		hashes[i] = r.Hash
	}
	err := f.messageSender.SendGetBlockAccessLists(ctx, peerId, eth.GetBlockAccessListsPacket66{
		RequestId:                 requestId,
		GetBlockAccessListsPacket: hashes,
	})
	if err != nil {
		return nil, err
	}
	message, _, err := awaitResponse(ctx, timeout, messages, filterBlockAccessLists(peerId, requestId))
	if err != nil {
		return nil, err
	}
	return message.BlockAccessListsPacket, nil
}

func (f *balFetcher) penalize(ctx context.Context, peerId *PeerId) {
	err := f.peerPenalizer.Penalize(ctx, peerId)
	if err != nil {
		f.logger.Debug("[p2p.bal] failed to penalize peer", "peerId", peerId, "err", err)
	}
}

func filterBlockAccessLists(peerId *PeerId, requestId uint64) func(*DecodedInboundMessage[*eth.BlockAccessListsPacket66]) bool {
	return func(message *DecodedInboundMessage[*eth.BlockAccessListsPacket66]) bool {
		return filter(peerId, message.PeerId, requestId, message.Decoded.RequestId)
	}
}
