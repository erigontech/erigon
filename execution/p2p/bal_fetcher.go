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
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// ErrBadBALResponse is returned when a peer sends a BAL whose keccak256 does not
// match the hash the block header commits to, or otherwise violates EIP-8159.
// The peer has been penalised before this error is returned.
var ErrBadBALResponse = errors.New("bal: peer returned invalid block access list")

// BALRequest pairs a block hash with the BAL hash its header commits to, so the
// fetcher can validate the EIP-8159 response without a separate header lookup.
type BALRequest struct {
	Hash         common.Hash
	Number       uint64
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
	Fetch(ctx context.Context, reqs []BALRequest, peerId *PeerId, fallbackPeers []PeerId, batchTimeout time.Duration, requestTimeout time.Duration) map[common.Hash][]byte
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

func (f *balFetcher) Fetch(ctx context.Context, reqs []BALRequest, peerId *PeerId, fallbackPeers []PeerId, batchTimeout time.Duration, requestTimeout time.Duration) map[common.Hash][]byte {
	if len(reqs) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, batchTimeout)
	defer cancel()
	fetch := func(ctx context.Context, rs []BALRequest, p *PeerId) map[common.Hash][]byte {
		got, err := f.fetchFromPeer(ctx, rs, p, requestTimeout)
		if err != nil {
			f.logger.Debug("[p2p.bal] peer did not serve BALs", "peerId", p, "err", err)
		}
		return got
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
// unit-testable without the network.
type peerFetchFunc func(ctx context.Context, reqs []BALRequest, peerId *PeerId) map[common.Hash][]byte

// balFetchShardingThreshold is the request-set size above which the first
// round shards; at or below it the dedup savings are negligible and coverage
// matters more, so every peer is asked for everything from the start.
const balFetchShardingThreshold = 16

// fetchAcrossPeers fetches reqs in rounds using two request shapes. The first
// round partitions a large request set into disjoint per-peer shards: peers
// truncate responses to the eth softResponseLimit, so asking every peer the
// same thing serializes on duplicate prefixes, while disjoint shards make
// throughput additive across peers. Whatever survives that round is straggler
// territory — blocks held by few peers — so every later round asks all peers
// for the full remainder: union coverage, a block is found if any single peer
// has it. The loop stops once covered or when a full-remainder round makes no
// progress, which proves no connected peer can serve the rest.
func fetchAcrossPeers(ctx context.Context, reqs []BALRequest, peerIds []PeerId, maxParallel int, fetch peerFetchFunc) map[common.Hash][]byte {
	out := make(map[common.Hash][]byte, len(reqs))
	if len(peerIds) == 0 {
		return out
	}
	remaining := reqs
	for round := 0; len(remaining) > 0 && ctx.Err() == nil; round++ {
		shardedFetch := round == 0 && len(remaining) > balFetchShardingThreshold
		shards := min(len(peerIds), maxParallel, len(remaining))
		workers := len(peerIds)
		if shardedFetch {
			workers = shards
		}
		results := make([]map[common.Hash][]byte, workers)
		var eg errgroup.Group
		eg.SetLimit(maxParallel)
		for i := 0; i < workers; i++ {
			slice := remaining
			if shardedFetch {
				slice = remaining[i*len(remaining)/shards : (i+1)*len(remaining)/shards]
			}
			peerId := peerIds[(i+round)%len(peerIds)]
			eg.Go(func() error {
				results[i] = fetch(ctx, slice, &peerId)
				return nil
			})
		}
		_ = eg.Wait()
		for _, got := range results {
			for hash, bal := range got {
				if _, ok := out[hash]; !ok {
					out[hash] = bal
				}
			}
		}
		next := make([]BALRequest, 0, len(remaining))
		for _, r := range remaining {
			if _, ok := out[r.Hash]; !ok {
				next = append(next, r)
			}
		}
		if len(next) == len(remaining) && !shardedFetch {
			break
		}
		remaining = next
	}
	return out
}

func (f *balFetcher) fetchFromPeer(ctx context.Context, reqs []BALRequest, peerId *PeerId, timeout time.Duration) (map[common.Hash][]byte, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	response, err := f.fetchOnce(ctx, reqs, peerId, timeout)
	if err != nil {
		return nil, err
	}
	out, badPeer, err := validateBALResponse(reqs, response)
	if badPeer {
		f.logger.Debug("[p2p.bal] penalizing peer for bad BAL response", "peerId", peerId, "err", err)
		f.penalize(ctx, peerId)
	}
	// An answered-but-undelivered entry (explicit 0x80 or a skipped violation)
	// means the peer does not have that BAL; entries beyond the response length
	// are only truncation and say nothing.
	for i := 0; i < len(response) && i < len(reqs); i++ {
		if _, ok := out[reqs[i].Hash]; !ok {
			f.peerTracker.BALNumMissing(peerId, reqs[i].Number)
		}
	}
	return out, err
}

// validateBALResponse maps a positional EIP-8159 BlockAccessLists response onto
// a hash-keyed result. badPeer is true when the peer must be penalised: an
// over-long response, a 0xc0 "empty" claim for a block whose header commits to a
// non-empty BAL, or a payload whose keccak256 does not match the committed hash.
// A violating entry is skipped while the remaining valid entries are kept —
// pruned peers answering 0xc0 must not cost the rest of the response.
func validateBALResponse(reqs []BALRequest, response []rlp.RawValue) (map[common.Hash][]byte, bool, error) {
	if len(response) > len(reqs) {
		return nil, true, fmt.Errorf("%w: peer returned %d entries for %d requests", ErrBadBALResponse, len(response), len(reqs))
	}
	var badPeer bool
	var err error
	out := make(map[common.Hash][]byte, len(reqs))
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
				err = fmt.Errorf("%w: req %d wanted non-empty BAL %x, peer returned empty", ErrBadBALResponse, i, expected)
				continue
			}
			out[reqs[i].Hash] = []byte{0xc0}
			continue
		}
		if crypto.Keccak256Hash(entry) != expected {
			badPeer = true
			err = fmt.Errorf("%w: req %d wanted %x got %x", ErrBadBALResponse, i, expected, crypto.Keccak256Hash(entry))
			continue
		}
		out[reqs[i].Hash] = entry
	}
	return out, badPeer, err
}

func (f *balFetcher) fetchOnce(ctx context.Context, reqs []BALRequest, peerId *PeerId, timeout time.Duration) ([]rlp.RawValue, error) {
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
