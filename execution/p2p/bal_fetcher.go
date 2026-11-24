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
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	commonerrors "github.com/erigontech/erigon/common/errors"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// ErrBadBALResponse marks a reply with more entries than requested or a BAL
// whose hash differs from its header commitment. In either case the fetcher
// asks the sentry to penalize the peer.
var ErrBadBALResponse = errors.New("bal: peer returned invalid block access list")

// BALRequest carries the header fields needed to validate an EIP-8159 response.
type BALRequest struct {
	Hash         common.Hash
	Number       uint64
	GasLimit     uint64
	ExpectedHash common.Hash
}

// BALFetcher fetches EIP-7928 block access lists over eth/71 (EIP-8159).
type BALFetcher interface {
	// Fetch queries peerId and fallbackPeers and keeps the first valid BAL for each
	// block. The result may be partial. batchTimeout sets a deadline for all rounds.
	// requestTimeout limits the wait for each reply after its request is sent.
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
	fetch := func(ctx context.Context, rs []BALRequest, p *PeerId) map[common.Hash]*types.BlockAccessListSidecar {
		got, err := f.fetchFromPeer(ctx, rs, p, requestTimeout)
		if err != nil && !commonerrors.IsOnlyCanceled(err) {
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
type peerFetchFunc func(ctx context.Context, reqs []BALRequest, peerId *PeerId) map[common.Hash]*types.BlockAccessListSidecar

// balFetchShardingThreshold is the request-set size above which the first
// round shards; at or below it the dedup savings are negligible and coverage
// matters more, so every peer is asked for everything from the start.
const balFetchShardingThreshold = 16

// fetchAcrossPeers splits a large batch across peers in the first round so
// size-limited replies cover different blocks. For small batches and all later
// rounds, each request covers the remaining blocks. If such a round yields no
// new BALs, the fetch returns what it has collected.
func fetchAcrossPeers(ctx context.Context, reqs []BALRequest, peerIds []PeerId, maxParallel int, fetch peerFetchFunc) map[common.Hash]*types.BlockAccessListSidecar {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	out := make(map[common.Hash]*types.BlockAccessListSidecar, len(reqs))
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
		var eg errgroup.Group
		eg.SetLimit(maxParallel)
		for i := 0; i < workers && ctx.Err() == nil; i++ {
			slice := remaining
			if shardedFetch {
				slice = remaining[i*len(remaining)/shards : (i+1)*len(remaining)/shards]
			}
			peerId := peerIds[(i+round)%len(peerIds)]
			eg.Go(func() error {
				if ctx.Err() != nil {
					return nil
				}
				got := fetch(ctx, slice, &peerId)
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

func (f *balFetcher) fetchFromPeer(ctx context.Context, reqs []BALRequest, peerId *PeerId, timeout time.Duration) (map[common.Hash]*types.BlockAccessListSidecar, error) {
	if len(reqs) == 0 {
		return nil, nil
	}
	payload, err := f.fetchOnce(ctx, reqs, peerId, timeout)
	if err != nil {
		return nil, err
	}
	entries, err := decodeBlockAccessListsResponse(payload, len(reqs))
	if err != nil {
		f.penalize(ctx, peerId)
		return nil, err
	}
	out, err := validateBALResponse(reqs, entries)
	if errors.Is(err, ErrBadBALResponse) {
		f.logger.Debug("[p2p.bal] penalizing peer for bad BAL response", "peerId", peerId, "err", err)
		f.penalize(ctx, peerId)
	}
	// Mark a BAL missing only when the peer explicitly reports it as unavailable.
	// A short reply leaves later blocks unanswered, and a discarded entry may
	// still match its header commitment.
	for i, entry := range entries {
		if isUnavailableBALEntry(entry) {
			f.peerTracker.BALNumMissing(peerId, reqs[i].Number)
		}
	}
	return out, err
}

func isUnavailableBALEntry(entry rlp.RawValue) bool {
	return len(entry) == 0 || (len(entry) == 1 && entry[0] == 0x80)
}

// validateBALResponse matches response[i] to reqs[i]. The decoder bounds the
// response to len(reqs). A commitment mismatch returns ErrBadBALResponse
// alongside any valid entries from the same reply.
func validateBALResponse(reqs []BALRequest, response []rlp.RawValue) (map[common.Hash]*types.BlockAccessListSidecar, error) {
	var responseErr error
	out := make(map[common.Hash]*types.BlockAccessListSidecar, len(reqs))
	for i := range response {
		entry := response[i]
		expected := reqs[i].ExpectedHash
		// EIP-8159 uses the RLP empty string (0x80) for an unavailable BAL. The
		// empty list (0xc0) is an actual BAL and must match the header commitment.
		if isUnavailableBALEntry(entry) {
			continue
		}
		if len(entry) == 1 && entry[0] == 0xc0 {
			if expected != empty.BlockAccessListHash {
				responseErr = fmt.Errorf("%w: req %d wanted non-empty BAL %x, peer returned empty", ErrBadBALResponse, i, expected)
				continue
			}
		}
		hash := crypto.Keccak256Hash(entry)
		if hash != expected {
			responseErr = fmt.Errorf("%w: req %d wanted %x got %x", ErrBadBALResponse, i, expected, hash)
			continue
		}
		// The peer supplied the bytes committed by the header. If their contents
		// are invalid, discard this BAL and continue checking the other entries.
		bal, err := types.DecodeBlockAccessListSidecarForHeader(entry, reqs[i].GasLimit)
		if err != nil {
			continue
		}
		if err = bal.ValidateForHeader(reqs[i].GasLimit); err != nil {
			continue
		}
		out[reqs[i].Hash] = bal
	}
	return out, responseErr
}

func decodeBlockAccessListsResponse(payload []byte, requested int) ([]rlp.RawValue, error) {
	if len(payload) == 0 {
		return nil, nil
	}

	stream := rlp.NewBytesStream(payload)
	defer rlp.PutStream(stream)

	var entries []rlp.RawValue
	for stream.Remaining() > 0 {
		// Raw copies the whole entry, so check the request limit before reading it.
		if len(entries) == requested {
			return nil, fmt.Errorf("%w: response exceeds requested count %d", ErrBadBALResponse, requested)
		}
		entry, err := stream.Raw()
		if err != nil {
			return nil, fmt.Errorf("decode block access list entry %d: %w", len(entries), err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (f *balFetcher) fetchOnce(ctx context.Context, reqs []BALRequest, peerId *PeerId, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	messages, unregister := subscribeResponses(ctx, f.messageListener.RegisterBlockAccessListsObserver)
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
	message, _, err := awaitResponse(ctx, timeout, messages, matchBlockAccessLists(peerId, requestId))
	if err != nil {
		return nil, err
	}
	return message.EncodedAccessLists, nil
}

func (f *balFetcher) penalize(ctx context.Context, peerId *PeerId) {
	err := f.peerPenalizer.Penalize(ctx, peerId)
	if err != nil {
		f.logger.Debug("[p2p.bal] failed to penalize peer", "peerId", peerId, "err", err)
	}
}

func matchBlockAccessLists(peerId *PeerId, requestId uint64) func(*DecodedInboundMessage[BlockAccessListsEnvelope]) bool {
	return func(message *DecodedInboundMessage[BlockAccessListsEnvelope]) bool {
		return matchesRequest(peerId, message.PeerId, requestId, message.Decoded.RequestId)
	}
}
