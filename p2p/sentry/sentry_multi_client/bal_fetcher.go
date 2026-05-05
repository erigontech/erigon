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

package sentry_multi_client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"golang.org/x/crypto/sha3"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// ErrBadBALResponse is returned by BALFetcher.FetchBlockAccessLists when a peer
// returns a non-empty BAL payload whose keccak256 does not match the expected
// header-committed hash. The peer has been penalised (kicked) at the sentry
// layer before this error is returned.
var ErrBadBALResponse = errors.New("bal: peer returned BAL with mismatched hash")

// ErrFetchTimeout is returned when a peer does not reply within the configured
// deadline. It is not a bad-peer signal on its own — the caller retries with
// another peer and may accumulate score over repeated timeouts.
var ErrFetchTimeout = errors.New("bal: fetch timed out waiting for peer response")

// ErrPeerGone is returned when SendMessageById accepts the request without
// error but the sentry's reply lists zero peers — i.e. the target peer
// disconnected (or routed to a different sentry) between selection and send.
// Caller should retry from a different peer next pass.
var ErrPeerGone = errors.New("bal: target peer no longer reachable on this sentry")

// defaultFetchTimeout bounds a single GetBlockAccessLists → BlockAccessLists
// round trip. EIP-8159 suggests a 2 MiB response cap; this timeout leaves
// plenty of room for slow peers without blocking sync indefinitely.
const defaultFetchTimeout = 30 * time.Second

// balRequest is a pending GetBlockAccessLists request waiting for its peer
// response. The inbound handler looks up an entry by RequestId in the fetcher's
// inflight map and forwards the decoded packet via the deliver channel.
type balRequest struct {
	// peer is the peer we sent this request to. Responses from any other peer
	// are ignored (a fetcher is always scoped to one target peer per request).
	peer [64]byte

	// n is the expected number of entries in the response — equal to the
	// number of block hashes in the original request.
	n int

	// deliver is a single-use, buffered(1) channel. Never closed: leak-freedom
	// comes from the buffered, non-blocking send in Deliver() plus the defer
	// delete(f.inflight, reqID) in FetchBlockAccessLists, which prevents any
	// later Deliver from finding this request and attempting another send.
	deliver chan []rlp.RawValue
}

// BALFetcher issues GetBlockAccessLists requests to specific peers and waits
// for validated BlockAccessLists responses (EIP-8159 / eth/71).
//
// The fetcher owns the request-id → waiting-goroutine mapping. It does NOT
// own sentry subscriptions; the MultiClient's existing blockAccessLists71
// inbound handler delivers responses to the fetcher by calling Deliver().
//
// Validation (per EIP-8159 post ethereum/EIPs#11553):
//   - 0x80 (empty RLP string) — peer's "not available" sentinel. Returned to
//     the caller as nil so they can retry from a different peer.
//   - 0xc0 (empty RLP list) — peer claims the BAL is genuinely empty.
//     Accepted only if the caller-supplied expectedHash equals
//     empty.BlockAccessListHash; otherwise the peer is lying and is kicked.
//   - any other payload — must hash (keccak256) to expectedHash. Otherwise
//     ErrBadBALResponse is returned and the peer is kicked via Sentry.PenalizePeer.
//
// This fetcher intentionally covers only the one-shot, explicit-peer request
// flow. A production sync stage integration (peer selection, pipelining,
// score-based withholding detection) will build on top of this primitive.
type BALFetcher struct {
	mu       sync.Mutex
	inflight map[uint64]*balRequest
}

// NewBALFetcher constructs an empty fetcher. Safe to share across goroutines.
func NewBALFetcher() *BALFetcher {
	return &BALFetcher{
		inflight: map[uint64]*balRequest{},
	}
}

// FetchBlockAccessLists sends a GetBlockAccessLists request to peerID and
// returns the validated BALs aligned positionally with blockHashes.
//
// expectedHashes MUST have the same length as blockHashes and carry the
// BlockAccessListHash each block's header commits to. Entries where the peer
// does not have the BAL (payload = 0x80, EIP-8159 not-available sentinel)
// are returned as nil — callers should retry those from a different peer.
//
// If the peer returns a non-sentinel payload whose keccak256 does not equal
// the expected hash, the peer is penalised (Sentry.PenalizePeer with Kick)
// and ErrBadBALResponse is returned without waiting for retries. The same
// applies if the peer returns 0xc0 (genuinely empty BAL claim) but the
// expected hash isn't the empty-BAL hash. A cancelled context or the default
// timeout each return their respective error; neither penalises the peer.
func (f *BALFetcher) FetchBlockAccessLists(
	ctx context.Context,
	sentry sentryproto.SentryClient,
	peerID [64]byte,
	blockHashes []common.Hash,
	expectedHashes []common.Hash,
) ([]rlp.RawValue, error) {
	if len(blockHashes) != len(expectedHashes) {
		return nil, fmt.Errorf("bal: blockHashes (%d) and expectedHashes (%d) length mismatch", len(blockHashes), len(expectedHashes))
	}
	if len(blockHashes) == 0 {
		return nil, nil
	}

	reqID := rand.Uint64() //nolint:gosec // cryptographic randomness not required for request id
	packet := eth.GetBlockAccessListsPacket66{
		RequestId:                 reqID,
		GetBlockAccessListsPacket: blockHashes,
	}
	encoded, err := rlp.EncodeToBytes(&packet)
	if err != nil {
		return nil, fmt.Errorf("bal: encode GetBlockAccessLists request: %w", err)
	}

	req := &balRequest{
		peer:    peerID,
		n:       len(blockHashes),
		deliver: make(chan []rlp.RawValue, 1),
	}
	f.mu.Lock()
	// In the astronomically-unlikely event of a request id collision, fail
	// fast rather than silently stealing the older waiter's delivery channel.
	if _, clash := f.inflight[reqID]; clash {
		f.mu.Unlock()
		return nil, fmt.Errorf("bal: request id %d already in flight (collision)", reqID)
	}
	f.inflight[reqID] = req
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.inflight, reqID)
		f.mu.Unlock()
	}()

	outreq := sentryproto.SendMessageByIdRequest{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Data: &sentryproto.OutboundMessageData{
			Id:   sentryproto.MessageId_GET_BLOCK_ACCESS_LISTS_71,
			Data: encoded,
		},
	}
	sent, err := sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return nil, fmt.Errorf("bal: send GetBlockAccessLists: %w", err)
	}
	// SendMessageById returns (reply, nil) with empty Peers when the peer
	// isn't reachable on this sentry — either because it disconnected since
	// pickEth71Peer or because of the multi-sentry peer-routing TODO in
	// sentry_grpc_server.go. Without this check we'd register the in-flight
	// request and waste the full defaultFetchTimeout waiting for a reply
	// that will never come.
	if sent == nil || len(sent.GetPeers()) == 0 {
		return nil, ErrPeerGone
	}

	timer := time.NewTimer(defaultFetchTimeout)
	defer timer.Stop()

	var response []rlp.RawValue
	select {
	case response = <-req.deliver:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrFetchTimeout
	}

	// Validate length. If the peer returned fewer entries than we asked for,
	// pad with nil rather than error — that's equivalent to "not available"
	// for the missing slots. Shorter-than-requested is allowed by the spec
	// when the peer hits its softResponseLimit.
	if len(response) > len(blockHashes) {
		// More entries than requested: peer is misbehaving. Kick.
		f.penalise(ctx, sentry, peerID)
		return nil, fmt.Errorf("%w: peer returned %d entries for %d-hash request", ErrBadBALResponse, len(response), len(blockHashes))
	}

	out := make([]rlp.RawValue, len(blockHashes))
	for i := range response {
		entry := response[i]
		expected := expectedHashes[i]
		// EIP-8159 (post ethereum/EIPs#11553) three-way decode:
		// 0x80 = "not available", 0xc0 = "genuinely empty BAL",
		// anything else = actual BAL bytes that must hash to expected.
		if len(entry) == 0 || (len(entry) == 1 && entry[0] == 0x80) {
			// Peer doesn't have it. Leave out[i] = nil so the caller can
			// retry from a different peer.
			continue
		}
		if len(entry) == 1 && entry[0] == 0xc0 {
			// Peer claims the BAL is genuinely empty. Accept only if the
			// expected hash actually is the empty-BAL hash; otherwise the
			// peer is lying about empty — treat as a hash mismatch.
			if expected != empty.BlockAccessListHash {
				f.penalise(ctx, sentry, peerID)
				return nil, fmt.Errorf("%w: entry %d expected non-empty BAL with hash %x, peer returned 0xc0", ErrBadBALResponse, i, expected)
			}
			out[i] = rlp.RawValue{0xc0}
			continue
		}
		// Non-sentinel payload — must hash to expected.
		if !bytes.Equal(keccak256(entry), expected[:]) {
			f.penalise(ctx, sentry, peerID)
			return nil, fmt.Errorf("%w: entry %d expected=%x got-hash=%x", ErrBadBALResponse, i, expected, keccak256(entry))
		}
		out[i] = entry
	}
	return out, nil
}

// Deliver routes an inbound BlockAccessLists response to the fetcher goroutine
// that issued the original GetBlockAccessLists request. Called from the sentry
// inbound message handler after the packet has been decoded. Returns true if a
// matching in-flight request was found AND the peer matches; false otherwise
// (unknown request id, late arrival after timeout, or spoofed peer).
func (f *BALFetcher) Deliver(peerID [64]byte, packet *eth.BlockAccessListsPacket66) bool {
	if packet == nil {
		return false
	}
	f.mu.Lock()
	req, ok := f.inflight[packet.RequestId]
	f.mu.Unlock()
	if !ok {
		return false
	}
	if req.peer != peerID {
		// Response from a peer we didn't ask. Drop.
		return false
	}
	// Non-blocking send: deliver is buffered(1); if the slot is full (duplicate
	// response), the second message is discarded safely.
	select {
	case req.deliver <- packet.BlockAccessListsPacket:
		return true
	default:
		return false
	}
}

// penalise kicks the peer via the provided sentry. Best-effort — logs nothing
// on failure because the caller returns an error that carries full context.
func (f *BALFetcher) penalise(ctx context.Context, sentry sentryproto.SentryClient, peerID [64]byte) {
	_, _ = sentry.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{
		PeerId:  gointerfaces.ConvertHashToH512(peerID),
		Penalty: sentryproto.PenaltyKind_Kick,
	}, &grpc.EmptyCallOption{})
}

// keccak256 computes the Ethereum Keccak-256 hash of the given bytes.
// Kept local to the fetcher to avoid a wider import.
func keccak256(b []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(b)
	return h.Sum(nil)
}
