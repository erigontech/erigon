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
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// fakeSentry is a minimal sentryproto.SentryClient used to exercise the BAL
// fetcher without a real gRPC sentry. It records SendMessageById calls, so
// tests can decode the outgoing GetBlockAccessLists to learn the request id,
// and records PenalizePeer calls so tests can assert on bad-peer handling.
type fakeSentry struct {
	sentryproto.SentryClient

	mu              sync.Mutex
	sent            []*sentryproto.SendMessageByIdRequest
	penalized       []*sentryproto.PenalizePeerRequest
	onSendMessageID func(*sentryproto.SendMessageByIdRequest)
}

func (f *fakeSentry) SendMessageById(_ context.Context, in *sentryproto.SendMessageByIdRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
	f.mu.Lock()
	f.sent = append(f.sent, in)
	cb := f.onSendMessageID
	f.mu.Unlock()
	if cb != nil {
		cb(in)
	}
	return &sentryproto.SentPeers{Peers: []*typesproto.H512{in.PeerId}}, nil
}

func (f *fakeSentry) PenalizePeer(_ context.Context, in *sentryproto.PenalizePeerRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	f.mu.Lock()
	f.penalized = append(f.penalized, in)
	f.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func makeHash(v byte) common.Hash {
	var h common.Hash
	for i := range h {
		h[i] = v
	}
	return h
}

// waitForRequestID blocks until the fetcher has sent its GetBlockAccessLists
// and registered the request id in its in-flight map. Returns the decoded
// request id so the test can craft a matching response.
func waitForRequestID(t *testing.T, sentry *fakeSentry) uint64 {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sentry.mu.Lock()
		sent := append([]*sentryproto.SendMessageByIdRequest(nil), sentry.sent...)
		sentry.mu.Unlock()
		if len(sent) == 0 {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		var pkt eth.GetBlockAccessListsPacket66
		if err := rlp.DecodeBytes(sent[0].Data.Data, &pkt); err != nil {
			t.Fatalf("decode sent GetBlockAccessLists: %v", err)
		}
		return pkt.RequestId
	}
	t.Fatal("fetcher did not send GetBlockAccessLists within 1s")
	return 0
}

// TestBALFetcher_ValidPopulatedResponse verifies a well-formed populated BAL
// passes validation and is returned unchanged to the caller.
func TestBALFetcher_ValidPopulatedResponse(t *testing.T) {
	f := NewBALFetcher()
	sentry := &fakeSentry{}

	peerID := [64]byte{0xaa}
	blockHash := makeHash(0x01)
	// A valid, non-trivial BAL payload stand-in.
	balPayload, err := rlp.EncodeToBytes([]any{[]byte{0x01, 0x02}, []byte{0x03}})
	if err != nil {
		t.Fatalf("encode stub BAL: %v", err)
	}
	expected := common.BytesToHash(keccak256(balPayload))

	// Deliver the response as soon as the fetcher registers the request.
	done := make(chan struct{})
	sentry.onSendMessageID = func(*sentryproto.SendMessageByIdRequest) {
		go func() {
			defer close(done)
			// Small wait so the fetcher has time to enter the select; no race
			// involved because deliver is buffered(1) — order-independent.
			reqID := waitForRequestID(t, sentry)
			f.Deliver(peerID, &eth.BlockAccessListsPacket66{
				RequestId:              reqID,
				BlockAccessListsPacket: []rlp.RawValue{balPayload},
			})
		}()
	}

	got, err := f.FetchBlockAccessLists(context.Background(), sentry, peerID, []common.Hash{blockHash}, []common.Hash{expected})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	<-done
	if len(got) != 1 {
		t.Fatalf("result len: have %d, want 1", len(got))
	}
	if !bytes.Equal(got[0], balPayload) {
		t.Errorf("payload mismatch")
	}
	if len(sentry.penalized) != 0 {
		t.Errorf("unexpected penalty on valid response: %+v", sentry.penalized)
	}
}

// TestBALFetcher_EmptyBALAcceptedOnlyWhenExpected verifies the empty-RLP
// ambiguity is resolved correctly: 0xc0 is accepted iff the expected hash
// equals empty.BlockAccessListHash; otherwise it is returned as "not
// available" (nil slot) without penalising the peer.
func TestBALFetcher_EmptyBALAcceptedOnlyWhenExpected(t *testing.T) {
	f := NewBALFetcher()
	sentry := &fakeSentry{}

	peerID := [64]byte{0xbb}
	// Two blocks: first expects an empty BAL, second expects a non-empty one.
	blockHashes := []common.Hash{makeHash(0x01), makeHash(0x02)}
	expected := []common.Hash{empty.BlockAccessListHash, makeHash(0xee)}

	sentry.onSendMessageID = func(*sentryproto.SendMessageByIdRequest) {
		go func() {
			reqID := waitForRequestID(t, sentry)
			f.Deliver(peerID, &eth.BlockAccessListsPacket66{
				RequestId: reqID,
				// Both slots return 0xc0. First slot — peer says "block has
				// empty BAL", which matches expected[0]. Second slot — peer
				// says "I don't have this one", expected[1] non-empty.
				BlockAccessListsPacket: []rlp.RawValue{{0xc0}, {0xc0}},
			})
		}()
	}

	got, err := f.FetchBlockAccessLists(context.Background(), sentry, peerID, blockHashes, expected)
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("result len: have %d, want 2", len(got))
	}
	if !bytes.Equal(got[0], rlp.RawValue{0xc0}) {
		t.Errorf("slot 0: expected empty BAL accepted as 0xc0, got %x", got[0])
	}
	if got[1] != nil {
		t.Errorf("slot 1: expected nil (not available), got %x", got[1])
	}
	if len(sentry.penalized) != 0 {
		t.Errorf("unexpected penalty on benign empty replies: %+v", sentry.penalized)
	}
}

// TestBALFetcher_HashMismatchPenalisesPeer verifies a non-empty payload whose
// keccak256 does not match the expected hash triggers ErrBadBALResponse and
// issues a PenalizePeer kick.
func TestBALFetcher_HashMismatchPenalisesPeer(t *testing.T) {
	f := NewBALFetcher()
	sentry := &fakeSentry{}

	peerID := [64]byte{0xcc}
	blockHash := makeHash(0x01)
	garbage := []byte{0x81, 0x99} // valid RLP (single byte-string) but arbitrary
	wrongExpected := makeHash(0xde)

	sentry.onSendMessageID = func(*sentryproto.SendMessageByIdRequest) {
		go func() {
			reqID := waitForRequestID(t, sentry)
			f.Deliver(peerID, &eth.BlockAccessListsPacket66{
				RequestId:              reqID,
				BlockAccessListsPacket: []rlp.RawValue{garbage},
			})
		}()
	}

	_, err := f.FetchBlockAccessLists(context.Background(), sentry, peerID, []common.Hash{blockHash}, []common.Hash{wrongExpected})
	if !errors.Is(err, ErrBadBALResponse) {
		t.Fatalf("expected ErrBadBALResponse, got %v", err)
	}
	if len(sentry.penalized) != 1 {
		t.Fatalf("expected exactly 1 penalty, got %d: %+v", len(sentry.penalized), sentry.penalized)
	}
	got := sentry.penalized[0]
	if got.Penalty != sentryproto.PenaltyKind_Kick {
		t.Errorf("expected Kick penalty, got %v", got.Penalty)
	}
	// Sanity-check: peer id round-trips to the same 64 bytes.
	if got.PeerId == nil {
		t.Error("penalise request missing peer id")
	}
}

// TestBALFetcher_DeliverIgnoresUnknownRequestID verifies that stale /
// unexpected responses (no matching in-flight entry) are silently dropped.
func TestBALFetcher_DeliverIgnoresUnknownRequestID(t *testing.T) {
	f := NewBALFetcher()
	peerID := [64]byte{0xdd}
	delivered := f.Deliver(peerID, &eth.BlockAccessListsPacket66{
		RequestId:              0xdeadbeef,
		BlockAccessListsPacket: []rlp.RawValue{{0xc0}},
	})
	if delivered {
		t.Fatal("Deliver should return false for an unknown request id")
	}
}

// TestBALFetcher_DeliverIgnoresWrongPeer verifies responses from peers we did
// NOT send the request to are dropped (anti-spoofing at the fetcher layer).
func TestBALFetcher_DeliverIgnoresWrongPeer(t *testing.T) {
	f := NewBALFetcher()
	sentry := &fakeSentry{}
	targetPeer := [64]byte{0x11}
	wrongPeer := [64]byte{0x22}

	blockHash := makeHash(0x01)
	expected := makeHash(0xee)

	sentry.onSendMessageID = func(*sentryproto.SendMessageByIdRequest) {
		go func() {
			reqID := waitForRequestID(t, sentry)
			// Impostor delivers first — must be ignored.
			if f.Deliver(wrongPeer, &eth.BlockAccessListsPacket66{
				RequestId:              reqID,
				BlockAccessListsPacket: []rlp.RawValue{{0xc0}},
			}) {
				t.Errorf("Deliver should return false for response from wrong peer")
			}
			// Target peer replies; expected != empty-list-hash → returns nil
			// for that slot (not available), not a penalty.
			f.Deliver(targetPeer, &eth.BlockAccessListsPacket66{
				RequestId:              reqID,
				BlockAccessListsPacket: []rlp.RawValue{{0xc0}},
			})
		}()
	}

	got, err := f.FetchBlockAccessLists(context.Background(), sentry, targetPeer, []common.Hash{blockHash}, []common.Hash{expected})
	if err != nil {
		t.Fatalf("fetch failed: %v", err)
	}
	if len(got) != 1 || got[0] != nil {
		t.Errorf("expected single nil slot (not available), got %v", got)
	}
	if len(sentry.penalized) != 0 {
		t.Errorf("impostor should not cause a penalty: %+v", sentry.penalized)
	}
}
