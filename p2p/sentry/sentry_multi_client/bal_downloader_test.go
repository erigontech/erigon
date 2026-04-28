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
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestPeerSupportsEth71(t *testing.T) {
	cases := []struct {
		name string
		caps []string
		want bool
	}{
		{"empty", nil, false},
		{"only-eth-70", []string{"eth/70"}, false},
		{"eth-71-exact", []string{"eth/71"}, true},
		{"mixed-includes-71", []string{"eth/68", "eth/70", "eth/71"}, true},
		{"prefix-not-match", []string{"eth/710"}, false},
		{"unrelated-protocol", []string{"snap/1", "les/4"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := peerSupportsEth71(&typesproto.PeerInfo{Caps: c.caps})
			if got != c.want {
				t.Errorf("caps=%v: got %v want %v", c.caps, got, c.want)
			}
		})
	}
}

// peersStub is a sentryproto.SentryClient that only implements Peers() — used
// to drive pickEth71PeerFromSentries without standing up a real sentry.
type peersStub struct {
	sentryproto.SentryClient
	peers []*typesproto.PeerInfo
	err   error
	ready *bool // nil = no Ready() method advertised
}

func (s *peersStub) Peers(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*sentryproto.PeersReply, error) {
	if s.err != nil {
		return nil, s.err
	}
	return &sentryproto.PeersReply{Peers: s.peers}, nil
}

// readyStub wraps peersStub and exposes a Ready() bool — pickEth71Peer skips
// sentries that report not-ready.
type readyStub struct {
	*peersStub
	ready bool
}

func (s *readyStub) Ready() bool { return s.ready }

func mkPeer(idByte byte, caps ...string) *typesproto.PeerInfo {
	id := make([]byte, 64)
	for i := range id {
		id[i] = idByte
	}
	return &typesproto.PeerInfo{Id: hex.EncodeToString(id), Caps: caps}
}

func TestPickEth71PeerFromSentries_NoCandidates(t *testing.T) {
	// One sentry with no peers, one with peers that don't speak eth/71.
	cases := [][]sentryproto.SentryClient{
		nil, // zero sentries
		{&peersStub{}},
		{&peersStub{peers: []*typesproto.PeerInfo{mkPeer(0x01, "eth/70")}}},
	}
	for i, sentries := range cases {
		_, idx, found, err := pickEth71PeerFromSentries(context.Background(), sentries, log.New())
		if err != nil {
			t.Errorf("case %d: unexpected error: %v", i, err)
		}
		if found {
			t.Errorf("case %d: expected found=false", i)
		}
		if idx != -1 {
			t.Errorf("case %d: expected idx=-1, got %d", i, idx)
		}
	}
}

func TestPickEth71PeerFromSentries_RecordsCorrectSentryIndex(t *testing.T) {
	// Three sentries: 0 has only eth/70, 1 has eth/71, 2 also has eth/71.
	// The picked candidate must come from sentry 1 or 2 — never 0.
	sentries := []sentryproto.SentryClient{
		&peersStub{peers: []*typesproto.PeerInfo{mkPeer(0xa0, "eth/70")}},
		&peersStub{peers: []*typesproto.PeerInfo{mkPeer(0xb1, "eth/71")}},
		&peersStub{peers: []*typesproto.PeerInfo{mkPeer(0xc2, "eth/71")}},
	}

	// Run several picks; the random choice should always land on idx 1 or 2.
	for i := 0; i < 32; i++ {
		peer, idx, found, err := pickEth71PeerFromSentries(context.Background(), sentries, log.New())
		if err != nil {
			t.Fatalf("iter %d: err %v", i, err)
		}
		if !found {
			t.Fatalf("iter %d: expected found=true", i)
		}
		if idx != 1 && idx != 2 {
			t.Errorf("iter %d: idx=%d, expected 1 or 2", i, idx)
		}
		// Sanity: the peer id from the picked sentry's first byte should match.
		want := byte(0xb1)
		if idx == 2 {
			want = 0xc2
		}
		if peer[0] != want {
			t.Errorf("iter %d: idx=%d got peer[0]=%02x want %02x", i, idx, peer[0], want)
		}
	}
}

func TestPickEth71PeerFromSentries_PeersErrorSkipsSentry(t *testing.T) {
	// Sentry 0 errors, sentry 1 has an eth/71 peer. Pick must succeed and
	// must come from sentry 1.
	sentries := []sentryproto.SentryClient{
		&peersStub{err: errors.New("boom")},
		&peersStub{peers: []*typesproto.PeerInfo{mkPeer(0xb1, "eth/71")}},
	}
	_, idx, found, err := pickEth71PeerFromSentries(context.Background(), sentries, log.New())
	if err != nil {
		t.Fatalf("err %v", err)
	}
	if !found || idx != 1 {
		t.Errorf("expected found from sentry 1, got found=%v idx=%d", found, idx)
	}
}

func TestPickEth71PeerFromSentries_NotReadySentrySkipped(t *testing.T) {
	// Sentry 0 reports not-ready (Ready()==false) and has eth/71 peers, but
	// must be skipped. Sentry 1 is plain (no Ready() method) with eth/71.
	notReady := &readyStub{
		peersStub: &peersStub{peers: []*typesproto.PeerInfo{mkPeer(0xa0, "eth/71")}},
		ready:     false,
	}
	sentries := []sentryproto.SentryClient{
		notReady,
		&peersStub{peers: []*typesproto.PeerInfo{mkPeer(0xb1, "eth/71")}},
	}
	_, idx, found, err := pickEth71PeerFromSentries(context.Background(), sentries, log.New())
	if err != nil {
		t.Fatalf("err %v", err)
	}
	if !found || idx != 1 {
		t.Errorf("not-ready sentry must be skipped: got found=%v idx=%d", found, idx)
	}
}

func TestPickEth71PeerFromSentries_BadHexPeerIdSkipped(t *testing.T) {
	// Peer with invalid hex id must be skipped, not crashed on.
	bad := &typesproto.PeerInfo{Id: "not-hex", Caps: []string{"eth/71"}}
	sentries := []sentryproto.SentryClient{
		&peersStub{peers: []*typesproto.PeerInfo{bad}},
	}
	_, _, found, err := pickEth71PeerFromSentries(context.Background(), sentries, log.New())
	if err != nil {
		t.Fatalf("err %v", err)
	}
	if found {
		t.Error("invalid-hex peer should not be selected")
	}
}
