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

package follower_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/follower"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
)

// replies maps the requests a follower sends to the replies it waits for.
var replies = map[sentryproto.MessageId]sentryproto.MessageId{
	sentryproto.MessageId_GET_BLOCK_HEADERS_66: sentryproto.MessageId_BLOCK_HEADERS_66,
	sentryproto.MessageId_GET_BLOCK_BODIES_66:  sentryproto.MessageId_BLOCK_BODIES_66,
}

// peerSentry is the sentry of a follower with at most one peer, a node that
// serves its chain through its own devp2p handlers: requests reach the
// node's mock sentry and its replies come back as the peer's messages.
type peerSentry struct {
	sentryproto.UnimplementedSentryServer
	node *execmoduletester.ExecModuleTester
	id   *typesproto.H512
	// tamper, when set, rewrites the node's replies.
	tamper func(id sentryproto.MessageId, data []byte) []byte
	// hold, when set, delays the reply to each request by the duration it
	// returns, or withholds it when the duration is negative.
	hold func(id sentryproto.MessageId) time.Duration

	// serve orders requests, whose replies the node appends to its log.
	serve  sync.Mutex
	served int

	mu        sync.Mutex
	connected bool
	// untracked withholds the peer's connection event, so that the
	// downloader's tracker lags the sentry.
	untracked bool
	requests  []sentryproto.MessageId
	penalties int
	streams   map[sentryproto.MessageId][]sentryproto.Sentry_MessagesServer
}

func (s *peerSentry) HandShake(context.Context, *emptypb.Empty) (*sentryproto.HandShakeReply, error) {
	return &sentryproto.HandShakeReply{Protocol: sentryproto.Protocol_ETH69}, nil
}

func (s *peerSentry) SetStatus(context.Context, *sentryproto.StatusData) (*sentryproto.SetStatusReply, error) {
	return &sentryproto.SetStatusReply{}, nil
}

func (s *peerSentry) PenalizePeer(context.Context, *sentryproto.PenalizePeerRequest) (*emptypb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.penalties++
	return &emptypb.Empty{}, nil
}

func (s *peerSentry) PeerCount(context.Context, *sentryproto.PeerCountRequest) (*sentryproto.PeerCountReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected {
		return &sentryproto.PeerCountReply{}, nil
	}
	return &sentryproto.PeerCountReply{Count: 1}, nil
}

func (s *peerSentry) Peers(context.Context, *emptypb.Empty) (*sentryproto.PeersReply, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.connected {
		return &sentryproto.PeersReply{}, nil
	}
	return &sentryproto.PeersReply{Peers: []*typesproto.PeerInfo{{Id: libsentry.AsPeerIdString(s.id)}}}, nil
}

func (s *peerSentry) PeerEvents(_ *sentryproto.PeerEventsRequest, stream sentryproto.Sentry_PeerEventsServer) error {
	s.mu.Lock()
	connected := s.connected && !s.untracked
	s.mu.Unlock()
	if connected {
		if err := stream.Send(&sentryproto.PeerEvent{PeerId: s.id, EventId: sentryproto.PeerEvent_Connect}); err != nil {
			return err
		}
	}
	<-stream.Context().Done()
	return nil
}

func (s *peerSentry) Messages(req *sentryproto.MessagesRequest, stream sentryproto.Sentry_MessagesServer) error {
	s.mu.Lock()
	for _, id := range req.Ids {
		s.streams[id] = append(s.streams[id], stream)
	}
	s.mu.Unlock()
	<-stream.Context().Done()
	return nil
}

func (s *peerSentry) SendMessageById(_ context.Context, req *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req.Data.Id)
	connected := s.connected
	s.mu.Unlock()
	want, ok := replies[req.Data.Id]
	if !connected || !ok {
		return &sentryproto.SentPeers{}, nil
	}

	s.serve.Lock()
	s.node.ReceiveWg.Add(1)
	errs := s.node.Send(&sentryproto.InboundMessage{Id: req.Data.Id, Data: req.Data.Data, PeerId: s.id})
	s.node.ReceiveWg.Wait()
	var reply *sentryproto.OutboundMessageData
	for reply == nil {
		msg, err := s.node.SentMessage(s.served)
		if err != nil {
			s.serve.Unlock()
			return nil, fmt.Errorf("no %s reply: %w", want, err)
		}
		s.served++
		if msg.Id == want {
			reply = msg
		}
	}
	s.serve.Unlock()
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	data := reply.Data
	if s.tamper != nil {
		data = s.tamper(reply.Id, data)
	}
	s.mu.Lock()
	streams := slices.Clone(s.streams[reply.Id])
	s.mu.Unlock()
	send := func() {
		for _, stream := range streams {
			_ = stream.Send(&sentryproto.InboundMessage{Id: reply.Id, Data: data, PeerId: s.id})
		}
	}
	var delay time.Duration
	if s.hold != nil {
		delay = s.hold(req.Data.Id)
	}
	switch {
	case delay < 0:
	case delay > 0:
		time.AfterFunc(delay, send)
	default:
		send()
	}
	return &sentryproto.SentPeers{Peers: []*typesproto.H512{s.id}}, nil
}

// newPeerSentry returns the sentry of a follower whose one peer is node,
// connected or not.
func newPeerSentry(node *execmoduletester.ExecModuleTester, connected bool) *peerSentry {
	s := &peerSentry{node: node, id: execp2p.PeerIdFromUint64(1).H512(), connected: connected, streams: map[sentryproto.MessageId][]sentryproto.Sentry_MessagesServer{}}
	// The node's replies to earlier peers are not this one's.
	for _, err := node.SentMessage(s.served); err == nil; _, err = node.SentMessage(s.served) {
		s.served++
	}
	return s
}

// sent returns the requests the follower sent.
func (s *peerSentry) sent() []sentryproto.MessageId {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.requests)
}

func (s *peerSentry) penalized() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.penalties
}

// sentryPeers counts the peers of a sentry.
type sentryPeers struct{ sentry sentryproto.SentryClient }

func (p sentryPeers) NetPeerCount() (uint64, error) {
	r, err := p.sentry.PeerCount(context.Background(), &sentryproto.PeerCountRequest{})
	if err != nil {
		return 0, err
	}
	return r.Count, nil
}

// newFetcher returns a follower's fetcher of blocks into chain from its
// sentry s, built as the node builds its backward block downloader.
func newFetcher(t *testing.T, s *peerSentry, chain deriver.Chain) *follower.Fetcher {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger := log.New()
	client, err := direct.NewSentryClientDirect(direct.ETH69, s)
	require.NoError(t, err)
	mux := libsentry.NewSentryMultiplexer([]sentryproto.SentryClient{client})
	penalizer := execp2p.NewPeerPenalizer(mux)
	status := func(context.Context) (*sentryproto.StatusData, error) { return &sentryproto.StatusData{}, nil }
	listener := execp2p.NewMessageListener(logger, mux, status, penalizer)
	tracker := execp2p.NewPeerTracker(logger, listener)
	var fetcher execp2p.Fetcher = execp2p.NewFetcher(logger, listener, execp2p.NewMessageSender(mux))
	fetcher = execp2p.NewPenalizingFetcher(logger, fetcher, penalizer)
	fetcher = execp2p.NewTrackingFetcher(fetcher, tracker)
	go func() { _ = listener.Run(ctx) }()
	go func() { _ = tracker.Run(ctx) }()
	// Ready once the listener streams the replies and the tracker knows the
	// peer.
	require.Eventually(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, id := range replies {
			if len(s.streams[id]) == 0 {
				return false
			}
		}
		return !s.connected || s.untracked || len(tracker.ListPeers()) == 1
	}, 10*time.Second, 5*time.Millisecond)
	bbd := execp2p.NewBackwardBlockDownloader(logger, fetcher, penalizer, tracker, t.TempDir())
	return follower.NewFetcher(bbd, sentryPeers{mux}, chain, logger)
}

// fetchChain composes blocks of the devnet whose Sync blocks hold system
// transactions: more than one downloaded batch of them.
func fetchChain(t *testing.T) *derivertest.Composer {
	t.Helper()
	c := derivertest.NewComposer(t)
	for range 3 {
		blocks := make([]derivertest.Block, 170)
		for i := range blocks {
			blocks[i] = derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}}
		}
		blocks[0].Txs = [][]byte{c.Transfer(bob, 1)}
		b := c.Compose([]da.Action{{
			SourceRollupID: protocol.MainnetRollupID, TargetRollupID: 1, SourceAddress: bob, TargetAddress: alice,
			Data: []byte{}, Success: true, ReturnData: []byte{},
		}}, blocks...)
		c.Post(derivertest.Posting{Batch: b})
	}
	return c
}

// canonical returns the canonical blocks of chain from first to last.
func canonical(t *testing.T, chain deriver.Chain, first, last uint64) []*types.Block {
	t.Helper()
	var blocks []*types.Block
	for n := first; n <= last; n++ {
		b, err := chain.BlockByNumber(t.Context(), n)
		require.NoError(t, err)
		require.NotNil(t, b, "block %d", n)
		blocks = append(blocks, b)
	}
	return blocks
}

func countSystemTxs(blocks []*types.Block) int {
	var n int
	for _, b := range blocks {
		for _, tx := range b.Transactions() {
			if tx.Type() == protocol.SystemTxType {
				n++
			}
		}
	}
	return n
}

// TestFetch: a follower fetches by hash, from its L2 peer over devp2p, the
// blocks it does not store, Sync blocks with system transactions among
// them, and inserts them in batches without moving its head. A body that
// does not match its header is rejected and its peer penalized. Without a
// peer the downloader tracks, or one that has the target, it fails with
// ErrNoPeers. Limit bounds both the distance from the local head and the
// missing chain.
func TestFetch(t *testing.T) {
	c := fetchChain(t)
	head := c.Head()
	want := canonical(t, c.Chain, 1, head.Number.Uint64())
	require.Equal(t, 3, countSystemTxs(want))
	newPeer := func(t *testing.T) (*peerSentry, *deriver.ExecChain) {
		_, chain := derivertest.NewL2(t)
		return newPeerSentry(c.L2, true), chain
	}
	// first records the first block an Inserted callback sees.
	first := func(n *uint64) func(context.Context, []*types.Block) error {
		return func(_ context.Context, blocks []*types.Block) error {
			if *n == 0 {
				*n = blocks[0].NumberU64()
			}
			return nil
		}
	}
	noInsert := func(t *testing.T) func(context.Context, []*types.Block) error {
		return func(context.Context, []*types.Block) error {
			t.Error("blocks inserted")
			return nil
		}
	}

	t.Run("by hash", func(t *testing.T) {
		s, chain := newPeer(t)
		f := newFetcher(t, s, chain)
		var batches [][2]uint64
		got, err := f.Fetch(t.Context(), head.Hash(), follower.FetchOptions{Inserted: func(_ context.Context, blocks []*types.Block) error {
			batches = append(batches, [2]uint64{blocks[0].NumberU64(), blocks[len(blocks)-1].NumberU64()})
			return nil
		}})
		require.NoError(t, err)
		require.Equal(t, head.Hash(), got.Hash())
		require.Equal(t, [][2]uint64{{1, 500}, {501, head.Number.Uint64()}}, batches)
		local, err := chain.Head(t.Context())
		require.NoError(t, err)
		require.Zero(t, local.Number.Uint64(), "the head stays")
		require.NotEmpty(t, s.sent())

		// The inserted blocks execute to the peer's state.
		require.NoError(t, chain.ForkChoice(t.Context(), head.Hash(), common.Hash{}, common.Hash{}))
		derivertest.RequireChain(t, chain, want...)
		for _, addr := range []common.Address{protocol.SystemAddress, derivertest.UserAddress()} {
			wantNonce, err := c.Chain.Nonce(t.Context(), head.Number.Uint64(), addr)
			require.NoError(t, err)
			gotNonce, err := chain.Nonce(t.Context(), head.Number.Uint64(), addr)
			require.NoError(t, err)
			require.Equal(t, wantNonce, gotNonce, "nonce of %x", addr)
		}

		// A canonical target needs no peer.
		requests := len(s.sent())
		got, err = f.Fetch(t.Context(), want[100].Hash(), follower.FetchOptions{Inserted: noInsert(t)})
		require.NoError(t, err)
		require.Equal(t, want[100].Hash(), got.Hash())
		require.Len(t, s.sent(), requests)
	})

	t.Run("stored blocks", func(t *testing.T) {
		s, chain := newPeer(t)
		// So few blocks stay in the execution module's overlay until a fork
		// choice.
		require.NoError(t, chain.Insert(t.Context(), want[:10]))
		f := newFetcher(t, s, chain)
		// A target stored off the canonical chain needs no peer.
		got, err := f.Fetch(t.Context(), want[9].Hash(), follower.FetchOptions{Inserted: noInsert(t)})
		require.NoError(t, err)
		require.Equal(t, want[9].Hash(), got.Hash())
		require.Empty(t, s.sent())
		// A fetch builds on stored blocks, canonical or not.
		require.NoError(t, chain.Insert(t.Context(), want[10:300]))
		var from uint64
		_, err = f.Fetch(t.Context(), head.Hash(), follower.FetchOptions{Inserted: first(&from)})
		require.NoError(t, err)
		require.Equal(t, uint64(301), from)

		_, chain = derivertest.NewL2(t)
		require.NoError(t, chain.Insert(t.Context(), want[:100]))
		require.NoError(t, chain.ForkChoice(t.Context(), want[99].Hash(), common.Hash{}, common.Hash{}))
		from = 0
		_, err = newFetcher(t, newPeerSentry(c.L2, true), chain).Fetch(t.Context(), head.Hash(), follower.FetchOptions{Inserted: first(&from)})
		require.NoError(t, err)
		require.Equal(t, uint64(101), from)
	})

	t.Run("body not matching its header", func(t *testing.T) {
		s, chain := newPeer(t)
		var tampered bool
		s.tamper = func(id sentryproto.MessageId, data []byte) []byte {
			if id != sentryproto.MessageId_BLOCK_BODIES_66 {
				return data
			}
			var p eth.BlockBodiesPacket66
			require.NoError(t, rlp.DecodeBytes(data, &p))
			for _, b := range p.BlockBodiesPacket {
				if n := len(b.Transactions); n > 0 && b.Transactions[n-1].Type() == protocol.SystemTxType {
					b.Transactions = b.Transactions[:n-1]
					tampered = true
				}
			}
			out, err := rlp.EncodeToBytes(&p)
			require.NoError(t, err)
			return out
		}
		f := newFetcher(t, s, chain)
		_, err := f.Fetch(t.Context(), head.Hash(), follower.FetchOptions{Inserted: noInsert(t)})
		require.Error(t, err)
		require.True(t, tampered)
		require.Positive(t, s.penalized())
		for _, b := range want {
			h, err := chain.HeaderByHash(t.Context(), b.Hash())
			require.NoError(t, err)
			require.Nil(t, h, "block %d stored", b.NumberU64())
		}
	})

	t.Run("no peer", func(t *testing.T) {
		_, chain := derivertest.NewL2(t)
		s := newPeerSentry(c.L2, false)
		f := newFetcher(t, s, chain)
		_, err := f.Fetch(t.Context(), head.Hash(), follower.FetchOptions{})
		require.ErrorIs(t, err, follower.ErrNoPeers)
		require.Empty(t, s.sent())
	})

	t.Run("peer not yet tracked", func(t *testing.T) {
		_, chain := derivertest.NewL2(t)
		s := newPeerSentry(c.L2, true)
		s.untracked = true
		f := newFetcher(t, s, chain)
		_, err := f.Fetch(t.Context(), head.Hash(), follower.FetchOptions{})
		require.ErrorIs(t, err, follower.ErrNoPeers)
		require.Empty(t, s.sent())
	})

	t.Run("target no peer has", func(t *testing.T) {
		s, chain := newPeer(t)
		f := newFetcher(t, s, chain)
		_, err := f.Fetch(t.Context(), common.HexToHash("0x01"), follower.FetchOptions{})
		require.ErrorIs(t, err, follower.ErrNoPeers)
		require.Equal(t, []sentryproto.MessageId{sentryproto.MessageId_GET_BLOCK_HEADERS_66}, s.sent())
	})

	t.Run("limit", func(t *testing.T) {
		s, chain := newPeer(t)
		f := newFetcher(t, s, chain)
		_, err := f.Fetch(t.Context(), head.Hash(), follower.FetchOptions{Limit: 100, Inserted: noInsert(t)})
		require.ErrorIs(t, err, execp2p.ErrChainLengthExceedsLimit)
		require.Equal(t, []sentryproto.MessageId{sentryproto.MessageId_GET_BLOCK_HEADERS_66}, s.sent(), "only the target's header")
		got, err := f.Fetch(t.Context(), want[99].Hash(), follower.FetchOptions{Limit: 100})
		require.NoError(t, err)
		require.Equal(t, want[99].Hash(), got.Hash())

		// The distance counts from the local head.
		require.NoError(t, chain.ForkChoice(t.Context(), want[99].Hash(), common.Hash{}, common.Hash{}))
		got, err = f.Fetch(t.Context(), head.Hash(), follower.FetchOptions{Limit: head.Number.Uint64() - 100})
		require.NoError(t, err)
		require.Equal(t, head.Hash(), got.Hash())
	})

	t.Run("missing chain past the limit", func(t *testing.T) {
		// A local chain of 100 blocks on another branch: the target 50 above
		// its head lacks 150 blocks.
		other := derivertest.NewComposer(t)
		stray := make([]derivertest.Block, 100)
		for i := range stray {
			stray[i] = derivertest.Block{Coinbase: bob, Extra: []byte("stray"), Txs: [][]byte{}}
		}
		branch := other.Compose(nil, stray...).Blocks
		s, chain := newPeer(t)
		require.NoError(t, chain.Insert(t.Context(), branch))
		require.NoError(t, chain.ForkChoice(t.Context(), branch[99].Hash(), common.Hash{}, common.Hash{}))
		f := newFetcher(t, s, chain)
		_, err := f.Fetch(t.Context(), want[149].Hash(), follower.FetchOptions{Limit: 60, Inserted: noInsert(t)})
		require.ErrorIs(t, err, execp2p.ErrChainLengthExceedsLimit)
		require.NotContains(t, s.sent(), sentryproto.MessageId_GET_BLOCK_BODIES_66)
	})
}
