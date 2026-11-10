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

package overlay

import (
	"context"
	"crypto/rand"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// requireAnonymous fails unless msg carries no author: no from, sequence
// number, signature or key.
func requireAnonymous(t *testing.T, msg *pubsub.Message) {
	t.Helper()
	require.Nil(t, msg.From, "from")
	require.Nil(t, msg.Seqno, "seqno")
	require.Nil(t, msg.Signature, "signature")
	require.Nil(t, msg.Key, "key")
}

// awaitHead reads heads until one has hash; ok is false at the deadline.
func awaitHead(heads <-chan *seqsig.VerifiedHead, hash common.Hash, within time.Duration) (*seqsig.VerifiedHead, bool) {
	timeout := time.After(within)
	for {
		select {
		case h := <-heads:
			if h.Head.BlockHash == hash {
				return h, true
			}
		case <-timeout:
			return nil, false
		}
	}
}

func TestPeerWithoutIdentify(t *testing.T) {
	t.Parallel()
	a := newTestNode(t, 1)
	oa := a.start()
	// A gossipsub peer that serves no identify protocol.
	n := newRawNode(t, 4, nil)
	n.host.RemoveStreamHandler(identify.ID)
	n.host.RemoveStreamHandler(identify.IDPush)
	require.NoError(t, n.host.Connect(t.Context(), a.info()))
	received := n.received(t.Context())
	publishHeads(t.Context(), oa, 100*time.Millisecond)

	// It receives every head A publishes once it is connected.
	var last uint64
	timeout := time.After(deadline)
	for got := range 10 {
		select {
		case msg := <-received:
			requireAnonymous(t, msg)
			e, err := seqsig.Decode(msg.Data)
			require.NoError(t, err)
			if last != 0 {
				require.Equal(t, last+1, e.Head.Counter, "a head was skipped")
			}
			last = e.Head.Counter
		case <-timeout:
			t.Fatalf("the peer without identify received %d heads", got)
		}
	}

	// Its own publish reaches A.
	h := head(nowMs(), 1, 0x77)
	require.NoError(t, n.topic.Publish(t.Context(), mustEncode(0, h)))
	_, ok := awaitHead(oa.Heads(), h.BlockHash, 10*time.Second)
	require.True(t, ok, "A did not receive the head the peer without identify published")
}

func TestSignedMessageNotDelivered(t *testing.T) {
	t.Parallel()
	a := newTestNode(t, 1)
	oa := a.start()
	// A peer on the library's defaults, author, sequence number and
	// signature, publishing to all its peers.
	s := newRawNode(t, 3, []pubsub.Option{pubsub.WithFloodPublish(true)}, a)
	require.Eventually(t, func() bool { return len(s.topic.ListPeers()) == 1 }, deadline, 50*time.Millisecond)

	signedHead := head(nowMs(), 1, 0x55)
	require.NoError(t, s.topic.Publish(t.Context(), mustEncode(0, signedHead)))
	// The same head without author passes: the policy, not the head, decides.
	// It arrives before the first heartbeat grafts a mesh: the publisher
	// floods it.
	plain := head(nowMs()+1, 1, 0x56)
	r := newRawNode(t, 4, nil, a)
	require.Eventually(t, func() bool { return len(r.topic.ListPeers()) == 1 }, deadline, 50*time.Millisecond)
	require.NoError(t, r.topic.Publish(t.Context(), mustEncode(0, plain)))
	_, ok := awaitHead(oa.Heads(), plain.BlockHash, 2*time.Second)
	require.True(t, ok, "A did not deliver the anonymous head")
	_, ok = awaitHead(oa.Heads(), signedHead.BlockHash, 2*time.Second)
	require.False(t, ok, "A delivered a signed message")
}

// largeBlock returns a block whose single transaction carries size random,
// incompressible bytes.
func largeBlock(t *testing.T, size int) *types.Block {
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	signer := types.LatestSignerForChainID(uint256.NewInt(testDomain.ChainID))
	tx, err := types.SignTx(types.NewTransaction(0, common.Address{1}, uint256.NewInt(0), 30_000_000, uint256.NewInt(1), data), *signer, key(9))
	require.NoError(t, err)
	return types.NewBlock(&types.Header{Number: *uint256.NewInt(1), ParentHash: common.Hash{0x11}}, []types.Transaction{tx}, nil, nil, nil, nil)
}

func TestLargeHeadRelays(t *testing.T) {
	t.Parallel()
	a, b, c := newTestNode(t, 1), newTestNode(t, 2), newTestNode(t, 3)
	oa := a.start()
	b.start(a)
	oc := c.start(b)
	ctx, stop := context.WithCancel(t.Context())
	publishHeads(ctx, oa, 100*time.Millisecond)
	awaitCounter(t, oc.Heads(), 1)
	stop()

	blk := largeBlock(t, 2<<20)
	enc, err := rlp.EncodeToBytes(blk)
	require.NoError(t, err)
	h := seqsig.Head{
		RollupID: testDomain.RollupID, Number: blk.NumberU64(), BlockHash: blk.Hash(),
		ParentHash: blk.ParentHash(), EpochMs: nowMs(), Counter: 1,
	}
	sig, err := seqsig.Sign(h, testDomain, key(0))
	require.NoError(t, err)
	e := seqsig.Envelope{Head: h, Signature: sig, Block: enc}
	data, err := seqsig.Encode(e)
	require.NoError(t, err)
	require.Greater(t, len(data), 2<<20, "the envelope compresses")
	require.NoError(t, oa.Publish(t.Context(), e))

	got, ok := awaitHead(oc.Heads(), blk.Hash(), deadline)
	require.True(t, ok, "C did not receive the large head")
	require.NotNil(t, got.Block)
	require.Equal(t, blk.Hash(), got.Block.Hash())
}

func TestSharedKeyThirdConnectionRefused(t *testing.T) {
	a := newTestNode(t, 1)
	oa := a.start()
	id, err := PeerID(&key(7).PublicKey)
	require.NoError(t, err)
	for range 3 {
		h, err := newHost(key(7), ma.StringCast("/ip4/127.0.0.1/tcp/0"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = h.Close() })
		_ = h.Connect(t.Context(), a.info())
	}
	time.Sleep(time.Second)
	require.Len(t, oa.host.Network().ConnsToPeer(id), MaxConnsPerPeer)
}

// field reads the unexported field name of the struct v points to.
func field(v any, name string) reflect.Value {
	return reflect.ValueOf(v).Elem().FieldByName(name)
}

func TestGossipSettings(t *testing.T) {
	h, err := newHost(key(1), ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer h.Close()
	ps, err := pubsub.NewGossipSub(t.Context(), h, gossipOptions()...)
	require.NoError(t, err)

	require.Empty(t, field(ps, "signID").String(), "messages carry no author")
	require.Equal(t, uint64(pubsub.StrictNoSign), field(ps, "signPolicy").Uint())
	require.Equal(t, int64(MaxMessageSize), field(ps, "maxMessageSize").Int())
	require.Equal(t, 10<<20+64<<10, MaxMessageSize)
	require.Equal(t, int64(time.Minute), field(ps, "seenMsgTTL").Int())
	require.True(t, field(ps, "rt").Elem().Elem().FieldByName("floodPublish").Bool(), "flood publish")

	params := field(ps, "rt").Elem().Elem().FieldByName("params")
	for name, want := range map[string]int64{
		"D": 6, "Dlo": 5, "Dhi": 12, "Dscore": 4, "Dout": 2, "Dlazy": 6,
		"HistoryLength": 5, "HistoryGossip": 3, "GossipRetransmission": 3,
		"HeartbeatInitialDelay": int64(5 * time.Second), "HeartbeatInterval": int64(time.Second),
		"FanoutTTL": int64(time.Minute), "PrunePeers": 0, "PruneBackoff": int64(time.Minute),
		"UnsubscribeBackoff": int64(10 * time.Second), "OpportunisticGraftTicks": 60, "OpportunisticGraftPeers": 2,
		"GraftFloodThreshold": int64(10 * time.Second), "MaxIHaveLength": 5000, "MaxIHaveMessages": 10,
		"IWantFollowupTime": int64(3 * time.Second), "IDontWantMessageThreshold": 1000,
	} {
		f := params.FieldByName(name)
		got := int64(0)
		if f.CanInt() {
			got = f.Int()
		} else {
			got = int64(f.Uint())
		}
		require.Equal(t, want, got, name)
	}
	require.InDelta(t, 0.25, params.FieldByName("GossipFactor").Float(), 0)

	// The offered versions are the stream handlers gossipsub registers.
	var offered []protocol.ID
	for _, p := range h.Mux().Protocols() {
		if strings.HasPrefix(string(p), "/meshsub/") || strings.HasPrefix(string(p), "/floodsub/") {
			offered = append(offered, p)
		}
	}
	slices.Sort(offered)
	require.Equal(t, []protocol.ID{pubsub.GossipSubID_v10, pubsub.GossipSubID_v11, pubsub.GossipSubID_v12}, offered)
}
