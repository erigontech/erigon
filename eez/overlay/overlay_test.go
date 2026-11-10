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
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/eez/seqsig"
)

const deadline = time.Minute

type testNode struct {
	t    *testing.T
	key  byte
	port int
	// authorized is the key whose heads the node accepts.
	authorized byte
}

func newTestNode(t *testing.T, key byte) *testNode {
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return &testNode{t: t, key: key, port: port}
}

func (n *testNode) listen() ma.Multiaddr {
	return ma.StringCast(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", n.port))
}

func (n *testNode) info() peer.AddrInfo {
	id, err := PeerID(&key(n.key).PublicKey)
	require.NoError(n.t, err)
	return peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{n.listen()}}
}

func (n *testNode) start(peers ...*testNode) *Overlay {
	cfg := Config{Listen: n.listen(), Key: key(n.key), Domain: testDomain, Signer: addr(n.authorized)}
	for _, p := range peers {
		cfg.Peers = append(cfg.Peers, p.info())
	}
	o, err := New(cfg, log.New())
	require.NoError(n.t, err)
	require.NoError(n.t, o.Start())
	n.t.Cleanup(func() { require.NoError(n.t, o.Stop()) })
	return o
}

// publishHeads publishes a head signed by key 0 every interval until ctx
// ends; the node may have no peers yet.
func publishHeads(ctx context.Context, o *Overlay, every time.Duration) {
	epoch := nowMs()
	go func() {
		for counter := uint64(1); ctx.Err() == nil; counter++ {
			h := head(epoch, counter, byte(counter))
			sig, err := seqsig.Sign(h, testDomain, key(0))
			if err != nil {
				panic(err)
			}
			_ = o.Publish(ctx, seqsig.Envelope{Head: h, Signature: sig})
			select {
			case <-ctx.Done():
			case <-time.After(every):
			}
		}
	}()
}

// awaitCounter reads heads until one has a counter of at least min; every
// head must be signed by key 0.
func awaitCounter(t *testing.T, heads <-chan *seqsig.VerifiedHead, min uint64) {
	t.Helper()
	timeout := time.After(deadline)
	for {
		select {
		case h := <-heads:
			require.Equal(t, addr(0), h.Signer, "head not signed by key 0")
			if h.Head.Counter >= min {
				return
			}
		case <-timeout:
			t.Fatalf("no head with counter %d within %s", min, deadline)
		}
	}
}

// rawNode joins the topic with the overlay's gossip settings and no
// validator: a rogue publisher, or a probe of what a node relays.
type rawNode struct {
	host  host.Host
	topic *pubsub.Topic
	sub   *pubsub.Subscription
}

// opts nil selects the overlay's settings.
func newRawNode(t *testing.T, k byte, opts []pubsub.Option, peers ...*testNode) *rawNode {
	h, err := newHost(key(k), ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	if opts == nil {
		opts = gossipOptions()
	}
	ctx, cancel := context.WithCancel(context.Background())
	ps, err := pubsub.NewGossipSub(ctx, h, opts...)
	require.NoError(t, err)
	topic, err := ps.Join(Topic(testDomain.ChainID, testDomain.RollupID))
	require.NoError(t, err)
	sub, err := topic.Subscribe()
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		sub.Cancel()
		_ = topic.Close()
		_ = h.Close()
	})
	for _, p := range peers {
		require.NoError(t, h.Connect(t.Context(), p.info()))
	}
	return &rawNode{host: h, topic: topic, sub: sub}
}

// received returns the messages from peers until ctx ends.
func (r *rawNode) received(ctx context.Context) <-chan *pubsub.Message {
	out := make(chan *pubsub.Message, 4096)
	go func() {
		defer close(out)
		for {
			msg, err := r.sub.Next(ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom != r.host.ID() {
				out <- msg
			}
		}
	}()
	return out
}

func TestHeadsRelayThroughUndrainedNode(t *testing.T) {
	t.Parallel()
	a, b, c := newTestNode(t, 1), newTestNode(t, 2), newTestNode(t, 3)
	oa := a.start()
	// B's consumer never reads: its channel fills while it relays.
	ob := b.start(a)
	oc := c.start(b)
	publishHeads(t.Context(), oa, 50*time.Millisecond)
	awaitCounter(t, oc.Heads(), HeadsBuffer+20)
	// B received every head C did, more than its channel holds.
	require.Eventually(t, func() bool { return ob.dropped.Load() > 0 }, 10*time.Second, 100*time.Millisecond, "B dropped no head")
	require.Len(t, ob.Heads(), HeadsBuffer)
	require.Empty(t, oa.Heads(), "the publisher delivered its own heads")
}

func TestRogueFloodDoesNotStallPublisher(t *testing.T) {
	t.Parallel()
	a, b := newTestNode(t, 1), newTestNode(t, 2)
	oa := a.start()
	ob := b.start(a)
	probe := newRawNode(t, 4, nil, a)
	rogue := newRawNode(t, 3, nil, a)
	ctx := t.Context()
	relayed := probe.received(ctx)
	honest := rogue.received(ctx)
	publishHeads(ctx, oa, 200*time.Millisecond)

	// A relays to its mesh, which forms on its first heartbeat; before it, a
	// relay could not reach the probe.
	awaitCounter(t, ob.Heads(), 5)
	time.Sleep(6 * time.Second)

	// The flood: heads of a signer A does not authorize, heads whose epoch
	// is ahead of A's clock, and re-encodings of the heads A sends.
	var flood sync.WaitGroup
	floodCtx, stop := context.WithTimeout(ctx, deadline)
	defer stop()
	flood.Go(func() {
		epoch := nowMs()
		for i := uint64(1); floodCtx.Err() == nil; i++ {
			_ = rogue.topic.Publish(floodCtx, mustEncode(1, head(epoch, i, 0xee)))
			_ = rogue.topic.Publish(floodCtx, mustEncode(0, head(epoch+10*seqsig.MaxFutureMs, i, 0xef)))
			time.Sleep(2 * time.Millisecond)
		}
	})
	flood.Go(func() {
		for floodCtx.Err() == nil {
			select {
			case msg := <-honest:
				for _, run := range []int{1, 2, 7, 60} {
					_ = rogue.topic.Publish(floodCtx, reencode(msg.Data, run))
				}
			case <-floodCtx.Done():
			}
		}
	})
	before := latestCounter(ob.Heads())
	awaitCounter(t, ob.Heads(), before+5)
	require.NoError(t, floodCtx.Err(), "the flood ended before B received five more heads")
	stop()
	flood.Wait()
	require.Positive(t, oa.rejected.Load(), "A rejected nothing")

	// Everything A relayed is one of its own heads, as it encoded them.
	time.Sleep(time.Second)
	var seen int
	for drained := false; !drained; {
		select {
		case msg := <-relayed:
			seen++
			requireAnonymous(t, msg)
			id := sha256.Sum256(msg.Data)
			require.Equal(t, string(id[:]), msg.ID, "message id")
			e, err := seqsig.Decode(msg.Data)
			require.NoError(t, err)
			h, err := e.Verify(testDomain, addr(0))
			require.NoError(t, err, "A relayed a head it must reject")
			require.Less(t, h.Head.EpochMs, nowMs()+seqsig.MaxFutureMs, "A relayed a future head")
			require.True(t, bytes.Equal(encode(t, e), msg.Data), "A relayed a re-encoded head")
		default:
			drained = true
		}
	}
	require.Positive(t, seen, "the probe saw nothing")
}

// latestCounter drains heads and returns the highest counter in it.
func latestCounter(heads <-chan *seqsig.VerifiedHead) uint64 {
	var c uint64
	for {
		select {
		case h := <-heads:
			c = max(c, h.Head.Counter)
		default:
			return c
		}
	}
}

func TestLatePeerIsDialled(t *testing.T) {
	t.Parallel()
	a, b := newTestNode(t, 1), newTestNode(t, 2)
	ob := b.start(a)
	time.Sleep(2 * time.Second)
	publishHeads(t.Context(), a.start(), 200*time.Millisecond)
	awaitCounter(t, ob.Heads(), 5)
}

func TestStaticPeerRedialled(t *testing.T) {
	t.Parallel()
	a, b := newTestNode(t, 1), newTestNode(t, 2)
	ob := b.start(a)
	// Long enough for the dial backoff of repeated failures to pass a tick.
	time.Sleep(40 * time.Second)
	a.start()
	require.Eventually(t, func() bool { return ob.Peers() == 1 }, DialInterval+time.Second, 50*time.Millisecond,
		"B did not redial A on its next tick")
}

func TestIdleConnectionClosed(t *testing.T) {
	t.Parallel()
	a, b := newTestNode(t, 1), newTestNode(t, 2)
	oa, err := New(Config{Listen: a.listen(), Key: key(a.key), Domain: testDomain, Signer: addr(0)}, log.New())
	require.NoError(t, err)
	oa.idle = time.Second
	require.NoError(t, oa.Start())
	t.Cleanup(func() { require.NoError(t, oa.Stop()) })
	ob := b.start(a)
	// A peer without gossipsub.
	plain, err := newHost(key(3), ma.StringCast("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = plain.Close() })
	require.NoError(t, plain.Connect(t.Context(), a.info()))
	require.Eventually(t, func() bool { return ob.Peers() == 1 }, deadline, 50*time.Millisecond)

	require.Eventually(t, func() bool { return len(oa.host.Network().ConnsToPeer(plain.ID())) == 0 },
		DialInterval+2*time.Second, 50*time.Millisecond, "the connection without gossip stayed open")
	require.Equal(t, 1, ob.Peers(), "the gossip connection was closed")
	require.Equal(t, IdleTimeout, time.Minute)
}

func TestStopWhileReceiving(t *testing.T) {
	t.Parallel()
	a := newTestNode(t, 1)
	oa := a.start()
	// Back to back, so that Stop lands while a head is being handed over.
	publishHeads(t.Context(), oa, 0)
	for i := range byte(20) {
		b := newTestNode(t, 20+i)
		cfg := Config{Listen: b.listen(), Key: key(b.key), Domain: testDomain, Signer: addr(0), Peers: []peer.AddrInfo{a.info()}}
		ob, err := New(cfg, log.New())
		require.NoError(t, err)
		require.NoError(t, ob.Start())
		awaitCounter(t, ob.Heads(), 1)
		require.NoError(t, ob.Stop())
		for range ob.Heads() {
		}
	}
}

func TestPublishBeforeStart(t *testing.T) {
	o, err := New(Config{Listen: newTestNode(t, 1).listen(), Key: key(1), Domain: testDomain, Signer: addr(0)}, log.New())
	require.NoError(t, err)
	require.Error(t, o.Publish(t.Context(), signed(t, 0, head(1, 1, 1), nil)))
	require.NoError(t, o.Stop())
}

func TestStopTwice(t *testing.T) {
	o := newTestNode(t, 1).start()
	require.NoError(t, o.Stop())
	require.NoError(t, o.Stop())
}

// TestPeersDuringStart: the node's RPC may count peers while the overlay
// starts.
func TestPeersDuringStart(t *testing.T) {
	o, err := New(Config{Listen: newTestNode(t, 1).listen(), Key: key(1), Domain: testDomain, Signer: addr(0)}, log.New())
	require.NoError(t, err)
	done := make(chan struct{})
	counted := make(chan int)
	go func() {
		var peers int
		for {
			select {
			case <-done:
				counted <- peers
				return
			default:
				peers = max(peers, o.Peers())
			}
		}
	}()
	require.NoError(t, o.Start())
	close(done)
	require.Zero(t, <-counted)
	require.NoError(t, o.Stop())
}

// TestPublishDuringStart: a node may publish while the overlay starts.
func TestPublishDuringStart(t *testing.T) {
	o, err := New(Config{Listen: newTestNode(t, 1).listen(), Key: key(1), Domain: testDomain, Signer: addr(1)}, log.New())
	require.NoError(t, err)
	e := signed(t, 1, head(nowMs(), 1, 1), nil)
	done, published := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(published)
		for {
			select {
			case <-done:
				return
			default:
				_ = o.Publish(t.Context(), e)
			}
		}
	}()
	require.NoError(t, o.Start())
	close(done)
	<-published
	require.NoError(t, o.Stop())
}

func counters(names ...string) map[string]uint64 {
	out := make(map[string]uint64, len(names))
	for _, n := range names {
		out[n] = metrics.GetOrCreateCounter(n).GetValueUint64()
	}
	return out
}

// TestMetricsAndEvents: the overlay counts its peers' messages by verdict,
// not its own, and the heads it drops, and each report warns once of the
// rejections and drops since the previous one.
func TestMetricsAndEvents(t *testing.T) {
	var out bytes.Buffer
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(&out, log.LogfmtFormat())))
	o, err := New(Config{Listen: newTestNode(t, 1).listen(), Key: key(1), Domain: testDomain, Signer: addr(0)}, logger)
	require.NoError(t, err)
	from, err := PeerID(&key(2).PublicKey)
	require.NoError(t, err)
	names := []string{
		"eez_p2p_message_received_total", "eez_p2p_message_accepted_total", "eez_p2p_message_ignored_total",
		"eez_p2p_message_rejected_total", "eez_p2p_inbound_dropped_total",
	}
	before := counters(names...)
	msg := func(data []byte) *pubsub.Message { return &pubsub.Message{Message: &pubsubpb.Message{Data: data}} }
	first := mustEncode(0, head(nowMs(), 1, 1))
	require.Equal(t, pubsub.ValidationAccept, o.validate(t.Context(), from, msg(first)))
	require.Equal(t, pubsub.ValidationIgnore, o.validate(t.Context(), from, msg(first)))
	require.Equal(t, pubsub.ValidationReject, o.validate(t.Context(), from, msg([]byte("junk"))))
	require.Equal(t, pubsub.ValidationAccept, o.validate(t.Context(), o.ID(), msg(mustEncode(0, head(nowMs(), 2, 2)))))
	h := &seqsig.VerifiedHead{Head: head(nowMs(), 3, 3), Signer: addr(0)}
	for range HeadsBuffer + 2 {
		o.hand(h)
	}
	after := counters(names...)
	for name, want := range map[string]uint64{
		"eez_p2p_message_received_total": 3,
		"eez_p2p_message_accepted_total": 1,
		"eez_p2p_message_ignored_total":  1,
		"eez_p2p_message_rejected_total": 1,
		"eez_p2p_inbound_dropped_total":  2,
	} {
		require.Equal(t, want, after[name]-before[name], name)
	}

	var r reported
	o.report(&r)
	o.report(&r)
	logs := out.String()
	require.Equal(t, 1, strings.Count(logs, "event=eez.p2p.message.rejected "))
	require.Equal(t, 1, strings.Count(logs, "event=eez.p2p.message.rejects count=1 "), logs)
	require.Equal(t, 1, strings.Count(logs, "event=eez.p2p.inbound.full count=2"), logs)
}

// TestPeerGauge: eez_p2p_peers counts the connected overlay peers, none once
// the overlay stops.
func TestPeerGauge(t *testing.T) {
	gauge := metrics.GetOrCreateGauge("eez_p2p_peers")
	a, b := newTestNode(t, 1), newTestNode(t, 2)
	oa := a.start()
	ob := b.start(a)
	require.Eventually(t, func() bool { return ob.Peers() == 1 }, deadline, 50*time.Millisecond)
	require.Eventually(t, func() bool { return gauge.GetValueUint64() == 1 }, DialInterval+time.Second, 50*time.Millisecond)
	require.NoError(t, ob.Stop())
	require.NoError(t, oa.Stop())
	require.Zero(t, gauge.GetValueUint64())
}
