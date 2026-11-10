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

// Package overlay gossips the heads the sequencer signs between EEZ nodes:
// a libp2p gossipsub network on one topic per rollup, where every node
// relays a head only after verifying it.
package overlay

import (
	"context"
	"crypto/ecdsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/x/rate"
	ma "github.com/multiformats/go-multiaddr"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/p2p"
)

const (
	// MaxMessageSize caps a gossip frame: the largest message and its
	// framing.
	MaxMessageSize = seqsig.MaxMessageSize + 64<<10
	// HeadsBuffer is the capacity of the channel of accepted heads.
	HeadsBuffer = 64
	// DialInterval is how often disconnected peers are dialled and rejected
	// messages summarised.
	DialInterval = 5 * time.Second
	// DiscoveryPeerLimit is the connection count below which discovered
	// peers are dialled; past the mesh's upper bound more peers add churn,
	// not reach. Static peers are always dialled.
	DiscoveryPeerLimit = 16
	// MaxInbound caps inbound connections, and so the peers a publisher
	// floods each head to.
	MaxInbound = 64
	// MaxConnsPerPeer keeps both connections a simultaneous dial can leave.
	MaxConnsPerPeer = 2
	// IdleTimeout is how long a connection may stay open without a gossip
	// stream.
	IdleTimeout = time.Minute
	// SeenTTL is how long a message id stays seen.
	SeenTTL = time.Minute
)

// gossipProtocols are the gossipsub versions the overlay offers.
var gossipProtocols = []protocol.ID{pubsub.GossipSubID_v12, pubsub.GossipSubID_v11, pubsub.GossipSubID_v10}

// Topic returns the gossip topic of the heads of rollup rollupID on the L2
// chain chainID.
func Topic(chainID, rollupID uint64) string {
	return fmt.Sprintf("/eez/%d/%d/heads/0", chainID, rollupID)
}

// Config is an overlay node's configuration.
type Config struct {
	// Listen is the TCP multiaddr the node listens on.
	Listen ma.Multiaddr
	// Peers are static peers, dialled whenever they are not connected.
	Peers []peer.AddrInfo
	// Key is the node's devp2p key, its identity on the overlay too.
	Key *ecdsa.PrivateKey
	// Domain and Signer select the heads the node accepts and relays: those
	// Signer signed for Domain's rollup.
	Domain seqsig.Domain
	Signer common.Address
	// Devp2p, when set, carries the overlay port in its node record and
	// supplies peers from its discv5 table.
	Devp2p *p2p.Server
}

// Overlay is a node of the signed-head overlay.
type Overlay struct {
	cfg    Config
	self   peer.ID
	port   uint16
	logger log.Logger
	m      *overlayMetrics
	val    *validator
	heads  chan *seqsig.VerifiedHead
	// idle is IdleTimeout, shorter in tests.
	idle time.Duration
	// discover returns the overlay addresses of discovered peers.
	discover func() []peer.AddrInfo

	host   host.Host
	topic  *pubsub.Topic
	sub    *pubsub.Subscription
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// mu also orders the writes of host and topic before Peers and Publish,
	// which the node may call while Start runs.
	mu      sync.Mutex
	dialing map[peer.ID]struct{}
	cursor  int

	rejected   atomic.Uint64
	lastReject atomic.Pointer[reject]
	dropped    atomic.Uint64
}

type reject struct {
	from peer.ID
	err  error
}

// New returns an overlay node of cfg; Start brings it up.
func New(cfg Config, logger log.Logger) (*Overlay, error) {
	if cfg.Listen == nil || cfg.Key == nil {
		return nil, errors.New("overlay: listen address and key are required")
	}
	self, err := PeerID(&cfg.Key.PublicKey)
	if err != nil {
		return nil, err
	}
	port, err := ListenPort(cfg.Listen)
	if err != nil {
		return nil, err
	}
	return &Overlay{
		cfg:     cfg,
		self:    self,
		port:    port,
		logger:  logger,
		m:       loadMetrics(),
		val:     &validator{domain: cfg.Domain, signer: cfg.Signer, now: nowMs},
		heads:   make(chan *seqsig.VerifiedHead, HeadsBuffer),
		idle:    IdleTimeout,
		dialing: make(map[peer.ID]struct{}),
	}, nil
}

// Heads delivers the heads the node accepts from its peers, verified. A head
// that finds the channel full is dropped: the overlay never waits for its
// consumer.
func (o *Overlay) Heads() <-chan *seqsig.VerifiedHead { return o.heads }

// Start listens, joins the topic and starts dialling peers.
func (o *Overlay) Start() error {
	h, err := newHost(o.cfg.Key, o.cfg.Listen)
	if err != nil {
		return fmt.Errorf("overlay: listen on %s: %w", o.cfg.Listen, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	topic, sub, err := join(ctx, h, Topic(o.cfg.Domain.ChainID, o.cfg.Domain.RollupID), o.validate)
	if err != nil {
		cancel()
		h.Close()
		return err
	}
	o.mu.Lock()
	o.host, o.topic = h, topic
	o.mu.Unlock()
	o.sub, o.cancel = sub, cancel
	if srv := o.cfg.Devp2p; srv != nil {
		srv.LocalNode().Set(Entry(o.port))
		o.discover = discoveryV5(srv)
	}
	o.wg.Go(func() { o.deliver(ctx) })
	o.wg.Go(func() { o.loop(ctx) })
	o.logger.Info("[eez] overlay listening", "event", "eez.p2p.listening", "addrs", h.Addrs(), "peer", h.ID(), "topic", topic.String())
	return nil
}

// Stop leaves the overlay and closes Heads.
func (o *Overlay) Stop() error {
	if o.cancel == nil {
		return nil
	}
	// Cancelling the context stops gossipsub with its topic and subscription.
	o.cancel()
	o.cancel = nil
	o.wg.Wait()
	o.m.peers.SetInt(0)
	return o.host.Close()
}

// Publish gossips e, a head the sequencer signed. The head passes the relay
// rule first: an error reports a head peers would not relay. It is safe to
// call concurrently with Start.
func (o *Overlay) Publish(ctx context.Context, e seqsig.Envelope) error {
	o.mu.Lock()
	topic := o.topic
	o.mu.Unlock()
	if topic == nil {
		return errors.New("overlay: not started")
	}
	data, err := seqsig.Encode(e)
	if err != nil {
		return err
	}
	return topic.Publish(ctx, data)
}

// ID returns the node's peer id.
func (o *Overlay) ID() peer.ID { return o.self }

// Peers returns the number of connected peers. It is safe to call
// concurrently with Start.
func (o *Overlay) Peers() int {
	o.mu.Lock()
	h := o.host
	o.mu.Unlock()
	if h == nil {
		return 0
	}
	return len(h.Network().Peers())
}

// validate is the topic validator: the relay rule, and the hand-over of an
// accepted head to deliver.
func (o *Overlay) validate(_ context.Context, from peer.ID, msg *pubsub.Message) pubsub.ValidationResult {
	h, res, err := o.val.check(msg.Data)
	if res == pubsub.ValidationAccept {
		msg.ValidatorData = h
	}
	if from == o.self {
		return res
	}
	o.m.received.Inc()
	switch res {
	case pubsub.ValidationAccept:
		o.m.accepted.Inc()
	case pubsub.ValidationIgnore:
		o.m.ignored.Inc()
	case pubsub.ValidationReject:
		o.m.rejected.Inc()
		o.rejected.Add(1)
		o.lastReject.Store(&reject{from: from, err: err})
		o.logger.Debug("[eez] overlay rejected a message", "event", "eez.p2p.message.rejected", "peer", from, "err", err)
	}
	return res
}

// deliver hands accepted heads from peers to Heads, dropping those that find
// it full, and closes Heads when ctx ends.
func (o *Overlay) deliver(ctx context.Context) {
	defer close(o.heads)
	for {
		msg, err := o.sub.Next(ctx)
		if err != nil {
			return
		}
		if msg.ReceivedFrom == o.self {
			continue
		}
		if h, ok := msg.ValidatorData.(*seqsig.VerifiedHead); ok {
			o.hand(h)
		}
	}
}

// hand passes h to Heads, dropping it when Heads is full.
func (o *Overlay) hand(h *seqsig.VerifiedHead) {
	select {
	case o.heads <- h:
	default:
		o.dropped.Add(1)
		o.m.dropped.Inc()
	}
}

func (o *Overlay) loop(ctx context.Context) {
	t := time.NewTicker(DialInterval)
	defer t.Stop()
	var r reported
	for {
		o.dialPeers(ctx)
		o.closeIdle(time.Now())
		o.report(&r)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// reported is what the previous report covered.
type reported struct{ rejected, dropped uint64 }

// report sets the peer gauge and warns of the messages rejected and heads
// dropped since the previous report, once each, so that a flood cannot
// flood the log.
func (o *Overlay) report(r *reported) {
	o.m.peers.SetInt(o.Peers())
	if last := o.lastReject.Swap(nil); last != nil {
		n := o.rejected.Load()
		o.logger.Warn("[eez] overlay rejected messages", "event", "eez.p2p.message.rejects", "count", n-r.rejected, "last", last.from, "err", last.err)
		r.rejected = n
	}
	if n := o.dropped.Load(); n != r.dropped {
		o.logger.Warn("[eez] overlay head channel full, heads dropped", "event", "eez.p2p.inbound.full", "count", n-r.dropped)
		r.dropped = n
	}
}

// dialPeers dials the static peers that are not connected, and the next
// discovered ones.
func (o *Overlay) dialPeers(ctx context.Context) {
	for _, p := range o.cfg.Peers {
		o.dial(ctx, p)
	}
	for _, p := range o.discovered() {
		o.dial(ctx, p)
	}
}

// discovered returns the discovered peers to dial: those not connected, as
// many as keep the node below DiscoveryPeerLimit.
func (o *Overlay) discovered() []peer.AddrInfo {
	budget := DiscoveryPeerLimit - len(o.host.Network().Peers())
	if o.discover == nil || budget <= 0 {
		return nil
	}
	var found []peer.AddrInfo
	for _, p := range o.discover() {
		if p.ID != o.self && o.host.Network().Connectedness(p.ID) != network.Connected {
			found = append(found, p)
		}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return nextBatch(found, &o.cursor, budget)
}

// nextBatch returns up to budget of items from cursor on, wrapping, and moves
// cursor past them, so that unreachable entries cannot take the whole budget
// on every tick.
func nextBatch[T any](items []T, cursor *int, budget int) []T {
	if len(items) == 0 {
		return nil
	}
	start := *cursor % len(items)
	batch := append(slices.Clone(items[start:]), items[:start]...)
	batch = batch[:min(budget, len(batch))]
	*cursor += len(batch)
	return batch
}

// dial connects to p in the background unless it is connected or a dial to
// it is under way.
func (o *Overlay) dial(ctx context.Context, p peer.AddrInfo) {
	if o.host.Network().Connectedness(p.ID) == network.Connected {
		return
	}
	o.mu.Lock()
	if _, ok := o.dialing[p.ID]; ok {
		o.mu.Unlock()
		return
	}
	o.dialing[p.ID] = struct{}{}
	o.mu.Unlock()
	o.wg.Go(func() {
		dctx, cancel := context.WithTimeout(ctx, DialInterval)
		defer cancel()
		// Forced: the swarm's dial backoff would otherwise skip ticks.
		dctx = network.WithForceDirectDial(dctx, "overlay peer")
		if err := o.host.Connect(dctx, p); err != nil && ctx.Err() == nil {
			o.logger.Debug("[eez] overlay dial failed", "event", "eez.p2p.dial.failed", "peer", p.ID, "err", err)
		}
		o.mu.Lock()
		delete(o.dialing, p.ID)
		o.mu.Unlock()
	})
}

// closeIdle closes the connections open for o.idle without a gossip stream.
func (o *Overlay) closeIdle(now time.Time) {
	for _, c := range o.host.Network().Conns() {
		if now.Sub(c.Stat().Opened) >= o.idle && !gossiping(c) {
			_ = c.Close()
		}
	}
}

func gossiping(c network.Conn) bool {
	for _, s := range c.GetStreams() {
		if slices.Contains(gossipProtocols, s.Protocol()) {
			return true
		}
	}
	return false
}

// newHost returns a libp2p host on TCP, Noise and Yamux alone, with key's
// identity, listening on listen.
func newHost(key *ecdsa.PrivateKey, listen ma.Multiaddr) (host.Host, error) {
	id, err := p2pcrypto.UnmarshalSecp256k1PrivateKey(crypto.FromECDSA(key))
	if err != nil {
		return nil, err
	}
	rm, err := newResourceManager()
	if err != nil {
		return nil, err
	}
	g := &gater{}
	h, err := libp2p.New(
		libp2p.Identity(id),
		libp2p.ListenAddrs(listen),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
		libp2p.ConnectionGater(g),
		libp2p.ResourceManager(rm),
		libp2p.DisableRelay(),
		libp2p.Ping(false),
		libp2p.DisableMetrics(),
		libp2p.UserAgent("erigon/eez"),
	)
	if err != nil {
		return nil, err
	}
	// The gater refuses every connection until setCounter, so no failed
	// identification precedes the subscription.
	oh, err := withUnidentified(h)
	if err != nil {
		h.Close()
		return nil, err
	}
	g.setCounter(counter(h.Network()))
	return oh, nil
}

// newResourceManager returns the default resource manager with its
// per-address connection count and burst raised to MaxInbound: followers
// behind one NAT share an address.
func newResourceManager() (network.ResourceManager, error) {
	limits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&limits)
	perAddress := rate.Limit{RPS: 0.2, Burst: MaxInbound}
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.AutoScale()),
		rcmgr.WithLimitPerSubnet(
			[]rcmgr.ConnLimitPerSubnet{{PrefixLength: 32, ConnCount: MaxInbound}},
			[]rcmgr.ConnLimitPerSubnet{{PrefixLength: 56, ConnCount: MaxInbound}},
		),
		rcmgr.WithConnRateLimiters(&rate.Limiter{
			NetworkPrefixLimits: []rate.PrefixLimit{
				{Prefix: netip.MustParsePrefix("127.0.0.0/8")},
				{Prefix: netip.MustParsePrefix("::1/128")},
			},
			SubnetRateLimiter: rate.SubnetLimiter{
				IPv4SubnetLimits: []rate.SubnetLimit{{PrefixLength: 32, Limit: perAddress}},
				IPv6SubnetLimits: []rate.SubnetLimit{
					{PrefixLength: 56, Limit: perAddress},
					{PrefixLength: 48, Limit: rate.Limit{RPS: 0.5, Burst: 2 * MaxInbound}},
				},
				GracePeriod: time.Minute,
			},
		}),
	)
}

// unidentifiedHost hands gossipsub the peers that run no identify protocol,
// which gossipsub would otherwise never open a stream to: it reports each
// failed identification as a peer adding the gossipsub protocol.
type unidentifiedHost struct {
	host.Host
	sub event.Subscription
	em  event.Emitter
}

func withUnidentified(h host.Host) (*unidentifiedHost, error) {
	sub, err := h.EventBus().Subscribe(new(event.EvtPeerIdentificationFailed))
	if err != nil {
		return nil, err
	}
	em, err := h.EventBus().Emitter(new(event.EvtPeerProtocolsUpdated))
	if err != nil {
		sub.Close()
		return nil, err
	}
	go func() {
		for ev := range sub.Out() {
			p := ev.(event.EvtPeerIdentificationFailed).Peer
			_ = em.Emit(event.EvtPeerProtocolsUpdated{Peer: p, Added: gossipProtocols[:1]})
		}
	}()
	return &unidentifiedHost{Host: h, sub: sub, em: em}, nil
}

func (h *unidentifiedHost) Close() error {
	h.sub.Close()
	h.em.Close()
	return h.Host.Close()
}

// join starts gossipsub on h and joins topic, with validate as its validator
// when set.
func join(ctx context.Context, h host.Host, topic string, validate pubsub.ValidatorEx) (*pubsub.Topic, *pubsub.Subscription, error) {
	ps, err := pubsub.NewGossipSub(ctx, h, gossipOptions()...)
	if err != nil {
		return nil, nil, err
	}
	if validate != nil {
		if err := ps.RegisterTopicValidator(topic, validate); err != nil {
			return nil, nil, err
		}
	}
	t, err := ps.Join(topic)
	if err != nil {
		return nil, nil, err
	}
	sub, err := t.Subscribe()
	if err != nil {
		t.Close()
		return nil, nil, err
	}
	return t, sub, nil
}

// gossipOptions are the overlay network's gossipsub settings: messages carry
// no author, since the sequencer's signature is their authentication.
func gossipOptions() []pubsub.Option {
	params := pubsub.DefaultGossipSubParams()
	params.HeartbeatInitialDelay = 5 * time.Second
	params.PrunePeers = 0
	params.IDontWantMessageThreshold = 1000
	return []pubsub.Option{
		pubsub.WithGossipSubProtocols(gossipProtocols, pubsub.GossipSubDefaultFeatures),
		pubsub.WithMessageSignaturePolicy(pubsub.StrictNoSign),
		pubsub.WithNoAuthor(),
		pubsub.WithMessageIdFn(msgID),
		pubsub.WithMaxMessageSize(MaxMessageSize),
		pubsub.WithGossipSubParams(params),
		// A publisher sends to every topic peer, not only to its mesh.
		pubsub.WithFloodPublish(true),
		pubsub.WithSeenMessagesTTL(SeenTTL),
	}
}

// msgID is the SHA-256 of the message data.
func msgID(m *pubsubpb.Message) string {
	h := sha256.Sum256(m.Data)
	return string(h[:])
}

// PeerID returns the overlay peer id of the node whose devp2p public key is
// pub.
func PeerID(pub *ecdsa.PublicKey) (peer.ID, error) {
	k, err := p2pcrypto.UnmarshalSecp256k1PublicKey(crypto.CompressPubkey(pub))
	if err != nil {
		return "", err
	}
	return peer.IDFromPublicKey(k)
}

func nowMs() uint64 { return uint64(time.Now().UnixMilli()) }
