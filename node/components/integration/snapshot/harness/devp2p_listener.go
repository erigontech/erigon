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

package harness

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	eventpkg "github.com/erigontech/erigon/p2p/event"
)

// DevP2PListener runs a real p2p.Server with a minimal no-op protocol and
// forwards its peer-connect / peer-disconnect events to a
// sentry.Provider via PublishPeerConnected / PublishPeerDisconnected.
//
// Purpose: exercise real DevP2P handshakes in integration tests without
// pulling in the full sentry.Provider.Initialize path (which needs
// ChainDB, BlockReader, chain config, genesis commit). The event
// contract downstream is identical to what a production sentry stack
// would produce — a real TCP handshake, a real *enode.Node carrying a
// real ENR.
type DevP2PListener struct {
	server  *p2p.Server
	sentry  *sentry.Provider
	log     log.Logger
	evtCh   chan *p2p.PeerEvent
	evtSub  eventpkg.Subscription
	peersMu sync.Mutex
	peers   map[enode.ID]*enode.Node

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// devP2PNoopProtocol is the minimal subprotocol we register so
// p2p.Server completes a handshake with connecting peers. Run blocks
// until the peer drops (ctx cancellation) — it never sends or reads
// messages, it just keeps the negotiated protocol alive.
const (
	devP2PProtocolName    = "sfp2p" // snapshot-flow-p2p (private test protocol)
	devP2PProtocolVersion = 1
)

func devP2PNoopProtocol() p2p.Protocol {
	return p2p.Protocol{
		Name:    devP2PProtocolName,
		Version: devP2PProtocolVersion,
		Length:  1,
		// Block on any incoming message. Since we never send any and
		// the peer drops via p2p.Server.removePeer on disconnect, this
		// returns once the peer disappears.
		Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) *p2p.PeerError {
			for {
				_, err := rw.ReadMsg()
				if err != nil {
					return p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscSubprotocolError, err, "sfp2p noop")
				}
			}
		},
	}
}

// StartDevP2PListener constructs, starts, and returns a DevP2PListener.
// Forwards peer-add events to sentry.PublishPeerConnected and peer-drop
// to sentry.PublishPeerDisconnected. Caller invokes Close to stop.
func StartDevP2PListener(t *testing.T, ctx context.Context, sp *sentry.Provider, logger log.Logger) *DevP2PListener {
	t.Helper()

	key, err := crypto.GenerateKey()
	require.NoError(t, err)

	srv := &p2p.Server{
		Config: p2p.Config{
			PrivateKey:      key,
			MaxPeers:        10,
			MaxPendingPeers: 5,
			NoDiscovery:     true,
			ListenAddr:      "127.0.0.1:0",
			Protocols:       []p2p.Protocol{devP2PNoopProtocol()},
			Name:            "snapshot-flow-test-node",
			NodeDatabase:    t.TempDir(),
			DiscoveryV4:     false,
			DiscoveryV5:     false,
			ProtocolVersion: []uint{devP2PProtocolVersion},
		},
	}

	listenerCtx, cancel := context.WithCancel(ctx)
	require.NoError(t, srv.Start(listenerCtx, logger))

	l := &DevP2PListener{
		server: srv,
		sentry: sp,
		log:    logger,
		evtCh:  make(chan *p2p.PeerEvent, 64),
		peers:  make(map[enode.ID]*enode.Node),
		ctx:    listenerCtx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	l.evtSub = srv.SubscribeEvents(l.evtCh)

	go l.dispatch()
	return l
}

// Self returns the local enode.Node — use this as the argument to
// AddStaticPeer on another listener so peers can find each other.
func (l *DevP2PListener) Self() *enode.Node {
	return l.server.Self()
}

// SetENREntry installs a custom entry on the server's LocalNode. Takes
// effect immediately — the node re-signs its ENR on each mutation and
// advertises the new version to peers via the handshake. This is the
// only way to publish an ENR key like enr.ChainToml from a live
// listener; Self().Record() returns a snapshot, not the live record.
func (l *DevP2PListener) SetENREntry(entry enr.Entry) {
	l.server.LocalNode().Set(entry)
}

// AddStaticPeer adds node to the local server's static peer set. The
// server will dial it and maintain the connection.
func (l *DevP2PListener) AddStaticPeer(node *enode.Node) {
	l.server.AddPeer(node)
}

// Close stops the listener. Unsubscribes the peer-event subscription,
// stops the server, and waits for the dispatch goroutine to exit.
func (l *DevP2PListener) Close() {
	l.cancel()
	l.evtSub.Unsubscribe()
	l.server.Stop()
	<-l.done
}

// dispatch translates p2p.PeerEvent streams into sentry.PeerConnected /
// PeerDisconnected publications on the framework bus.
//
// p2p.PeerEvent carries only the peer's ID, not the full *enode.Node.
// We resolve via srv.PeersInfo() + srv.Peers() — the server's internal
// peer set — at the moment of the event. Since the Peer hasn't yet been
// torn down by the time Drop fires, this is stable.
func (l *DevP2PListener) dispatch() {
	defer close(l.done)
	for {
		select {
		case <-l.ctx.Done():
			return
		case err := <-l.evtSub.Err():
			if err != nil {
				l.log.Warn("[devp2p-listener] subscription error", "err", err)
				return
			}
		case evt := <-l.evtCh:
			if evt == nil {
				continue
			}
			switch evt.Type {
			case p2p.PeerEventTypeAdd:
				node := l.resolvePeer(evt.Peer)
				if node == nil {
					// Race: add event arrived before the peer is in
					// server.Peers(). The next add for the same peer
					// (from the other side of the connection) will
					// resolve; drop silently here.
					continue
				}
				l.peersMu.Lock()
				l.peers[node.ID()] = node
				l.peersMu.Unlock()
				l.sentry.PublishPeerConnected(node)
			case p2p.PeerEventTypeDrop:
				l.sentry.PublishPeerDisconnected(evt.Peer.TerminalString())
				l.peersMu.Lock()
				delete(l.peers, evt.Peer)
				l.peersMu.Unlock()
			}
		}
	}
}

// resolvePeer returns the full *enode.Node for a peer ID by scanning
// the server's live peer set. Returns nil if the peer is not present
// (e.g. the event arrived after the peer was torn down).
func (l *DevP2PListener) resolvePeer(id enode.ID) *enode.Node {
	for _, p := range l.server.Peers() {
		if p.ID() == id {
			return p.Node()
		}
	}
	return nil
}
