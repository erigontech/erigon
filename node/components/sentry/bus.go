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

package sentry

import (
	"context"
	"fmt"
	"sync"

	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/enode"
)

// busState holds the Provider's event-bus wiring state. It is a separate
// struct so the publish API can evolve without reshaping the public Provider.
//
// announcedPeers tracks peers for which a PeerManifestReceived has been
// published but no matching PeerDeparted has fired yet. UnbindBus drains
// it to keep the flow orchestrator's invariant (every observed peer
// eventually sees a PeerDeparted) intact across Provider teardown.
type busState struct {
	mu             sync.Mutex
	bus            event.EventBus
	announcedPeers map[string]struct{}
}

func (s *busState) bound() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bus != nil
}

// BindBus wires the Provider to the framework event bus so
// AnnouncePeerManifest / AnnouncePeerDeparted publish flow events.
//
// Peer-manifest announcements are not wired to live P2P discovery yet —
// that path depends on V2 ENR support which is not in place. Production
// callers are no-ops until a discovery adapter drives them; integration
// tests exercise the contract by calling AnnouncePeerManifest directly.
func (p *Provider) BindBus(bus event.EventBus) error {
	if p == nil {
		return fmt.Errorf("sentry.BindBus: nil provider")
	}
	if bus == nil {
		return fmt.Errorf("sentry.BindBus: nil bus")
	}

	p.busStateOnce.Do(func() { p.bus = &busState{} })
	p.bus.mu.Lock()
	defer p.bus.mu.Unlock()
	if p.bus.bus != nil {
		return fmt.Errorf("sentry.BindBus: already bound")
	}
	p.bus.bus = bus
	p.bus.announcedPeers = make(map[string]struct{})
	return nil
}

// UnbindBus clears the event-bus binding. Before clearing, it publishes
// PeerDeparted for every peer currently in the announced set so downstream
// subscribers can release per-peer state. Idempotent: a second call with no
// prior bind returns nil.
func (p *Provider) UnbindBus() error {
	if p == nil || p.bus == nil {
		return nil
	}
	p.bus.mu.Lock()
	bus := p.bus.bus
	peers := p.bus.announcedPeers
	p.bus.bus = nil
	p.bus.announcedPeers = nil
	p.bus.mu.Unlock()

	if bus == nil {
		return nil
	}
	for peerID := range peers {
		bus.Publish(flow.PeerDeparted{PeerID: peerID})
	}
	return nil
}

// AnnouncePeerManifest publishes flow.PeerManifestReceived on the bus and
// records the peer in the announced set. A no-op when not bound, so test
// setups and Close orderings that race against teardown are tolerated.
//
// Duplicate announcements for the same peer ID simply republish — the flow
// orchestrator treats the newer manifest as authoritative (supersedes the
// prior one), matching the contract documented on the event type.
func (p *Provider) AnnouncePeerManifest(peerID string, domains map[snapshot.Domain][]*snapshot.FileEntry, blocks []*snapshot.FileEntry) {
	if p == nil || p.bus == nil {
		return
	}
	p.bus.mu.Lock()
	bus := p.bus.bus
	if bus == nil {
		p.bus.mu.Unlock()
		return
	}
	p.bus.announcedPeers[peerID] = struct{}{}
	p.bus.mu.Unlock()

	bus.Publish(flow.PeerManifestReceived{
		PeerID:  peerID,
		Domains: domains,
		Blocks:  blocks,
	})
}

// AnnouncePeerDeparted publishes flow.PeerDeparted on the bus and removes
// the peer from the announced set so UnbindBus won't double-publish. A no-op
// when not bound, or when the peer was never announced.
func (p *Provider) AnnouncePeerDeparted(peerID string) {
	if p == nil || p.bus == nil {
		return
	}
	p.bus.mu.Lock()
	bus := p.bus.bus
	if bus == nil {
		p.bus.mu.Unlock()
		return
	}
	if _, ok := p.bus.announcedPeers[peerID]; !ok {
		p.bus.mu.Unlock()
		return
	}
	delete(p.bus.announcedPeers, peerID)
	p.bus.mu.Unlock()

	bus.Publish(flow.PeerDeparted{PeerID: peerID})
}

// PublishPeerConnected fires a sentry.PeerConnected event for the given
// peer. This is the production signal the manifest-exchange component
// subscribes to in order to trigger a V2 chain.toml fetch.
//
// No-op when not bound. No state is tracked per-peer at this layer —
// PublishPeerDisconnected is independent.
func (p *Provider) PublishPeerConnected(peer *enode.Node) {
	if p == nil || p.bus == nil || peer == nil {
		return
	}
	p.bus.mu.Lock()
	bus := p.bus.bus
	p.bus.mu.Unlock()
	if bus == nil {
		return
	}
	bus.Publish(PeerConnected{Peer: peer})
}

// PublishPeerDisconnected fires a sentry.PeerDisconnected event. No-op
// when not bound. The peerID should match what PeerConnected carried via
// Peer.ID().String() so subscribers can correlate.
func (p *Provider) PublishPeerDisconnected(peerID string) {
	if p == nil || p.bus == nil {
		return
	}
	p.bus.mu.Lock()
	bus := p.bus.bus
	p.bus.mu.Unlock()
	if bus == nil {
		return
	}
	bus.Publish(PeerDisconnected{PeerID: peerID})
}

// EnablePeerEventAutoWire subscribes to the ExecutionP2PMessageListener's
// peer-event stream and translates sentry protobuf peer events into
// PeerConnected / PeerDisconnected publications on the framework event
// bus. This is how real DevP2P handshakes drive manifest_exchange in a
// live-network configuration.
//
// Requires Initialize (which builds ExecutionP2PMessageListener) and
// BindBus to have completed. Returns an UnregisterFunc the caller must
// invoke on teardown — typically via t.Cleanup in tests or as part of
// the component's Close path in production.
//
// External-sentry mode is not supported here because the peer-node
// lookup requires a local GrpcServer; in that mode Peer Connect fires
// but the enode.Node isn't available locally. This returns nil in that
// mode without error — the auto-wire is simply skipped.
func (p *Provider) EnablePeerEventAutoWire(ctx context.Context) (execp2p.UnregisterFunc, error) {
	if p == nil {
		return nil, fmt.Errorf("sentry.EnablePeerEventAutoWire: nil provider")
	}
	if p.ExecutionP2PMessageListener == nil {
		return nil, fmt.Errorf("sentry.EnablePeerEventAutoWire: MessageListener not initialised; call Initialize first")
	}
	if p.bus == nil || !p.bus.bound() {
		return nil, fmt.Errorf("sentry.EnablePeerEventAutoWire: bus not bound; call BindBus first")
	}
	if len(p.Servers) == 0 {
		// External-sentry mode: no local peer-node lookup. Skip silently.
		return func() {}, nil
	}

	observer := func(msg *sentryproto.PeerEvent) {
		peerID := execp2p.PeerIdFromH512(msg.PeerId)
		switch msg.EventId {
		case sentryproto.PeerEvent_Connect:
			node := p.peerNodeByID([64]byte(*peerID))
			if node == nil {
				return
			}
			p.PublishPeerConnected(node)
		case sentryproto.PeerEvent_Disconnect:
			p.PublishPeerDisconnected(peerID.String())
		}
	}

	unreg := p.ExecutionP2PMessageListener.RegisterPeerEventObserver(observer, execp2p.WithReplayConnected(ctx))
	return unreg, nil
}

// peerNodeByID asks each local GrpcServer in turn for the *enode.Node of
// the peer. Returns nil if the peer is not in any server's good-peers set
// (e.g. the connect event raced the auto-wire).
func (p *Provider) peerNodeByID(peerID [64]byte) *enode.Node {
	for _, s := range p.Servers {
		if n := s.PeerNode(peerID); n != nil {
			return n
		}
	}
	return nil
}
