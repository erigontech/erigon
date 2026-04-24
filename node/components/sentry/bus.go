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
	"fmt"
	"sync"

	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
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
