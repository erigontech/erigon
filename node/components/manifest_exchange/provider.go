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

// Package manifest_exchange is the consumer side of chain.toml v2: on
// every peer-connect event from sentry it reads the peer's chain-toml
// ENR entry, fetches the peer's V2 manifest via the downloader
// (BitTorrent), parses it, and publishes flow.PeerManifestReceived so the
// flow orchestrator can compute gap-fill downloads.
//
// Nodes wired to a real P2P network drive manifest exchange through this
// component. The sentry.AnnouncePeerManifest direct-publish path remains
// available for harness tests that don't exercise the full lifecycle.
package manifest_exchange

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/p2p/enr"
)

// ManifestFetcher abstracts the "fetch a peer's chain.toml.v2 by
// infohash" operation so manifest_exchange doesn't depend on a concrete
// torrent client. Production wires the downloader.Provider's
// FetchPeerManifestV2 method; tests wire a canned-bytes stub.
type ManifestFetcher interface {
	FetchPeerManifestV2(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) ([]byte, error)
}

// Provider is the manifest_exchange component's runtime state. It owns a
// bus binding and a ManifestFetcher for fetching peer chain.toml.v2
// manifests.
type Provider struct {
	mu        sync.Mutex
	bus       event.EventBus
	fetcher   ManifestFetcher
	ctx       context.Context
	cancelCtx context.CancelFunc
	log       log.Logger

	// Materialised handlers — stable reflect.Value.Pointer() for
	// Subscribe/Unsubscribe, same pattern used in downloader/bus.go.
	hConnected    func(sentry.PeerConnected)
	hDisconnected func(sentry.PeerDisconnected)

	// inflight tracks peer IDs whose manifest fetch is still in progress
	// so concurrent PeerConnected events for the same peer don't stack.
	inflight map[string]struct{}

	// fetchWG tracks active fetchAndPublish goroutines so UnbindBus can
	// cancel them via ctx and then wait for them to return. Without this,
	// a slow or hung peer download would outlive the component.
	fetchWG sync.WaitGroup
}

// BindBus wires the component to the framework event bus. After this
// call, peer-connect events trigger manifest fetches via the
// ManifestFetcher and peer-disconnect events publish flow.PeerDeparted.
//
// ctx is the component's lifetime context — used as the context for
// underlying fetch calls.
func (p *Provider) BindBus(ctx context.Context, bus event.EventBus, fetcher ManifestFetcher, logger log.Logger) error {
	if p == nil {
		return fmt.Errorf("manifest_exchange.BindBus: nil provider")
	}
	if bus == nil {
		return fmt.Errorf("manifest_exchange.BindBus: nil bus")
	}
	if fetcher == nil {
		return fmt.Errorf("manifest_exchange.BindBus: nil manifest fetcher")
	}
	if logger == nil {
		logger = log.Root()
	}

	p.mu.Lock()
	if p.hConnected != nil {
		p.mu.Unlock()
		return fmt.Errorf("manifest_exchange.BindBus: already bound")
	}
	// Derive a child context so UnbindBus can cancel in-flight fetches
	// without disturbing the caller's ctx.
	childCtx, cancel := context.WithCancel(ctx)
	p.bus = bus
	p.fetcher = fetcher
	p.ctx = childCtx
	p.cancelCtx = cancel
	p.log = logger
	p.inflight = make(map[string]struct{})
	p.hConnected = p.onPeerConnected
	p.hDisconnected = p.onPeerDisconnected
	p.mu.Unlock()

	if err := bus.Subscribe(p.hConnected); err != nil {
		p.unbindNoLock()
		return fmt.Errorf("subscribe sentry.PeerConnected: %w", err)
	}
	if err := bus.Subscribe(p.hDisconnected); err != nil {
		_ = bus.Unsubscribe(p.hConnected)
		p.unbindNoLock()
		return fmt.Errorf("subscribe sentry.PeerDisconnected: %w", err)
	}
	return nil
}

// UnbindBus removes subscriptions installed by BindBus. Cancels the
// component's context to unblock in-flight fetches, waits for them, then
// unsubscribes. Idempotent.
func (p *Provider) UnbindBus() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	bus := p.bus
	hC := p.hConnected
	hD := p.hDisconnected
	cancel := p.cancelCtx
	p.mu.Unlock()
	if bus == nil || hC == nil {
		return nil
	}

	// Cancel in-flight Download calls and drain the bus handlers that may
	// still be launching fetch goroutines.
	if cancel != nil {
		cancel()
	}
	bus.WaitAsync()
	p.fetchWG.Wait()

	var firstErr error
	if err := bus.Unsubscribe(hC); err != nil {
		firstErr = err
	}
	if err := bus.Unsubscribe(hD); err != nil && firstErr == nil {
		firstErr = err
	}
	p.unbindNoLock()
	return firstErr
}

func (p *Provider) unbindNoLock() {
	p.mu.Lock()
	p.bus = nil
	p.fetcher = nil
	p.ctx = nil
	p.cancelCtx = nil
	p.hConnected = nil
	p.hDisconnected = nil
	p.inflight = nil
	p.mu.Unlock()
}

// onPeerConnected reads the peer's chain-toml ENR entry and kicks off a
// manifest fetch in a goroutine. If the peer has no ENR entry or the
// advertised InfoHash is zero, the event is silently dropped — the peer
// doesn't advertise a V2 manifest.
func (p *Provider) onPeerConnected(e sentry.PeerConnected) {
	if e.Peer == nil {
		return
	}
	peerID := e.Peer.ID().String()

	var ct enr.ChainToml
	if err := e.Peer.Record().Load(&ct); err != nil {
		// Peer doesn't advertise chain-toml. Common during rollout.
		return
	}
	if ct.InfoHash == ([20]byte{}) {
		return
	}

	// Extract BT endpoint from the ENR so the fetcher can add the peer
	// as a direct torrent peer. Falls back to zero if absent — the
	// fetcher will rely on static peers or discovery in that case.
	var btPort enr.BT
	_ = e.Peer.Record().Load(&btPort)
	peerIP := e.Peer.IP()

	p.mu.Lock()
	if p.inflight == nil {
		p.mu.Unlock()
		return
	}
	if _, exists := p.inflight[peerID]; exists {
		p.mu.Unlock()
		return
	}
	p.inflight[peerID] = struct{}{}
	ctx := p.ctx
	p.fetchWG.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.fetchWG.Done()
		p.fetchAndPublish(ctx, peerID, ct.InfoHash, peerIP, uint16(btPort))
	}()
}

// onPeerDisconnected publishes flow.PeerDeparted so the orchestrator can
// release per-peer state.
func (p *Provider) onPeerDisconnected(e sentry.PeerDisconnected) {
	p.mu.Lock()
	bus := p.bus
	p.mu.Unlock()
	if bus == nil {
		return
	}
	bus.Publish(flow.PeerDeparted{PeerID: e.PeerID})
}

// fetchAndPublish runs the full fetch → parse → publish pipeline for a
// single peer. Runs in its own goroutine so multiple peer-connects can
// proceed in parallel without serialising on the bus handler.
func (p *Provider) fetchAndPublish(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) {
	defer p.clearInflight(peerID)

	p.mu.Lock()
	fetcher := p.fetcher
	logger := p.log
	bus := p.bus
	p.mu.Unlock()
	if fetcher == nil || bus == nil {
		return
	}

	data, err := fetcher.FetchPeerManifestV2(ctx, peerID, infoHash, peerIP, peerPort)
	if err != nil {
		logger.Warn("[manifest_exchange] fetch peer manifest", "peer", peerID, "err", err)
		return
	}

	manifest, err := downloader.ParseV2(data)
	if err != nil {
		logger.Warn("[manifest_exchange] parse peer manifest", "peer", peerID, "err", err)
		return
	}

	bus.Publish(v2ToPeerManifest(peerID, manifest))
}

func (p *Provider) clearInflight(peerID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inflight == nil {
		return
	}
	delete(p.inflight, peerID)
}
