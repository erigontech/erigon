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
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"fmt"
	"net"
	"os"

	dirutil "github.com/erigontech/erigon/common/dir"
	"path/filepath"
	"sync"
	"time"

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

	// trust gates which peers' manifests reach the orchestrator. Nil
	// means "trust everyone" — preserves the pre-UCAN behaviour. When
	// non-nil, fetchAndPublish runs the UCAN verification flow before
	// publishing PeerManifestReceived; failures are warn-logged and
	// the peer is blacklisted for trust.BlacklistDuration.
	trust      *TrustConfig
	trustState *trustState

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

	// nowFn is overridable for tests that want deterministic time
	// evaluation (blacklist expiry, UCAN time-window checks). Defaults
	// to time.Now.
	nowFn func() time.Time

	// canonicalValidator is invoked on every received peer manifest
	// before it's published to the orchestrator. Per the three-layer
	// model (docs/plans/20260515-three-layer-snapshot-distribution.md)
	// the consumer-side rule is "an advertisement entry survives iff
	// its (name, hash) matches at least one accepted canonical
	// version's entry for the same name." The callback (typically
	// wraps snapshotsync.ValidateAdvertisement) returns the filtered
	// subset; the Provider replaces the manifest's content with that
	// subset before publishing. Nil disables the check (default).
	canonicalValidator CanonicalValidatorFn

	// cacheDir, if set, is the directory where Provider writes each
	// validated peer manifest as chain.<peer_id>.toml on disk. Lets
	// peers persist for re-seeding and lets the node survive restarts
	// without re-fetching every peer's manifest from scratch.
	// Empty string disables the cache (default).
	cacheDir string
}

// CanonicalValidatorFn is invoked on each received peer manifest to
// filter its entries against the current canonical set. Returns the
// validated subset (entries whose (name, hash) appear in at least one
// canonical version). Production wires this to a closure calling
// snapshotsync.ValidateAdvertisement; tests/harness leave it nil.
type CanonicalValidatorFn func(adv *downloader.ChainTomlV2) *downloader.ChainTomlV2

// SetCanonicalValidator installs the consumer-side validation
// callback called on every received peer manifest. The callback
// returns the validated subset; non-matching entries are silently
// dropped before the manifest is published to the orchestrator.
// Pass nil to disable (default; not recommended in production).
func (p *Provider) SetCanonicalValidator(fn CanonicalValidatorFn) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.canonicalValidator = fn
}

// SetCacheDir configures the on-disk cache directory for validated
// peer manifests. Each peer's chain.toml is persisted as
// chain.<peer_id>.toml inside this directory. Empty string disables
// the cache. Call before BindBus or while unbound.
func (p *Provider) SetCacheDir(dir string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cacheDir = dir
}

// SetTrust configures the UCAN verification gate. Call before BindBus
// (or while unbound) — modifying trust mid-flight is not supported.
// Passing nil disables the gate (trust-everyone). When trust is
// non-nil, the same Fetcher must support FetchPeerUCAN — typically
// the production downloader.Provider satisfies both ManifestFetcher
// and UCANFetcher.
func (p *Provider) SetTrust(cfg *TrustConfig) error {
	if p == nil {
		return fmt.Errorf("manifest_exchange.SetTrust: nil provider")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hConnected != nil {
		return fmt.Errorf("manifest_exchange.SetTrust: cannot reconfigure while bound")
	}
	if cfg != nil {
		if cfg.Verifier == nil {
			return fmt.Errorf("manifest_exchange.SetTrust: TrustConfig.Verifier required")
		}
		if cfg.Fetcher == nil {
			return fmt.Errorf("manifest_exchange.SetTrust: TrustConfig.Fetcher required")
		}
		if len(cfg.RequiredCapabilities) == 0 {
			return fmt.Errorf("manifest_exchange.SetTrust: TrustConfig.RequiredCapabilities is empty (would reject every peer)")
		}
		p.trust = cfg
		p.trustState = newTrustState()
	} else {
		p.trust = nil
		p.trustState = nil
	}
	return nil
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
	if p.nowFn == nil {
		p.nowFn = time.Now
	}
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
//
// When a TrustConfig is set, peers currently on the blacklist are
// skipped here without spending a fetch. Trust-cached peers proceed
// through fetchAndPublish; the cache is consulted again there to
// short-circuit the UCAN re-verification path.
func (p *Provider) onPeerConnected(e sentry.PeerConnected) {
	if e.Peer == nil {
		return
	}
	peerID := e.Peer.ID().String()

	var ct enr.ChainToml
	if err := e.Peer.Record().Load(&ct); err != nil {
		if p.log != nil {
			p.log.Debug("[manifest_exchange] onPeerConnected: no chain-toml in ENR", "peer", peerID[:16], "err", err)
		}
		return
	}
	if ct.InfoHash == ([20]byte{}) {
		if p.log != nil {
			p.log.Debug("[manifest_exchange] onPeerConnected: zero info-hash", "peer", peerID[:16])
		}
		return
	}
	if p.log != nil {
		p.log.Info("[manifest_exchange] onPeerConnected: triggering fetch", "peer", peerID[:16], "infoHash", fmt.Sprintf("%x", ct.InfoHash[:8]))
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
	state := p.trustState
	now := p.nowFn
	p.mu.Unlock()

	if state != nil && now != nil && state.blacklisted(peerID, now()) {
		// Peer failed UCAN verification recently; skip until the
		// blacklist entry expires.
		return
	}

	p.mu.Lock()
	p.inflight[peerID] = struct{}{}
	ctx := p.ctx
	peerPub := e.Peer.Pubkey()
	p.fetchWG.Add(1)
	p.mu.Unlock()

	go func() {
		defer p.fetchWG.Done()
		p.fetchAndPublish(ctx, peerID, ct.InfoHash, peerIP, uint16(btPort), peerPub)
	}()
}

// onPeerDisconnected publishes flow.PeerDeparted so the orchestrator can
// release per-peer state.
//
// When TrustConfig.ReverifyOnReconnect is set, the peer's cached trust
// is also evicted so the next reconnect re-runs the UCAN check. This
// is the takeover-protection path — operators in adversarial
// environments accept the re-verification cost to defend against a
// compromised peer rotating keys between issuance and abuse.
func (p *Provider) onPeerDisconnected(e sentry.PeerDisconnected) {
	p.mu.Lock()
	bus := p.bus
	state := p.trustState
	cfg := p.trust
	p.mu.Unlock()
	if bus == nil {
		return
	}
	if state != nil && cfg != nil && cfg.ReverifyOnReconnect {
		state.forgetVerified(e.PeerID)
	}
	bus.Publish(flow.PeerDeparted{PeerID: e.PeerID})
}

// fetchAndPublish runs the full fetch → parse → (verify) → publish
// pipeline for a single peer. Runs in its own goroutine so multiple
// peer-connects can proceed in parallel without serialising on the
// bus handler.
//
// When TrustConfig is set, the pipeline gains a UCAN-verification
// step between parse and publish: fetch the chain.ucan.<seq>.bin
// sidecar pointed to by the V2 manifest's UCANHash field, run it
// through the Verifier against the peer's pubkey from ENR, and only
// publish PeerManifestReceived on success. Failures are warn-logged
// and the peer is added to the in-memory blacklist for
// trust.BlacklistDuration.
func (p *Provider) fetchAndPublish(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16, peerPub *ecdsa.PublicKey) {
	defer p.clearInflight(peerID)

	p.mu.Lock()
	fetcher := p.fetcher
	logger := p.log
	bus := p.bus
	cfg := p.trust
	state := p.trustState
	now := p.nowFn
	validator := p.canonicalValidator
	cacheDir := p.cacheDir
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

	// Trust gate. Skipped entirely when trust is unconfigured.
	if cfg != nil {
		if !p.gateOnUCAN(ctx, peerID, manifest, peerIP, peerPort, peerPub, cfg, state, now, logger) {
			return
		}
	}

	// Consumer-side canonical validation per the three-layer model
	// (docs/plans/20260515-three-layer-snapshot-distribution.md).
	// Drops entries whose (name, hash) doesn't match any accepted
	// canonical version. The peer's other valid entries pass through;
	// only mismatches are filtered.
	if validator != nil {
		manifest = validator(manifest)
		if manifest == nil {
			logger.Warn("[manifest_exchange] peer manifest fully filtered against canonical",
				"peer", peerID)
			return
		}
	}

	// Disk cache of validated peer manifests. Lets the node survive
	// restarts without re-fetching every peer's manifest, and lets us
	// re-seed peers' manifests so peer A going offline doesn't mean
	// peer A's content set disappears from the swarm.
	if cacheDir != "" {
		if err := writePeerManifestCache(cacheDir, peerID, data); err != nil {
			// Log but continue — caching is best-effort; the bus
			// publish is the load-bearing path.
			logger.Warn("[manifest_exchange] cache peer manifest",
				"peer", peerID, "err", err)
		}
	}

	bus.Publish(v2ToPeerManifest(peerID, manifest))
}

// writePeerManifestCache atomically writes a peer's chain.toml bytes
// to cacheDir/chain.<peer_id>.toml. Existing files are overwritten —
// peers' manifests can be refreshed on every reconnect, and the disk
// cache is best-effort recovery state, not authoritative storage.
//
// Atomic via write-temp-then-rename so a crash mid-write doesn't
// leave partial content visible.
func writePeerManifestCache(cacheDir, peerID string, data []byte) error {
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return fmt.Errorf("mkdir cache dir: %w", err)
	}
	final := filepath.Join(cacheDir, "chain."+peerID+".toml")
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = dirutil.RemoveFile(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// gateOnUCAN runs the manifest-side UCAN verification. Returns true iff
// the peer is trusted (cache hit OR fresh verify success), in which
// case fetchAndPublish should proceed with the orchestrator
// publication.
//
// On any failure path the peer is blacklisted for cfg.BlacklistDuration
// and the function returns false. Success caches the verified-until
// time so the next reconnect can short-circuit (unless
// ReverifyOnReconnect cleared the cache on disconnect).
func (p *Provider) gateOnUCAN(
	ctx context.Context,
	peerID string,
	manifest *downloader.ChainTomlV2,
	peerIP net.IP,
	peerPort uint16,
	peerPub *ecdsa.PublicKey,
	cfg *TrustConfig,
	state *trustState,
	nowFn func() time.Time,
	logger log.Logger,
) bool {
	if state.trusted(peerID, nowFn()) {
		// Cache hit — UCAN previously verified, still in its
		// validity window. Skip the re-fetch.
		return true
	}

	if peerPub == nil {
		logger.Warn("[manifest_exchange] UCAN gate: peer has no pubkey",
			"peer", peerID)
		state.markBlacklisted(peerID, nowFn().Add(cfg.blacklistDuration()))
		return false
	}

	if manifest.UCANHash == "" {
		logger.Warn("[manifest_exchange] UCAN gate: peer manifest has no UCAN hash",
			"peer", peerID)
		state.markBlacklisted(peerID, nowFn().Add(cfg.blacklistDuration()))
		return false
	}
	ucanHashBytes, err := hex.DecodeString(manifest.UCANHash)
	if err != nil || len(ucanHashBytes) != 20 {
		logger.Warn("[manifest_exchange] UCAN gate: malformed UCANHash",
			"peer", peerID, "ucan_hash", manifest.UCANHash, "err", err)
		state.markBlacklisted(peerID, nowFn().Add(cfg.blacklistDuration()))
		return false
	}
	var ucanHash [20]byte
	copy(ucanHash[:], ucanHashBytes)

	ucanData, err := cfg.Fetcher.FetchPeerUCAN(ctx, peerID, ucanHash, peerIP, peerPort)
	if err != nil {
		logger.Warn("[manifest_exchange] UCAN gate: fetch sidecar",
			"peer", peerID, "err", err)
		state.markBlacklisted(peerID, nowFn().Add(cfg.blacklistDuration()))
		return false
	}

	audience := compressedFromECDSA(peerPub)
	res, err := cfg.Verifier.Verify(ucanData, audience, cfg.RequiredCapabilities, nowFn())
	if err != nil {
		logger.Warn("[manifest_exchange] UCAN gate: verify",
			"peer", peerID, "err", err)
		state.markBlacklisted(peerID, nowFn().Add(cfg.blacklistDuration()))
		return false
	}

	expiresAt := time.Time{}
	if res.Leaf.Expires != 0 {
		expiresAt = time.Unix(res.Leaf.Expires, 0)
	}
	state.markVerified(peerID, expiresAt)
	logger.Debug("[manifest_exchange] UCAN gate: peer trusted",
		"peer", peerID, "root", res.MatchedRoot.Kind.String())
	return true
}

// compressedFromECDSA returns the 33-byte compressed encoding of an
// ECDSA pubkey. Mirrors the helper in snapshotauth without re-exporting
// it (small enough that duplication beats a public dependency for a
// single internal call site).
func compressedFromECDSA(pub *ecdsa.PublicKey) []byte {
	if pub == nil || pub.X == nil || pub.Y == nil {
		return nil
	}
	return elliptic.MarshalCompressed(pub.Curve, pub.X, pub.Y)
}

func (p *Provider) clearInflight(peerID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inflight == nil {
		return
	}
	delete(p.inflight, peerID)
}
