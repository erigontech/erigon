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

package manifest_exchange

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/app/event"
	"github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// mockFetcher is a ManifestFetcher implementation for unit tests. It
// returns bytes from a pre-registered (infohash → file) map, simulating
// a successful peer fetch without touching a real torrent client.
type mockFetcher struct {
	mu      sync.Mutex
	sources map[[20]byte]string
	err     error
	calls   int
}

func newMockFetcher() *mockFetcher {
	return &mockFetcher{sources: make(map[[20]byte]string)}
}

func (m *mockFetcher) register(hash [20]byte, srcPath string) {
	m.mu.Lock()
	m.sources[hash] = srcPath
	m.mu.Unlock()
}

func (m *mockFetcher) setError(err error) {
	m.mu.Lock()
	m.err = err
	m.mu.Unlock()
}

func (m *mockFetcher) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *mockFetcher) FetchPeerManifestV2(_ context.Context, _ string, infoHash [20]byte, _ net.IP, _ uint16) ([]byte, error) {
	m.mu.Lock()
	m.calls++
	err := m.err
	src, ok := m.sources[infoHash]
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("hash %x not registered with mockFetcher", infoHash)
	}
	return os.ReadFile(src)
}

var _ ManifestFetcher = (*mockFetcher)(nil)

func newTestBus() event.EventBus { return event.NewEventBus(nil) }

// makePeerNode builds an enode.Node whose ENR carries the given
// chain-toml entry. If ct is nil, no chain-toml entry is added.
func makePeerNode(t *testing.T, ct *enr.ChainToml) *enode.Node {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	var r enr.Record
	if ct != nil {
		r.Set(*ct)
	}
	require.NoError(t, enode.SignV4(&r, key))
	n, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)
	return n
}

// seedPeerManifest publishes a V2 manifest into seedDir using P.1's
// PublishChainTomlV2, then returns the on-disk chain.toml.v2 path and
// the computed V2 infohash. The caller registers the (hash → path)
// mapping with the mockCopyClient.
func seedPeerManifest(t *testing.T, seedDir string, inv *snapshot.Inventory) (string, [20]byte) {
	t.Helper()
	torrentFS := downloader.NewAtomicTorrentFS(seedDir)
	const enrFP = "a1b2c3d4e5f60718"
	hash, err := downloader.PublishChainTomlV2(seedDir, torrentFS, inv, 0, enrFP, nil)
	require.NoError(t, err)
	return filepath.Join(seedDir, downloader.ChainTomlV2FileName(enrFP, 0)), [20]byte(hash)
}

func makeInventory(t *testing.T) *snapshot.Inventory {
	t.Helper()
	inv := snapshot.NewInventory()
	inv.AddFile(&snapshot.FileEntry{
		Domain:      snapshot.DomainAccounts,
		FromStep:    0,
		ToStep:      2048,
		Name:        "v1.0-accounts.0-2048.kv",
		TorrentHash: [20]byte{0x11, 0x22, 0x33, 0x44},
		Local:       true,
		Trust:       snapshot.TrustVerified,
	})
	inv.AddFile(&snapshot.FileEntry{
		Name:        "v1.0-000000-000500-headers.seg",
		TorrentHash: [20]byte{0xaa, 0xbb, 0xcc, 0xdd},
		Local:       true,
		Trust:       snapshot.TrustVerified,
	})
	return inv
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", msg)
}

type env struct {
	bus      event.EventBus
	p        *Provider
	sentry   *sentry.Provider
	fetcher  *mockFetcher
	received []flow.PeerManifestReceived
	departed []flow.PeerDeparted
	recvMu   sync.Mutex
	departMu sync.Mutex
}

func (e *env) receivedCount() int {
	e.recvMu.Lock()
	defer e.recvMu.Unlock()
	return len(e.received)
}

func (e *env) departedCount() int {
	e.departMu.Lock()
	defer e.departMu.Unlock()
	return len(e.departed)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	bus := newTestBus()
	fetcher := newMockFetcher()

	sp := &sentry.Provider{}
	require.NoError(t, sp.BindBus(bus))

	e := &env{
		bus:     bus,
		p:       &Provider{},
		sentry:  sp,
		fetcher: fetcher,
	}
	require.NoError(t, e.p.BindBus(context.Background(), bus, fetcher, nil))

	require.NoError(t, bus.Subscribe(func(evt flow.PeerManifestReceived) {
		e.recvMu.Lock()
		e.received = append(e.received, evt)
		e.recvMu.Unlock()
	}))
	require.NoError(t, bus.Subscribe(func(evt flow.PeerDeparted) {
		e.departMu.Lock()
		e.departed = append(e.departed, evt)
		e.departMu.Unlock()
	}))

	t.Cleanup(func() {
		_ = e.p.UnbindBus()
		_ = sp.UnbindBus()
		bus.WaitAsync()
	})
	return e
}

func TestOnPeerConnectedFetchesAndPublishes(t *testing.T) {
	e := newEnv(t)

	seedDir := t.TempDir()
	inv := makeInventory(t)
	seedPath, hash := seedPeerManifest(t, seedDir, inv)
	e.fetcher.register(hash, seedPath)

	peer := makePeerNode(t, &enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            hash,
	})
	e.sentry.PublishPeerConnected(peer)

	waitFor(t, func() bool { return e.receivedCount() == 1 },
		2*time.Second, "PeerManifestReceived publish")

	e.recvMu.Lock()
	defer e.recvMu.Unlock()
	got := e.received[0]
	require.Equal(t, peer.ID().String(), got.PeerID)
	require.Contains(t, got.Domains, snapshot.DomainAccounts)
	require.Len(t, got.Domains[snapshot.DomainAccounts], 1)
	accEntry := got.Domains[snapshot.DomainAccounts][0]
	require.Equal(t, "v1.0-accounts.0-2048.kv", accEntry.Name)
	require.Equal(t, uint64(0), accEntry.FromStep)
	require.Equal(t, uint64(2048), accEntry.ToStep)
	require.Equal(t, snapshot.TrustVerified, accEntry.Trust)

	require.Len(t, got.Blocks, 1)
	require.Equal(t, "v1.0-000000-000500-headers.seg", got.Blocks[0].Name)

	require.Equal(t, 1, e.fetcher.callCount())
}

func TestOnPeerConnectedNoENRIsNoop(t *testing.T) {
	e := newEnv(t)

	peer := makePeerNode(t, nil) // no chain-toml entry
	e.sentry.PublishPeerConnected(peer)

	e.bus.WaitAsync()
	time.Sleep(20 * time.Millisecond)

	require.Zero(t, e.receivedCount())
	require.Zero(t, e.fetcher.callCount(), "no download should fire for peer without chain-toml ENR")
}

func TestOnPeerConnectedZeroInfoHashIsNoop(t *testing.T) {
	e := newEnv(t)

	peer := makePeerNode(t, &enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		// InfoHash intentionally zero
	})
	e.sentry.PublishPeerConnected(peer)

	e.bus.WaitAsync()
	time.Sleep(20 * time.Millisecond)

	require.Zero(t, e.receivedCount())
	require.Zero(t, e.fetcher.callCount())
}

func TestOnPeerConnectedDownloadFailure(t *testing.T) {
	e := newEnv(t)

	e.fetcher.setError(errors.New("simulated transport failure"))

	// Register a hash so the peer advertises a non-zero InfoHash.
	hash := [20]byte{0x99, 0x88, 0x77}
	peer := makePeerNode(t, &enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            hash,
	})
	e.sentry.PublishPeerConnected(peer)

	// Allow the goroutine to fire.
	waitFor(t, func() bool { return e.fetcher.callCount() == 1 },
		2*time.Second, "download attempt")

	e.bus.WaitAsync()
	time.Sleep(20 * time.Millisecond)

	require.Zero(t, e.receivedCount(), "no PeerManifestReceived on download failure")
}

func TestOnPeerDisconnectedPublishesPeerDeparted(t *testing.T) {
	e := newEnv(t)

	e.sentry.PublishPeerDisconnected("peer-X")

	waitFor(t, func() bool { return e.departedCount() == 1 },
		2*time.Second, "PeerDeparted publish")

	e.departMu.Lock()
	defer e.departMu.Unlock()
	require.Equal(t, "peer-X", e.departed[0].PeerID)
}

func TestBindBusRejectsInvalidInputs(t *testing.T) {
	p := &Provider{}
	bus := newTestBus()
	fetcher := newMockFetcher()

	require.Error(t, p.BindBus(context.Background(), nil, fetcher, nil))
	require.Error(t, p.BindBus(context.Background(), bus, nil, nil))
}

func TestBindBusDoubleBind(t *testing.T) {
	p := &Provider{}
	bus := newTestBus()
	fetcher := newMockFetcher()

	require.NoError(t, p.BindBus(context.Background(), bus, fetcher, nil))
	err := p.BindBus(context.Background(), bus, fetcher, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already bound")
}

func TestUnbindBusIdempotent(t *testing.T) {
	p := &Provider{}
	require.NoError(t, p.UnbindBus())

	bus := newTestBus()
	fetcher := newMockFetcher()
	require.NoError(t, p.BindBus(context.Background(), bus, fetcher, nil))
	require.NoError(t, p.UnbindBus())
	require.NoError(t, p.UnbindBus())
}

func TestConcurrentPeerConnectedDoesntDoubleFetch(t *testing.T) {
	e := newEnv(t)

	seedDir := t.TempDir()
	inv := makeInventory(t)
	seedPath, hash := seedPeerManifest(t, seedDir, inv)
	e.fetcher.register(hash, seedPath)

	peer := makePeerNode(t, &enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            hash,
	})

	// Fire two PeerConnected events for the same peer back to back.
	e.sentry.PublishPeerConnected(peer)
	e.sentry.PublishPeerConnected(peer)

	waitFor(t, func() bool { return e.receivedCount() >= 1 },
		2*time.Second, "at least one PeerManifestReceived")

	// Allow any second fetch to complete; it should have been suppressed.
	e.bus.WaitAsync()
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, 1, e.fetcher.callCount(),
		"second PeerConnected for same peer should not trigger a second fetch while first is in flight")
}

// TestCanonicalValidatorFilters pins the consumer-side canonical
// filter wiring: a SetCanonicalValidator callback is invoked on the
// received manifest, and its returned subset is what gets published.
// Per docs/plans/20260515-three-layer-snapshot-distribution.md the
// validator drops entries whose (name, hash) doesn't match canonical;
// the rest pass through to the orchestrator.
func TestCanonicalValidatorFilters(t *testing.T) {
	e := newEnv(t)

	// Wire a validator that drops everything except a single block
	// entry — simulates "every entry except headers.seg disagrees
	// with canonical".
	e.p.SetCanonicalValidator(func(adv *downloader.ChainTomlV2) *downloader.ChainTomlV2 {
		require.NotNil(t, adv)
		return &downloader.ChainTomlV2{
			Version: downloader.ChainTomlV2Version,
			Blocks: map[string]string{
				"v1.0-000000-000500-headers.seg": adv.Blocks["v1.0-000000-000500-headers.seg"],
			},
		}
	})

	seedDir := t.TempDir()
	inv := makeInventory(t)
	seedPath, hash := seedPeerManifest(t, seedDir, inv)
	e.fetcher.register(hash, seedPath)

	peer := makePeerNode(t, &enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            hash,
	})
	e.sentry.PublishPeerConnected(peer)

	waitFor(t, func() bool { return e.receivedCount() == 1 },
		2*time.Second, "PeerManifestReceived publish")

	e.recvMu.Lock()
	defer e.recvMu.Unlock()
	got := e.received[0]
	require.Equal(t, peer.ID().String(), got.PeerID)
	// Validator dropped everything except the single block entry.
	require.Len(t, got.Blocks, 1, "validator-returned subset is what's published")
	require.Empty(t, got.Domains, "non-blocks entries dropped by validator")
}

// TestCanonicalValidatorRejectsFullyFiltered pins the "validator
// drops every entry" case: a nil return means the peer has nothing
// useful to offer relative to canonical; no PeerManifestReceived
// publishes.
func TestCanonicalValidatorRejectsFullyFiltered(t *testing.T) {
	e := newEnv(t)

	e.p.SetCanonicalValidator(func(adv *downloader.ChainTomlV2) *downloader.ChainTomlV2 {
		return nil // entire manifest invalid against canonical
	})

	seedDir := t.TempDir()
	inv := makeInventory(t)
	seedPath, hash := seedPeerManifest(t, seedDir, inv)
	e.fetcher.register(hash, seedPath)

	peer := makePeerNode(t, &enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            hash,
	})
	e.sentry.PublishPeerConnected(peer)

	e.bus.WaitAsync()
	time.Sleep(20 * time.Millisecond)
	require.Zero(t, e.receivedCount(),
		"fully-filtered manifest must NOT publish (peer has nothing to offer)")
}

// TestCacheDirWritesPeerManifest pins that with a cache directory
// configured, each received peer manifest's bytes are persisted as
// chain.<peer_id>.toml. Lets the node survive restarts without
// re-fetching every peer's manifest from scratch.
func TestCacheDirWritesPeerManifest(t *testing.T) {
	e := newEnv(t)
	cacheDir := t.TempDir()
	e.p.SetCacheDir(cacheDir)

	seedDir := t.TempDir()
	inv := makeInventory(t)
	seedPath, hash := seedPeerManifest(t, seedDir, inv)
	e.fetcher.register(hash, seedPath)

	peer := makePeerNode(t, &enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            hash,
	})
	e.sentry.PublishPeerConnected(peer)

	waitFor(t, func() bool { return e.receivedCount() == 1 },
		2*time.Second, "PeerManifestReceived publish")

	cached := filepath.Join(cacheDir, "chain."+peer.ID().String()+".toml")
	require.FileExists(t, cached,
		"validated peer manifest is persisted under chain.<peer_id>.toml")

	// Verify cached content equals what the fetcher served.
	cachedBytes, err := os.ReadFile(cached)
	require.NoError(t, err)
	origBytes, err := os.ReadFile(seedPath)
	require.NoError(t, err)
	require.Equal(t, origBytes, cachedBytes,
		"cached manifest bytes match what the fetcher delivered (no transformation)")
}
