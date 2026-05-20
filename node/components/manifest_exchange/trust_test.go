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
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/snapshotauth"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// mockUCANFetcher returns canned UCAN bytes by infohash. Same shape as
// mockFetcher (manifest side); they share none of their state.
type mockUCANFetcher struct {
	mu      sync.Mutex
	sources map[[20]byte][]byte
	calls   int
}

func newMockUCANFetcher() *mockUCANFetcher {
	return &mockUCANFetcher{sources: make(map[[20]byte][]byte)}
}

func (m *mockUCANFetcher) register(hash [20]byte, ucanBytes []byte) {
	m.mu.Lock()
	m.sources[hash] = ucanBytes
	m.mu.Unlock()
}

func (m *mockUCANFetcher) FetchPeerUCAN(_ context.Context, _ string, infoHash [20]byte, _ net.IP, _ uint16) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	bytes, ok := m.sources[infoHash]
	if !ok {
		return nil, fmt.Errorf("UCAN hash %x not registered", infoHash)
	}
	return bytes, nil
}

func (m *mockUCANFetcher) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

var _ UCANFetcher = (*mockUCANFetcher)(nil)

// trustHarness owns the keys + verifier for trust-gated tests.
type trustHarness struct {
	rootKey  *ecdsa.PrivateKey
	verifier *snapshotauth.Verifier
}

func newTrustHarness(t *testing.T) *trustHarness {
	t.Helper()
	rk, err := crypto.GenerateKey()
	require.NoError(t, err)
	v := snapshotauth.NewVerifier([]snapshotauth.TrustRoot{{
		Kind:   snapshotauth.RootENR,
		Pubkey: compressedPub(&rk.PublicKey),
	}})
	return &trustHarness{rootKey: rk, verifier: v}
}

// issueDelegation returns CBOR bytes of a root-issued delegation to
// audienceKey. Indefinite expiry; depth 1.
func (h *trustHarness) issueDelegation(t *testing.T, audienceKey *ecdsa.PrivateKey, caps []string) []byte {
	t.Helper()
	d, err := snapshotauth.New(
		&h.rootKey.PublicKey, &audienceKey.PublicKey,
		caps, time.Time{}, time.Time{}, 1, nil,
	)
	require.NoError(t, err)
	require.NoError(t, d.Sign(h.rootKey))
	enc, err := d.Encode()
	require.NoError(t, err)
	return enc
}

// makeSignedPeer constructs an enode.Node signed by peerKey.
func makeSignedPeer(t *testing.T, peerKey *ecdsa.PrivateKey, ct enr.ChainToml) *enode.Node {
	t.Helper()
	var r enr.Record
	r.Set(ct)
	require.NoError(t, enode.SignV4(&r, peerKey))
	n, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)
	return n
}

func compressedPub(p *ecdsa.PublicKey) []byte {
	return elliptic.MarshalCompressed(p.Curve, p.X, p.Y)
}

// writeV2WithAuthorityUCANHash writes a V2 manifest TOML carrying the given
// AuthorityUCANHash into dir and returns the path. dir is supplied by the caller
// so its lifetime is controlled (avoids cleanup-vs-fetch races at test
// teardown). The manifest body is the standard makeInventory shape.
func writeV2WithAuthorityUCANHash(t *testing.T, dir string, name string, ucanHashHex string) string {
	t.Helper()
	v2 := downloader.ChainTomlV2{
		Version:           2,
		AuthorityUCANHash: ucanHashHex,
		Domains: map[string]*downloader.DomainManifest{
			"accounts": {
				Coverage: [2]uint64{0, 2048},
				Files: []downloader.DomainFileEntry{{
					Name:  "v1.0-accounts.0-2048.kv",
					Range: [2]uint64{0, 2048},
					Kind:  "kv",
					Hash:  "1122334400000000000000000000000000000000",
					Trust: "verified",
				}},
			},
		},
		Blocks: map[string]string{
			"v1.0-000000-000500-headers.seg": "aabbccdd00000000000000000000000000000000",
		},
	}
	path := filepath.Join(dir, name)
	data, err := toml.Marshal(&v2)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

// fakeHash builds a deterministic 20-byte hash from a label so tests
// can wire (V2 hash, UCAN hash) by name.
func fakeHash(label string) [20]byte {
	var h [20]byte
	copy(h[:], label)
	return h
}

// trustEnv composes a manifest_exchange Provider wired with a
// TrustConfig. dir is the test's working directory — owned by the
// trustEnv so its cleanup runs AFTER the Provider's UnbindBus drains
// the in-flight fetch goroutines (avoids "no such file" warnings at
// teardown when the goroutine reads from disk).
type trustEnv struct {
	*env
	ucanFetcher *mockUCANFetcher
	harness     *trustHarness
	dir         string
}

func newTrustEnv(t *testing.T, cfgMutate func(*TrustConfig)) *trustEnv {
	t.Helper()
	// Allocate the tempdir BEFORE registering the Provider's cleanup so
	// the Provider's UnbindBus runs first (LIFO) and the fetch
	// goroutine has its files until it returns.
	dir := t.TempDir()

	bus := newTestBus()
	manifestFetcher := newMockFetcher()
	ucanFetcher := newMockUCANFetcher()
	harness := newTrustHarness(t)

	sp := &sentry.Provider{}
	require.NoError(t, sp.BindBus(bus))

	p := &Provider{}
	cfg := &TrustConfig{
		Verifier: harness.verifier,
		Fetcher:  ucanFetcher,
		RequiredCapabilities: []string{
			string(snapshotauth.CapAdvertise),
			string(snapshotauth.CapServe),
		},
	}
	if cfgMutate != nil {
		cfgMutate(cfg)
	}
	require.NoError(t, p.SetTrust(cfg))
	require.NoError(t, p.BindBus(context.Background(), bus, manifestFetcher, nil))

	e := &env{bus: bus, p: p, sentry: sp, fetcher: manifestFetcher}
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
	return &trustEnv{env: e, ucanFetcher: ucanFetcher, harness: harness, dir: dir}
}

// seedPair returns the V2 infohash to wire into the peer's ENR. Both
// the V2 file and the UCAN bytes are registered with their respective
// mock fetchers under deterministic fake hashes — the test does not
// exercise real torrent infohash computation, only the gate logic.
func (te *trustEnv) seedPair(t *testing.T, peerKey *ecdsa.PrivateKey, caps []string) (v2Hash [20]byte) {
	t.Helper()
	ucanBytes := te.harness.issueDelegation(t, peerKey, caps)
	tag := fmt.Sprintf("%x", peerKey.PublicKey.X.Bytes()[:4])
	ucanHash := fakeHash("ucan-" + tag)
	te.ucanFetcher.register(ucanHash, ucanBytes)

	v2Path := writeV2WithAuthorityUCANHash(t, te.dir, "v2-"+tag+".toml", hex.EncodeToString(ucanHash[:]))
	v2Hash = fakeHash("v2-" + tag)
	te.fetcher.register(v2Hash, v2Path)
	return v2Hash
}

func TestUCANGate_NilTrustPreservesOldBehaviour(t *testing.T) {
	// Without SetTrust, the manifest_exchange Provider behaves as
	// before. (Standard env from provider_test.go.)
	e := newEnv(t)

	seedPath, hash := seedPeerManifest(t, t.TempDir(), makeInventory(t))
	e.fetcher.register(hash, seedPath)

	peer := makePeerNode(t, &enr.ChainToml{InfoHash: hash})
	e.sentry.PublishPeerConnected(peer)

	waitFor(t, func() bool { return e.receivedCount() == 1 },
		2*time.Second, "PeerManifestReceived published with no trust gate")
}

func TestUCANGate_ValidUCANIsAccepted(t *testing.T) {
	te := newTrustEnv(t, nil)

	peerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	v2Hash := te.seedPair(t, peerKey, []string{
		string(snapshotauth.CapAdvertise),
		string(snapshotauth.CapServe),
	})

	peer := makeSignedPeer(t, peerKey, enr.ChainToml{InfoHash: v2Hash})
	te.sentry.PublishPeerConnected(peer)

	waitFor(t, func() bool { return te.receivedCount() == 1 },
		2*time.Second, "valid UCAN admits PeerManifestReceived")
}

func TestUCANGate_InvalidUCANIsRejectedAndPeerBlacklisted(t *testing.T) {
	te := newTrustEnv(t, func(c *TrustConfig) {
		c.BlacklistDuration = 50 * time.Millisecond
	})

	// Issue a delegation under an UNTRUSTED root key. Signature
	// verifies but the chain has no matching trust root.
	rogueRoot, err := crypto.GenerateKey()
	require.NoError(t, err)
	peerKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	bogus, err := snapshotauth.New(
		&rogueRoot.PublicKey, &peerKey.PublicKey,
		[]string{string(snapshotauth.CapAdvertise), string(snapshotauth.CapServe)},
		time.Time{}, time.Time{}, 0, nil,
	)
	require.NoError(t, err)
	require.NoError(t, bogus.Sign(rogueRoot))
	bogusBytes, err := bogus.Encode()
	require.NoError(t, err)

	ucanHash := fakeHash("rogue-ucan")
	te.ucanFetcher.register(ucanHash, bogusBytes)
	v2Path := writeV2WithAuthorityUCANHash(t, te.dir, "rogue.toml", hex.EncodeToString(ucanHash[:]))
	v2Hash := fakeHash("rogue-v2")
	te.fetcher.register(v2Hash, v2Path)

	peer := makeSignedPeer(t, peerKey, enr.ChainToml{InfoHash: v2Hash})
	te.sentry.PublishPeerConnected(peer)
	te.bus.WaitAsync()

	require.Equal(t, 0, te.receivedCount(),
		"untrusted UCAN must not produce PeerManifestReceived")

	// Peer is now blacklisted. A second connect must NOT trigger a
	// fresh UCAN fetch.
	callsBefore := te.ucanFetcher.callCount()
	te.sentry.PublishPeerConnected(peer)
	te.bus.WaitAsync()
	require.Equal(t, callsBefore, te.ucanFetcher.callCount(),
		"blacklisted peer must not re-trigger UCAN fetch")
}

func TestUCANGate_MissingAuthorityUCANHashRejects(t *testing.T) {
	te := newTrustEnv(t, nil)

	peerKey, err := crypto.GenerateKey()
	require.NoError(t, err)

	// V2 manifest has NO AuthorityUCANHash field.
	v2Path := writeV2WithAuthorityUCANHash(t, te.dir, "no-ucan.toml", "")
	v2Hash := fakeHash("no-ucan-v2")
	te.fetcher.register(v2Hash, v2Path)

	peer := makeSignedPeer(t, peerKey, enr.ChainToml{InfoHash: v2Hash})
	te.sentry.PublishPeerConnected(peer)
	te.bus.WaitAsync()

	require.Equal(t, 0, te.receivedCount(),
		"manifest with no UCAN reference must be rejected when trust is configured")
}

func TestUCANGate_CachedTrustSkipsReverify(t *testing.T) {
	te := newTrustEnv(t, nil)

	peerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	v2Hash := te.seedPair(t, peerKey, []string{
		string(snapshotauth.CapAdvertise),
		string(snapshotauth.CapServe),
	})

	peer := makeSignedPeer(t, peerKey, enr.ChainToml{InfoHash: v2Hash})

	te.sentry.PublishPeerConnected(peer)
	waitFor(t, func() bool { return te.receivedCount() == 1 },
		2*time.Second, "first publish")
	firstCalls := te.ucanFetcher.callCount()

	// Disconnect, then reconnect. With ReverifyOnReconnect=false (the
	// default), trust cache survives — UCAN is NOT re-fetched.
	te.sentry.PublishPeerDisconnected(peer.ID().String())
	te.bus.WaitAsync()
	te.sentry.PublishPeerConnected(peer)
	waitFor(t, func() bool { return te.receivedCount() == 2 },
		2*time.Second, "reconnect publishes again")
	require.Equal(t, firstCalls, te.ucanFetcher.callCount(),
		"cached trust must short-circuit UCAN re-fetch")
}

func TestUCANGate_ReverifyOnReconnectEvictsCache(t *testing.T) {
	te := newTrustEnv(t, func(c *TrustConfig) {
		c.ReverifyOnReconnect = true
	})

	peerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	v2Hash := te.seedPair(t, peerKey, []string{
		string(snapshotauth.CapAdvertise),
		string(snapshotauth.CapServe),
	})

	peer := makeSignedPeer(t, peerKey, enr.ChainToml{InfoHash: v2Hash})

	te.sentry.PublishPeerConnected(peer)
	waitFor(t, func() bool { return te.receivedCount() == 1 },
		2*time.Second, "first publish")
	firstCalls := te.ucanFetcher.callCount()

	te.sentry.PublishPeerDisconnected(peer.ID().String())
	te.bus.WaitAsync()
	te.sentry.PublishPeerConnected(peer)
	waitFor(t, func() bool { return te.receivedCount() == 2 },
		2*time.Second, "reconnect publishes again")

	require.Greater(t, te.ucanFetcher.callCount(), firstCalls,
		"ReverifyOnReconnect=true must re-fetch the UCAN after disconnect")
}
