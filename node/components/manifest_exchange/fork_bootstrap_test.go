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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/storage/flow"
)

var errForkBootstrapFetchFailed = errors.New("fetch failed (test)")

// captureSyntheticManifest returns a subscriber that records every
// flow.PeerManifestReceived whose PeerID matches the fork-bootstrap
// sentinel — gates out real-peer events so the test only sees
// parent-manifest events.
func captureSyntheticManifest(t *testing.T, p *Provider) *[]flow.PeerManifestReceived {
	t.Helper()
	var mu sync.Mutex
	captured := []flow.PeerManifestReceived{}
	require.NoError(t, p.bus.Subscribe(func(e flow.PeerManifestReceived) {
		if e.PeerID != ForkBootstrapParentPeerID {
			return
		}
		mu.Lock()
		captured = append(captured, e)
		mu.Unlock()
	}))
	return &captured
}

func TestOnForkBootstrapRequired_ZeroHashSkipsFetch(t *testing.T) {
	// A fork off a pre-Phase-1 root with no V2 manifest to pin: zero
	// ParentManifestHash. Subscriber must skip the fetch (no
	// fetcher call) and publish nothing.
	p := &Provider{log: log.New()}
	bus := newTestBus()
	fetcher := newMockFetcher()
	require.NoError(t, p.BindBus(context.Background(), bus, fetcher, log.New()))
	t.Cleanup(func() { _ = p.UnbindBus() })

	captured := captureSyntheticManifest(t, p)

	p.onForkBootstrapRequired(flow.ForkBootstrapRequired{
		Parent:             "legacy-root",
		CutBlock:           100,
		ParentManifestHash: [20]byte{}, // zero
	})
	bus.WaitAsync()
	p.fetchWG.Wait()

	require.Equal(t, 0, fetcher.callCount(), "zero hash → no fetch")
	require.Empty(t, *captured, "zero hash → no publication")
}

func TestOnForkBootstrapRequired_PublishesAsSyntheticPeerManifest(t *testing.T) {
	// Happy path: a registered parent manifest is fetched, parsed,
	// re-emitted as a flow.PeerManifestReceived whose PeerID is the
	// fork-bootstrap sentinel. Entries land in the corresponding
	// buckets — Blocks here since the seeded inventory contains
	// block files.
	dir := t.TempDir()
	inv := makeInventory(t)
	manifestPath, manifestHash := seedPeerManifest(t, dir, inv)
	require.NotEmpty(t, manifestPath)

	p := &Provider{log: log.New()}
	bus := newTestBus()
	fetcher := newMockFetcher()
	fetcher.register(manifestHash, manifestPath)
	require.NoError(t, p.BindBus(context.Background(), bus, fetcher, log.New()))
	t.Cleanup(func() { _ = p.UnbindBus() })

	captured := captureSyntheticManifest(t, p)

	p.onForkBootstrapRequired(flow.ForkBootstrapRequired{
		Parent:             "mainnet",
		CutBlock:           20_000_000,
		ParentManifestHash: manifestHash,
	})

	require.Eventually(t, func() bool {
		p.fetchWG.Wait()
		bus.WaitAsync()
		return len(*captured) == 1
	}, 2*time.Second, 25*time.Millisecond, "exactly one synthetic peer manifest emitted")

	got := (*captured)[0]
	require.Equal(t, ForkBootstrapParentPeerID, got.PeerID)
	require.NotEmpty(t, got.Blocks, "block entries from the seeded inventory must survive into the synthetic event")
	require.Equal(t, 1, fetcher.callCount())
}

func TestOnForkBootstrapRequired_FetchErrorIsLoggedNotPanic(t *testing.T) {
	// Fetcher returns an error (network failure / unreachable swarm).
	// Subscriber must log and return — no publication, no panic.
	p := &Provider{log: log.New()}
	bus := newTestBus()
	fetcher := newMockFetcher()
	fetcher.setError(errForkBootstrapFetchFailed)
	require.NoError(t, p.BindBus(context.Background(), bus, fetcher, log.New()))
	t.Cleanup(func() { _ = p.UnbindBus() })

	captured := captureSyntheticManifest(t, p)

	require.NotPanics(t, func() {
		p.onForkBootstrapRequired(flow.ForkBootstrapRequired{
			Parent:             "mainnet",
			CutBlock:           100,
			ParentManifestHash: [20]byte{0x01},
		})
		p.fetchWG.Wait()
	})
	bus.WaitAsync()
	require.Empty(t, *captured, "fetch error → no publication")
}

func TestOnForkBootstrapRequired_ParseErrorIsLoggedNotPanic(t *testing.T) {
	// Fetcher returns garbage bytes. ParseV2 fails; subscriber logs
	// + returns without publishing.
	dir := t.TempDir()
	garbagePath := dir + "/garbage.toml"
	require.NoError(t, os.WriteFile(garbagePath, []byte("not valid toml v2 = ="), 0o644))

	p := &Provider{log: log.New()}
	bus := newTestBus()
	fetcher := newMockFetcher()
	garbageHash := [20]byte{0xde, 0xad, 0xbe, 0xef}
	fetcher.register(garbageHash, garbagePath)
	require.NoError(t, p.BindBus(context.Background(), bus, fetcher, log.New()))
	t.Cleanup(func() { _ = p.UnbindBus() })

	captured := captureSyntheticManifest(t, p)

	p.onForkBootstrapRequired(flow.ForkBootstrapRequired{
		Parent:             "mainnet",
		ParentManifestHash: garbageHash,
	})
	p.fetchWG.Wait()
	bus.WaitAsync()
	require.Empty(t, *captured, "parse error → no publication")
}

func TestOnForkBootstrapRequired_UnboundIsNoOp(t *testing.T) {
	// Subscriber called on an unbound Provider must return cleanly
	// (no nil-deref). Defends against the event firing between
	// UnbindBus and goroutine cleanup.
	p := &Provider{log: log.New()}
	require.NotPanics(t, func() {
		p.onForkBootstrapRequired(flow.ForkBootstrapRequired{
			Parent:             "mainnet",
			ParentManifestHash: [20]byte{0x01},
		})
	})
}
