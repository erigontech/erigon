//go:build p2p_integration

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

package scenarios_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	dl "github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// seedSubdirFile writes content to <snap>/<relPath> and registers the
// resulting .torrent for seeding, then adds an inventory entry with the
// caller-supplied Name + Domain/Kind/range. Mirrors what production's
// discoverNewFiles + scanAndSeed produce: file lives in its kind's
// subdir on disk, but the inventory entry's Name is the BARE basename
// (so downstream RelPathForName / ResolveExistingPath round-trip).
//
// Use this for files whose production path includes a kind subdir
// (.kv/.kvi/.bt in domain/, .v in history/, .ef in idx/, caplin .seg in
// caplin/). The simpler harness.P2PNode.SeedFile / SeedExistingFile
// helpers don't separate "on-disk relpath" from "inventory entry Name"
// so they can't reproduce the production layout.
func seedSubdirFile(t *testing.T, n *harness.P2PNode, relPath string, content []byte, entry *snapshot.FileEntry) [20]byte {
	t.Helper()
	path := filepath.Join(n.Dirs.Snap, relPath)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, content, 0o644))
	require.NoError(t, n.DownloaderCore().AddNewSeedableFile(context.Background(), relPath))
	torrentFS := dl.NewAtomicTorrentFS(n.Dirs.Snap)
	spec, err := torrentFS.LoadByName(relPath + ".torrent")
	require.NoError(t, err)
	e := *entry
	e.Size = int64(len(content))
	e.TorrentHash = [20]byte(spec.InfoHash)
	e.Local = true
	e.Trust = snapshot.TrustVerified
	require.NoError(t, n.Inventory.AddFile(&e))
	return [20]byte(spec.InfoHash)
}

// TestP2P_PublisherConsumer_RealStorage is the publisher→consumer e2e
// against the production flow.InventoryStorage on the consumer side —
// real RecordFile + validation chain, file resolved on disk. This is
// the path the harness's MockStorage tests skip, and where the gap-D2 /
// gap-I / torrent-naming class of bug lives.
//
// Flow:
//  1. Publisher seeds a state-domain fixture + its chain.toml.v2 via real
//     anacrolix/torrent, advertises the V2 infohash + BT port on its ENR.
//  2. Consumer (real-storage P2PNode) statically peers with the publisher
//     (BitTorrent + DevP2P).
//  3. eth/68 handshake → sentry.PeerConnected → manifest_exchange fetches
//     chain.toml.v2 → flow.PeerManifestReceived → orchestrator gap-fill
//     → DownloadRequested → real transfer → DownloadComplete →
//     storage.RecordFile (real validation, file must be on disk) →
//     statePending hits 0 → InitialStateReady fires.
//  4. Assert: InitialStateReady closes; the file is in the consumer
//     inventory at TrustVerified; no "[flow] storage.RecordFile failed".
func TestP2P_PublisherConsumer_RealStorage(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	publisher := harness.NewP2PNode(t, logger)
	consumer := harness.NewP2PNodeWithRealStorage(t, logger)
	require.NotNil(t, consumer.InitialStateReady, "real-storage node must wire InitialStateReady")

	fixtureName := "v1.0-accounts.0-2048.kv"
	const fixtureSize = 2 << 20
	fixtureContent := multiPieceFixtureBytes("publisher-rs", fixtureSize)
	fixtureHash := publisher.SeedFile(fixtureName, fixtureContent, snapshot.DomainAccounts, 0, 2048)
	require.NotEqual(t, [20]byte{}, fixtureHash)

	v2Hash := publisher.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2Hash)

	_, btPort := publisher.LocalTorrentAddr()
	publisher.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2Hash, DomainSteps: 2048, MergeDepth: 2048})
	publisher.SetDevP2PENREntry(enr.BT(btPort))

	consumer.AddSeederPeer(publisher)

	var (
		manifestCount  atomic.Int32
		promotedCount  atomic.Int32
		requestedCount atomic.Int32
		completeCount  atomic.Int32
		failedCount    atomic.Int32
		initialReady   atomic.Int32
		initialStateCh = consumer.InitialStateReady
	)
	require.NoError(t, consumer.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, consumer.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))
	require.NoError(t, consumer.Bus.Subscribe(func(e flow.DownloadRequested) {
		requestedCount.Add(1)
		t.Logf("DownloadRequested file=%s domain=%q", e.FileName, e.Domain)
	}))
	require.NoError(t, consumer.Bus.Subscribe(func(e flow.DownloadComplete) {
		completeCount.Add(1)
		t.Logf("DownloadComplete file=%s", e.FileName)
	}))
	require.NoError(t, consumer.Bus.Subscribe(func(e flow.DownloadFailed) {
		failedCount.Add(1)
		t.Logf("DownloadFailed file=%s reason=%s", e.FileName, e.Reason)
	}))
	require.NoError(t, consumer.Bus.Subscribe(func(flow.InitialStateReady) { initialReady.Add(1) }))
	var subOnceFired atomic.Int32
	require.NoError(t, consumer.Bus.SubscribeOnce(func(flow.InitialStateReady) {
		subOnceFired.Add(1)
		t.Log("TEST SubscribeOnce(flow.InitialStateReady) FIRED")
	}))

	consumer.AddDevP2PPeer(publisher.DevP2PSelf())

	// The whole point: InitialStateReady must fire, which means the
	// downloaded file actually landed where RecordFile looks for it.
	select {
	case <-initialStateCh:
	case <-time.After(45 * time.Second):
		localFiles := consumer.Inventory.LocalFiles(snapshot.DomainAccounts)
		t.Fatalf("InitialStateReady did not fire within 45s "+
			"(manifests=%d requested=%d complete=%d failed=%d promoted=%d initialReadyEvents=%d subOnceFired=%d localAccountsFiles=%d)",
			manifestCount.Load(), requestedCount.Load(), completeCount.Load(), failedCount.Load(),
			promotedCount.Load(), initialReady.Load(), subOnceFired.Load(), len(localFiles))
	}

	require.Equal(t, int32(1), manifestCount.Load(), "exactly one PeerManifestReceived")
	require.GreaterOrEqual(t, promotedCount.Load(), int32(1), "fixture promoted to TrustVerified")

	localFiles := consumer.Inventory.LocalFiles(snapshot.DomainAccounts)
	require.Len(t, localFiles, 1)
	require.Equal(t, fixtureName, localFiles[0].Name)
	require.Equal(t, fixtureHash, localFiles[0].TorrentHash)
	require.Equal(t, snapshot.TrustVerified, localFiles[0].Trust)
}

// TestP2P_PublisherConsumer_RealStorage_ProductionLayout extends the
// real-storage e2e with the file shapes a production publisher emits:
//
//   - accounts .kv in domain/   (gap-D2 base case)
//   - accounts .v  in history/  (gap-I: bare entry Name, subdir file)
//   - accounts .ef in idx/      (gap-I)
//   - receipt  .kv in domain/   (gap-G: scanAndSeed must cover all domains)
//   - salt-state.txt top-level  (gap-K: SetTorrentHash must cover salt)
//
// The publisher writes each file in its kind subdir with the
// production-shaped torrent info.Name, and registers a bare-named
// inventory entry. The consumer must reach InitialStateReady — which
// means every file landed where storage.RecordFile resolves it AND
// every state-domain DownloadComplete decremented statePending.
func TestP2P_PublisherConsumer_RealStorage_ProductionLayout(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	publisher := harness.NewP2PNode(t, logger)
	consumer := harness.NewP2PNodeWithRealStorage(t, logger)
	require.NotNil(t, consumer.InitialStateReady)

	// Smallish content so the test is fast on loopback.
	mkContent := func(tag string, n int) []byte { return multiPieceFixtureBytes("prodlayout:"+tag, n) }

	type seedSpec struct {
		relPath string
		entry   snapshot.FileEntry // Domain/Kind/FromStep/ToStep/Name only
		size    int
	}
	specs := []seedSpec{
		// accounts domain — kv primary, v history, ef idx
		{relPath: "domain/v1.0-accounts.0-2048.kv", entry: snapshot.FileEntry{Name: "v1.0-accounts.0-2048.kv", Domain: snapshot.DomainAccounts, Kind: snapshot.KindKV, FromStep: 0, ToStep: 2048}, size: 32 << 10},
		{relPath: "history/v1.0-accounts.0-2048.v", entry: snapshot.FileEntry{Name: "v1.0-accounts.0-2048.v", Domain: snapshot.DomainAccounts, Kind: snapshot.KindHistory, FromStep: 0, ToStep: 2048}, size: 16 << 10},
		{relPath: "idx/v1.0-accounts.0-2048.ef", entry: snapshot.FileEntry{Name: "v1.0-accounts.0-2048.ef", Domain: snapshot.DomainAccounts, Kind: snapshot.KindIdx, FromStep: 0, ToStep: 2048}, size: 16 << 10},
		// receipt domain — gap-G regression
		{relPath: "domain/v3.0-receipt.0-2048.kv", entry: snapshot.FileEntry{Name: "v3.0-receipt.0-2048.kv", Domain: snapshot.DomainReceipt, Kind: snapshot.KindKV, FromStep: 0, ToStep: 2048}, size: 16 << 10},
	}

	expectedHashes := make(map[string][20]byte, len(specs)+1)
	for _, s := range specs {
		content := mkContent(s.relPath, s.size)
		e := s.entry
		h := seedSubdirFile(t, publisher, s.relPath, content, &e)
		expectedHashes[s.entry.Name] = h
	}
	// Salt — top-level on disk, KindSalt entry. Gap-K regression: the
	// publisher's V2 manifest must include this; without K's fix the
	// salt entry stays at zero hash and GenerateV2 drops it.
	saltContent := mkContent("salt-state.txt", 4<<10)
	saltHash := seedSubdirFile(t, publisher, "salt-state.txt", saltContent, &snapshot.FileEntry{Name: "salt-state.txt", Kind: snapshot.KindSalt})
	expectedHashes["salt-state.txt"] = saltHash

	v2Hash := publisher.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2Hash)

	_, btPort := publisher.LocalTorrentAddr()
	publisher.SetDevP2PENREntry(enr.ChainToml{InfoHash: v2Hash, DomainSteps: 2048, MergeDepth: 2048})
	publisher.SetDevP2PENREntry(enr.BT(btPort))

	consumer.AddSeederPeer(publisher)

	var (
		promotedCount  atomic.Int32
		manifestCount  atomic.Int32
		initialStateCh = consumer.InitialStateReady
	)
	require.NoError(t, consumer.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, consumer.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))

	var (
		stateReqs      atomic.Int32
		simpleReqs     atomic.Int32
		downloadComp   atomic.Int32
		downloadFail   atomic.Int32
		initialReadyEv atomic.Int32
	)
	require.NoError(t, consumer.Bus.Subscribe(func(e flow.DownloadRequested) {
		if e.Domain != "" {
			stateReqs.Add(1)
		} else {
			simpleReqs.Add(1)
		}
		t.Logf("DownloadRequested file=%q domain=%q", e.FileName, e.Domain)
	}))
	require.NoError(t, consumer.Bus.Subscribe(func(e flow.DownloadComplete) {
		downloadComp.Add(1)
		t.Logf("DownloadComplete file=%q", e.FileName)
	}))
	require.NoError(t, consumer.Bus.Subscribe(func(e flow.DownloadFailed) {
		downloadFail.Add(1)
		t.Logf("DownloadFailed file=%q reason=%q", e.FileName, e.Reason)
	}))
	require.NoError(t, consumer.Bus.Subscribe(func(e flow.InitialStateReady) {
		initialReadyEv.Add(1)
		t.Logf("InitialStateReady fired (domains=%v)", e.StateDomains)
	}))

	consumer.AddDevP2PPeer(publisher.DevP2PSelf())

	// Wait for everything to land — state (4) + salt (1) = 5 promotions.
	// initialStateCh closes earlier (when state phase drains) but salt
	// is queued behind state-ready and needs more time. Waiting on the
	// last TrustPromoted is the durable signal that the whole manifest
	// transferred.
	const expectedPromotions = int32(5)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if promotedCount.Load() >= expectedPromotions {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if promotedCount.Load() < expectedPromotions {
		t.Fatalf("only %d/%d files promoted within 60s "+
			"(manifests=%d stateReqs=%d simpleReqs=%d complete=%d failed=%d accounts=%d receipt=%d salt=%d initialReadyFired=%d)",
			promotedCount.Load(), expectedPromotions,
			manifestCount.Load(), stateReqs.Load(), simpleReqs.Load(),
			downloadComp.Load(), downloadFail.Load(),
			len(consumer.Inventory.LocalFiles(snapshot.DomainAccounts)),
			len(consumer.Inventory.LocalFiles(snapshot.DomainReceipt)),
			len(consumer.Inventory.SaltFiles()), initialReadyEv.Load())
	}
	t.Logf("All %d files promoted — stateReqs=%d simpleReqs=%d complete=%d failed=%d initialReadyEvents=%d",
		promotedCount.Load(), stateReqs.Load(), simpleReqs.Load(), downloadComp.Load(), downloadFail.Load(), initialReadyEv.Load())

	// InitialStateReady channel must have closed too.
	select {
	case <-initialStateCh:
	default:
		t.Fatalf("InitialStateReady channel never closed despite %d promotions", promotedCount.Load())
	}

	// Manifest carries the salt entry (gap-K guard).
	require.GreaterOrEqual(t, len(consumer.Inventory.SaltFiles()), 1,
		"consumer must have received the salt entry; if zero, GenerateV2 dropped it because the publisher inventory's salt entry had a zero TorrentHash (gap-K)")

	// All state-domain files reached the consumer at the right path.
	for _, dom := range []snapshot.Domain{snapshot.DomainAccounts, snapshot.DomainReceipt} {
		files := consumer.Inventory.LocalFiles(dom)
		require.NotEmpty(t, files, "consumer.Inventory.LocalFiles(%s) must be non-empty", dom)
		for _, f := range files {
			require.Equal(t, snapshot.TrustVerified, f.Trust, "%s should be TrustVerified", f.Name)
			require.Equal(t, expectedHashes[f.Name], f.TorrentHash, "%s torrent hash must match publisher's", f.Name)
		}
	}

	// Salt entry is present.
	saltFiles := consumer.Inventory.SaltFiles()
	require.NotEmpty(t, saltFiles)
	var sawSalt bool
	for _, f := range saltFiles {
		if f.Name == "salt-state.txt" {
			sawSalt = true
			require.Equal(t, expectedHashes["salt-state.txt"], f.TorrentHash)
			require.Equal(t, snapshot.TrustVerified, f.Trust)
		}
	}
	require.True(t, sawSalt, "salt-state.txt entry must be present on consumer")
}
