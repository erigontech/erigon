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

// Publisher-restart scenario: a seeder publishes several V2 manifest
// generations, then the process restarts over the same data directory.
// The rebuilt node must rediscover its on-disk generations into the
// RollingV2Publisher history (so the validity rule stays enforceable)
// and continue to seed its latest manifest over real BitTorrent to a
// fresh leecher. This is the integration-level counterpart of the
// RollingV2Publisher restart unit tests, which run against a nil
// downloader — here a real anacrolix torrent client is torn down and
// rebuilt across the restart.

package scenarios_test

import (
	"os"
	"testing"
	"time"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// restartStateFile describes one state file the restart seeder holds.
type restartStateFile struct {
	name     string
	domain   snapshot.Domain
	fromStep uint64
	toStep   uint64
}

// TestP2P_PublisherRestartResumesGenerations is the end-to-end proof
// that a publisher's V2 generations survive a process restart:
//
//  1. A seeder seeds a state archive and publishes generation 1.
//  2. It seeds one more file and publishes generation 2 — a distinct
//     content-addressed generation whose manifest is a superset of
//     generation 1, so generation 1 stays subset-valid (not evicted).
//  3. The node restarts: every component is torn down and rebuilt over
//     the same data directory. The chain.v2.<fp>.<genID>.toml files and
//     their .torrent sidecars survive on disk.
//  4. The rebuilt node re-seeds its files from disk; ResumePublisherSeeding
//     rediscovers both prior generations and re-registers their torrents
//     — including the older, non-latest one — so a peer holding a stale
//     ENR can still fetch the generation it handshook on.
//  5. The node publishes again; discovery must recover exactly the two
//     prior generations with no duplicates.
//  6. A fresh leecher joins and fetches the restarted seeder's latest
//     manifest over real BitTorrent — proving the restarted node still
//     seeds correctly.
func TestP2P_PublisherRestartResumesGenerations(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	seeder := harness.NewP2PNode(t, logger)

	// Generation 1: a two-file state archive.
	gen1Files := []restartStateFile{
		{"v1.0-accounts.0-512.kv", snapshot.DomainAccounts, 0, 512},
		{"v1.0-storage.0-512.kv", snapshot.DomainStorage, 0, 512},
	}
	// Generation 2 adds one more file on top, so gen 1's name-set stays
	// a subset of gen 2's — the validity rule keeps gen 1 retained.
	gen2Extra := restartStateFile{"v1.0-code.0-512.kv", snapshot.DomainCode, 0, 512}
	allFiles := append(append([]restartStateFile(nil), gen1Files...), gen2Extra)

	for _, f := range gen1Files {
		seeder.SeedFile(f.name, multiPieceFixtureBytes("restart:"+f.name, 256<<10),
			f.domain, f.fromStep, f.toStep)
	}
	gen1Hash := seeder.PublishV2Manifest()

	seeder.SeedFile(gen2Extra.name, multiPieceFixtureBytes("restart:"+gen2Extra.name, 256<<10),
		gen2Extra.domain, gen2Extra.fromStep, gen2Extra.toStep)
	gen2Hash := seeder.PublishV2Manifest()

	require.NotEqual(t, gen1Hash, gen2Hash, "the two generations must be distinct")
	preRestartHistory := seeder.V2Publisher().History()
	require.Len(t, preRestartHistory, 2,
		"two distinct inventories must produce two retained generations")

	// --- Restart -----------------------------------------------------
	seeder = seeder.Restart()

	// The rebuilt node has a fresh empty inventory. Re-seed every file
	// from disk, exactly as production's startup disk scan does.
	for _, f := range allFiles {
		seeder.SeedExistingFile(f.name, &snapshot.FileEntry{
			Domain:   f.domain,
			FromStep: f.fromStep,
			ToStep:   f.toStep,
		})
	}

	// ResumePublisherSeeding rediscovers the on-disk generations and
	// re-registers their torrents. Both must now be seedable — the
	// LATEST generation and the older retained one alike.
	seeder.ResumePublisherSeeding()
	client := seeder.DownloaderCore().TorrentClient()
	_, gen1Seeded := client.Torrent(metainfo.Hash(gen1Hash))
	require.True(t, gen1Seeded,
		"the older retained generation's manifest torrent must be re-seeded after restart")
	_, gen2Seeded := client.Torrent(metainfo.Hash(gen2Hash))
	require.True(t, gen2Seeded,
		"the latest generation's manifest torrent must be re-seeded after restart")

	// First post-restart publish: the RollingV2Publisher republishes the
	// current (gen-2-equivalent) inventory; discovery must have recovered
	// both prior chain.v2.*.toml generations into history with no
	// content-addressed-republish duplicate.
	v2Hash := seeder.PublishV2Manifest()
	require.NotEqual(t, [20]byte{}, v2Hash)

	postRestartHistory := seeder.V2Publisher().History()
	require.ElementsMatch(t, preRestartHistory, postRestartHistory,
		"restart discovery must recover exactly the pre-restart generations — "+
			"no duplicates from the content-addressed republish, no losses")

	// --- A fresh leecher fetches the restarted seeder's manifest -----
	leecher := harness.NewP2PNode(t, logger)
	leecher.AddSeederPeer(seeder)

	var manifestCount int
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount++ }))

	_, btPort := seeder.LocalTorrentAddr()
	seeder.SetDevP2PENREntry(enr.ChainToml{
		InfoHash:    v2Hash,
		DomainSteps: 512,
		MergeDepth:  512,
	})
	seeder.SetDevP2PENREntry(enr.BT(btPort))
	leecher.AddDevP2PPeer(seeder.DevP2PSelf())

	waitForP2P(t, func() bool { return manifestCount >= 1 }, 30*time.Second,
		"leecher fetches the restarted seeder's V2 manifest over BitTorrent")
}
