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

// End-to-end swarm test: a minimal but complete snapshot archive is
// partitioned across multiple cooperating seeders (no single seeder has
// the full archive), a fresh leecher joins, and the full real-stack
// lifecycle pulls everything through. Every file on the leecher is
// byte-compared against the source. Phased scheduling must serialise
// state-domain downloads before block downloads.

package scenarios_test

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/components/integration/snapshot/harness"
	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// archiveFile describes one file in the target minimal archive. Name,
// canonical step range, domain (empty for block files), and the seeder
// that holds it. The content is derived deterministically from Name so
// the leecher-side byte check doesn't need to carry it around.
type archiveFile struct {
	name     string
	domain   snapshot.Domain
	fromStep uint64
	toStep   uint64
	seederID int
}

// buildTargetArchive describes the archive the swarm holds collectively.
// Four state domains each with one canonical file at [0, 512), plus
// three block ranges carrying headers + bodies each. Fourteen files in
// total; no single seeder has more than four.
func buildTargetArchive() []archiveFile {
	var out []archiveFile
	// State domains: each has .kv + .kvi at [0, 512).
	states := []struct {
		d   snapshot.Domain
		sid int
	}{
		{snapshot.DomainAccounts, 0},
		{snapshot.DomainStorage, 0},
		{snapshot.DomainCode, 1},
		{snapshot.DomainCommitment, 1},
	}
	for _, s := range states {
		out = append(out,
			archiveFile{
				name:   fmt.Sprintf("v1.0-%s.0-512.kv", s.d),
				domain: s.d, fromStep: 0, toStep: 512, seederID: s.sid,
			},
			archiveFile{
				name:   fmt.Sprintf("v1.0-%s.0-512.kvi", s.d),
				domain: s.d, fromStep: 0, toStep: 512, seederID: s.sid,
			},
		)
	}
	// Block ranges: 3 ranges × headers + bodies = 6 block files split
	// across two seeders.
	blockRanges := []struct {
		from, to uint64
		sid      int
	}{
		{0, 500, 2},
		{500, 1000, 2},
		{1000, 1500, 3},
	}
	for _, r := range blockRanges {
		out = append(out,
			archiveFile{
				name:     fmt.Sprintf("v1.0-%06d-%06d-headers.seg", r.from, r.to),
				fromStep: r.from, toStep: r.to, seederID: r.sid,
			},
			archiveFile{
				name:     fmt.Sprintf("v1.0-%06d-%06d-bodies.seg", r.from, r.to),
				fromStep: r.from, toStep: r.to, seederID: r.sid,
			},
		)
	}
	return out
}

// fileContent is the deterministic content for a given filename. 8 KiB
// — small enough for the scenario to run in seconds, large enough to
// exercise multi-piece torrent transfer.
func fileContent(name string) []byte {
	return multiPieceFixtureBytes("swarm:"+name, 8<<10)
}

// TestP2P_Swarm_CompleteArchive is the end-to-end proof that a complete
// minimal archive partitioned across N cooperating seeders can be
// assembled by a fresh leecher joining the swarm. No single seeder has
// the whole archive; the leecher must pull from multiple peers, phased
// scheduling must put state files before block files, and every byte
// on the leecher's disk must match the source.
func TestP2P_Swarm_CompleteArchive(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	const numSeeders = 4
	archive := buildTargetArchive()

	seeders := make([]*harness.P2PNode, numSeeders)
	for i := range seeders {
		seeders[i] = harness.NewP2PNode(t, logger)
	}

	// Seed each file on its designated seeder. Record the hash so the
	// leecher-side byte check can verify via content and the inventory
	// check can verify by torrent hash.
	expectedHashes := make(map[string][20]byte, len(archive))
	for _, f := range archive {
		content := fileContent(f.name)
		h := seeders[f.seederID].SeedFile(f.name, content, f.domain, f.fromStep, f.toStep)
		expectedHashes[f.name] = h
	}

	// Every seeder publishes its V2 manifest + ENR so the leecher can
	// find its files via manifest_exchange.
	for _, s := range seeders {
		v2 := s.PublishV2Manifest()
		_, btPort := s.LocalTorrentAddr()
		s.SetDevP2PENREntry(enr.ChainToml{
			InfoHash:    v2,
			DomainSteps: 512,
			MergeDepth:  512,
		})
		s.SetDevP2PENREntry(enr.BT(btPort))
	}

	// Fresh leecher joins the swarm. Add every seeder as a static peer
	// at both BT and DevP2P layers.
	leecher := harness.NewP2PNode(t, logger)
	for _, s := range seeders {
		leecher.AddSeederPeer(s)
	}

	// Observers for the phased-ordering check.
	var (
		manifestCount       atomic.Int32
		promotedCount       atomic.Int32
		stateReadyCount     atomic.Int32
		stateReqBeforeReady atomic.Int32
		blockReqBeforeReady atomic.Int32
		stateReqAfterReady  atomic.Int32
		blockReqAfterReady  atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.PeerManifestReceived) { manifestCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) { promotedCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.InitialStateReady) { stateReadyCount.Add(1) }))
	require.NoError(t, leecher.Bus.Subscribe(func(e flow.DownloadRequested) {
		if stateReadyCount.Load() == 0 {
			if e.Domain != "" {
				stateReqBeforeReady.Add(1)
			} else {
				blockReqBeforeReady.Add(1)
			}
		} else {
			if e.Domain != "" {
				stateReqAfterReady.Add(1)
			} else {
				blockReqAfterReady.Add(1)
			}
		}
	}))

	// Kick off real DevP2P handshakes with every seeder. The
	// manifest_exchange component fetches each peer's chain.toml.v2,
	// the orchestrator gap-fills state files first, then unblocks
	// blocks after InitialStateReady.
	for _, s := range seeders {
		leecher.AddDevP2PPeer(s.DevP2PSelf())
	}

	// Wait for the full archive to land. Every file in the target
	// archive must promote on the leecher.
	expectedTotal := int32(len(archive))
	waitForP2P(t, func() bool {
		return promotedCount.Load() >= expectedTotal &&
			leecher.Orch.PendingCount() == 0
	}, 90*time.Second, "complete archive promoted on leecher")

	// InitialStateReady fired exactly once.
	require.Equal(t, int32(1), stateReadyCount.Load(),
		"InitialStateReady must fire exactly once")

	// Phased-ordering check: every block DownloadRequested must happen
	// AFTER InitialStateReady. Before state-ready: only state requests
	// allowed.
	require.Zero(t, blockReqBeforeReady.Load(),
		"no block DownloadRequested may fire before InitialStateReady")
	require.Zero(t, stateReqAfterReady.Load(),
		"no new state DownloadRequested may fire after InitialStateReady")
	require.Greater(t, stateReqBeforeReady.Load(), int32(0),
		"at least one state DownloadRequested before InitialStateReady")
	require.Greater(t, blockReqAfterReady.Load(), int32(0),
		"at least one block DownloadRequested after InitialStateReady")

	// Every seeder's manifest was observed.
	require.GreaterOrEqual(t, manifestCount.Load(), int32(numSeeders),
		"every seeder's manifest must have been fetched")

	// Every file in the archive is on the leecher with matching bytes.
	for _, f := range archive {
		want := fileContent(f.name)
		gotPath := filepath.Join(leecher.Dirs.Snap, f.name)
		got, err := os.ReadFile(gotPath)
		require.NoError(t, err, "leecher missing file %s", f.name)
		require.Equal(t, sha256.Sum256(want), sha256.Sum256(got),
			"content mismatch for %s", f.name)
	}

	// Inventory reflects every archive file at TrustVerified with the
	// expected torrent hash.
	for _, f := range archive {
		var entry *snapshot.FileEntry
		if f.domain != "" {
			for _, e := range leecher.Inventory.LocalFiles(f.domain) {
				if e.Name == f.name {
					entry = e
					break
				}
			}
		} else {
			for _, e := range leecher.Inventory.BlockFiles() {
				if e.Name == f.name && e.Local {
					entry = e
					break
				}
			}
		}
		require.NotNil(t, entry, "inventory missing entry for %s", f.name)
		require.Equal(t, expectedHashes[f.name], entry.TorrentHash,
			"inventory torrent hash mismatch for %s", f.name)
		require.Equal(t, snapshot.TrustVerified, entry.Trust,
			"inventory trust level for %s", f.name)
	}

	// Per-domain coverage check: each of the four state domains has
	// [0, 512) at TrustVerified.
	for _, d := range []snapshot.Domain{
		snapshot.DomainAccounts, snapshot.DomainStorage,
		snapshot.DomainCode, snapshot.DomainCommitment,
	} {
		cov := leecher.Inventory.CoverageAtTrust(d, snapshot.TrustVerified)
		require.Equal(t, snapshot.StepRanges{{From: 0, To: 512}}, cov,
			"domain %s coverage mismatch", d)
	}
}
