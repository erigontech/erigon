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

// Latest-download + backfill scenario — the gate-blocking shape from
// snapshot-flow-merge-gate.md. A multi-step archive on a single
// seeder, fresh leecher, exercises:
//
//   - Phase 0 — latest-state download. Per-domain DownloadRequested
//     events fire in latest-step-first order (the orchestrator's
//     ToStep-descending sort).
//   - Phase 0 → Phase 2 — InitialStateReady fires once after all
//     state files complete; only THEN do block DownloadRequested
//     events drain.
//   - Phase 1 — backfill. Older step ranges populate after the
//     latest range; every file at every range eventually promotes
//     to TrustVerified.
//
// What this scenario does NOT yet assert: cross-file consistency
// (commitment chain across .kv generations, history/kv alignment).
// Those are stage-2 validators that need the storage-integration's
// View interface to land. Per
// `inventory-visible-set-views.md`'s Phase-1 simplification, this
// test covers the inventory-only correctness scope; stage-2
// extensions slot in alongside without restructuring the test.

package scenarios_test

import (
	"fmt"
	"os"
	"sync"
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

// latestBackfillArchive returns a multi-step archive: three
// contiguous state ranges + matching block ranges. The seeder holds
// every file; the leecher starts empty.
//
// Three state domains × 2 roles (.kv + .kvi) per range × 3 ranges
// = 18 state files. Three block ranges × headers + bodies = 6 block
// files. Total: 24 files.
//
// Step ranges are 256-aligned (0-256, 256-512, 512-768) so the
// per-range gap-fill batches each generate 6 state DownloadRequested
// events — enough to make the latest-first sort observable but
// small enough to run in seconds.
func latestBackfillArchive() []archiveFile {
	stateRanges := []struct{ from, to uint64 }{
		{0, 256}, {256, 512}, {512, 768},
	}
	stateDomains := []snapshot.Domain{
		snapshot.DomainAccounts, snapshot.DomainStorage, snapshot.DomainCommitment,
	}
	var out []archiveFile
	for _, r := range stateRanges {
		for _, d := range stateDomains {
			for _, ext := range []string{"kv", "kvi"} {
				out = append(out, archiveFile{
					name:     fmt.Sprintf("v1.0-%s.%d-%d.%s", d, r.from, r.to, ext),
					domain:   d,
					fromStep: r.from,
					toStep:   r.to,
					seederID: 0,
				})
			}
		}
	}
	blockRanges := []struct{ from, to uint64 }{
		{0, 500}, {500, 1000}, {1000, 1500},
	}
	for _, r := range blockRanges {
		for _, kind := range []string{"headers", "bodies"} {
			out = append(out, archiveFile{
				name:     fmt.Sprintf("v1.0-%06d-%06d-%s.seg", r.from, r.to, kind),
				fromStep: r.from,
				toStep:   r.to,
				seederID: 0,
			})
		}
	}
	return out
}

// requestRecord is one DownloadRequested observation, retained in
// arrival order so latest-first ordering can be asserted post-hoc.
type requestRecord struct {
	domain   snapshot.Domain
	name     string
	fromStep uint64
	toStep   uint64
}

// TestP2P_LatestDownloadBackfill is the gate-blocking demonstration
// (per `snapshot-flow-merge-gate.md`) that the snapshot-flow
// architecture supports continuous validated publication via Phase
// 0 (latest-state download) + Phase 1 (older-range backfill) without
// the world-stop ceremony today's `erigon seg integrity` requires.
func TestP2P_LatestDownloadBackfill(t *testing.T) {
	logger := log.New()
	logger.SetHandler(log.StreamHandler(os.Stderr, log.TerminalFormat()))

	archive := latestBackfillArchive()

	// Single seeder holds the whole archive; that keeps the test
	// focused on the multi-step ordering rather than swarm
	// coordination (which p2p_swarm_test covers).
	seeder := harness.NewP2PNode(t, logger)
	for _, f := range archive {
		content := fileContent(f.name)
		seeder.SeedFile(f.name, content, f.domain, f.fromStep, f.toStep)
	}

	v2 := seeder.PublishV2Manifest()
	_, btPort := seeder.LocalTorrentAddr()
	seeder.SetDevP2PENREntry(enr.ChainToml{
		InfoHash:    v2,
		DomainSteps: 768,
		MergeDepth:  768,
	})
	seeder.SetDevP2PENREntry(enr.BT(btPort))

	leecher := harness.NewP2PNode(t, logger)
	leecher.AddSeederPeer(seeder)

	var (
		stateReadyCount atomic.Int32
		promotedCount   atomic.Int32
		stateReqs       []requestRecord
		blockReqs       []requestRecord
		reqMu           sync.Mutex

		stateBeforeReady atomic.Int32
		blockBeforeReady atomic.Int32
	)
	require.NoError(t, leecher.Bus.Subscribe(func(flow.InitialStateReady) {
		stateReadyCount.Add(1)
	}))
	require.NoError(t, leecher.Bus.Subscribe(func(flow.TrustPromoted) {
		promotedCount.Add(1)
	}))
	require.NoError(t, leecher.Bus.Subscribe(func(e flow.DownloadRequested) {
		rec := requestRecord{
			domain:   e.Domain,
			name:     e.FileName,
			fromStep: e.Range.From,
			toStep:   e.Range.To,
		}
		reqMu.Lock()
		if e.Domain != "" {
			stateReqs = append(stateReqs, rec)
			if stateReadyCount.Load() == 0 {
				stateBeforeReady.Add(1)
			}
		} else {
			blockReqs = append(blockReqs, rec)
			if stateReadyCount.Load() == 0 {
				blockBeforeReady.Add(1)
			}
		}
		reqMu.Unlock()
	}))

	leecher.AddDevP2PPeer(seeder.DevP2PSelf())

	expectedTotal := int32(len(archive))
	waitForP2P(t, func() bool {
		return promotedCount.Load() >= expectedTotal &&
			leecher.Orch.PendingCount() == 0
	}, 90*time.Second, "all archive files promoted on leecher")

	// --- Assertions ---

	// 1. InitialStateReady fired exactly once.
	require.Equal(t, int32(1), stateReadyCount.Load(),
		"InitialStateReady must fire exactly once across the run")

	// 2. Phase ordering: blocks NEVER request before InitialStateReady.
	require.Zero(t, blockBeforeReady.Load(),
		"no block DownloadRequested may fire before InitialStateReady — phase gate runs one way")
	require.Greater(t, stateBeforeReady.Load(), int32(0),
		"at least one state DownloadRequested must precede InitialStateReady")

	// 3. Latest-first ordering PER DOMAIN. The orchestrator's
	//    requestGapsFor sorts each domain's batch by ToStep descending,
	//    so for any domain, the first request observed must be at the
	//    highest ToStep that domain advertised.
	reqMu.Lock()
	stateByDomain := make(map[snapshot.Domain][]requestRecord)
	for _, r := range stateReqs {
		stateByDomain[r.domain] = append(stateByDomain[r.domain], r)
	}
	reqMu.Unlock()

	for d, recs := range stateByDomain {
		require.NotEmpty(t, recs, "domain %s must have at least one state request", d)

		// Highest ToStep across all this domain's recorded requests
		// is what we EXPECT to appear first.
		var maxToStep uint64
		for _, r := range recs {
			if r.toStep > maxToStep {
				maxToStep = r.toStep
			}
		}
		require.Equal(t, maxToStep, recs[0].toStep,
			"domain %s: first DownloadRequested must be the highest-ToStep file (latest-first sort); got %s (toStep=%d), want toStep=%d",
			d, recs[0].name, recs[0].toStep, maxToStep)

		// Defensive: the LAST request for this domain must have a
		// ToStep <= the first. The sort is stable, so weakly
		// monotonic.
		require.LessOrEqual(t, recs[len(recs)-1].toStep, recs[0].toStep,
			"domain %s: per-domain request order must be ToStep-descending", d)
	}

	// 4. Backfill complete. Every range's full file set is promoted —
	//    no gaps in the inventory at any range. Sample the leecher's
	//    inventory at end-of-run to confirm.
	leecherInv := leecher.Inventory
	for _, f := range archive {
		var present []*snapshot.FileEntry
		if f.domain != "" {
			present = leecherInv.LocalFiles(f.domain)
		} else {
			present = leecherInv.BlockFiles()
		}
		var found bool
		for _, p := range present {
			if p.Name == f.name && p.Trust == snapshot.TrustVerified {
				found = true
				break
			}
		}
		require.True(t, found,
			"backfill check: file %s (domain=%q range=[%d,%d)) must be in leecher inventory at TrustVerified",
			f.name, f.domain, f.fromStep, f.toStep)
	}

	t.Logf("latest+backfill complete: %d files promoted, InitialStateReady fired %d time(s), state requests=%d, block requests=%d",
		promotedCount.Load(), stateReadyCount.Load(), len(stateReqs), len(blockReqs))
}
