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

package fork_graph

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// A period ranks its candidates best first, and an update signed in the next period is not a candidate for the period
// of its attested header.
func TestLightClientUpdateCandidatesRankAndSkipOtherPeriods(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	periodStart := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod

	f.addLightClientUpdateCandidateLocked(common.Hash{1}, lightClientTestUpdate(cfg, 10, periodStart+1))
	best := lightClientTestUpdate(cfg, 400, periodStart+5)
	f.addLightClientUpdateCandidateLocked(common.Hash{2}, best)
	f.addLightClientUpdateCandidateLocked(common.Hash{3}, lightClientTestUpdate(cfg, 300, periodStart+7))
	f.addLightClientUpdateCandidateLocked(common.Hash{4}, lightClientTestUpdate(cfg, int(cfg.SyncCommitteeSize), 2*periodStart-1))

	candidates := f.lightClientUpdateCandidates[1]
	require.Len(t, candidates, 3)
	require.Same(t, best, candidates[0].update)
	require.Equal(t, []common.Hash{{2}, {3}, {1}}, []common.Hash{candidates[0].signatureBlock, candidates[1].signatureBlock, candidates[2].signatureBlock})
}

// The update of a block that passed its transition is served at once as the newest update, but its period store waits
// for finalization.
func TestRecordLightClientUpdateWaitsForFinalizationForThePeriodStore(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	periodStart := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	update := lightClientTestUpdate(cfg, 400, periodStart+10)

	f.recordLightClientUpdateLocked(common.Hash{1}, update)

	require.Same(t, update, f.NewestLightClientUpdate())
	_, ok := f.GetLightClientUpdate(1)
	require.False(t, ok)
	require.Len(t, f.lightClientUpdateCandidates[1], 1)
}

func lightClientTestUpdate(cfg *clparams.BeaconChainConfig, participants int, attestedSlot uint64) *cltypes.LightClientUpdate {
	u := cltypes.NewLightClientUpdate(clparams.DenebVersion, cfg)
	for i := range participants {
		u.SyncAggregate.SyncCommiteeBits[i/8] |= 1 << (i % 8)
	}
	u.AttestedHeader.Beacon.Slot = attestedSlot
	u.SignatureSlot = attestedSlot + 1
	return u
}

// Only an update whose signature block is on the finalized chain reaches the period store: a better update of a
// sibling that lost fork choice is dropped, and nothing is served for the period before finalization reaches it.
func TestFinalizationKeepsOnlyCanonicalLightClientUpdates(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	periodStart := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	attested, canonical, orphan, finalized := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}
	f.headers.Store(attested, &cltypes.BeaconBlockHeader{Slot: periodStart + 10})
	f.headers.Store(canonical, &cltypes.BeaconBlockHeader{Slot: periodStart + 11, ParentRoot: attested})
	f.headers.Store(orphan, &cltypes.BeaconBlockHeader{Slot: periodStart + 11, ParentRoot: attested})
	f.headers.Store(finalized, &cltypes.BeaconBlockHeader{Slot: periodStart + 64, ParentRoot: canonical})

	canonicalUpdate := lightClientTestUpdate(cfg, 400, periodStart+10)
	f.addLightClientUpdateCandidateLocked(canonical, canonicalUpdate)
	f.addLightClientUpdateCandidateLocked(orphan, lightClientTestUpdate(cfg, 450, periodStart+10))
	// Signed after the finalized block, so finalization cannot settle it yet.
	later, nextFinalized := common.Hash{5}, common.Hash{6}
	f.headers.Store(later, &cltypes.BeaconBlockHeader{Slot: periodStart + 70, ParentRoot: finalized})
	f.headers.Store(nextFinalized, &cltypes.BeaconBlockHeader{Slot: periodStart + 96, ParentRoot: later})
	laterUpdate := lightClientTestUpdate(cfg, 500, periodStart+69)
	f.addLightClientUpdateCandidateLocked(later, laterUpdate)
	_, ok := f.GetLightClientUpdate(1)
	require.False(t, ok, "served before finalization")

	f.KeepFinalizedLightClientUpdates(finalized)

	got, ok := f.GetLightClientUpdate(1)
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got)
	require.Len(t, f.lightClientUpdateCandidates[1], 1, "only the candidate above the finalized slot waits")

	f.KeepFinalizedLightClientUpdates(nextFinalized)

	got, ok = f.GetLightClientUpdate(1)
	require.True(t, ok)
	require.Same(t, laterUpdate, got, "the next finalization settles it")
	require.Empty(t, f.lightClientUpdateCandidates)
}

// A period keeps a bounded number of candidates, the best ones, so a long stretch without finality does not hold a
// sync committee per block.
func TestLightClientUpdateCandidatesKeepTheBestOfAPeriod(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	periodStart := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	for i := range maxLightClientUpdateCandidates + 1 {
		f.addLightClientUpdateCandidateLocked(common.Hash{byte(i + 1)}, lightClientTestUpdate(cfg, 400+i, periodStart+uint64(i)))
	}
	candidates := f.lightClientUpdateCandidates[1]
	require.Len(t, candidates, maxLightClientUpdateCandidates)
	for _, c := range candidates {
		require.NotEqual(t, common.Hash{1}, c.signatureBlock, "the worst candidate is dropped")
	}
}
