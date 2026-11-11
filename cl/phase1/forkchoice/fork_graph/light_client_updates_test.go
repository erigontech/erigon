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

// An update signed in the next period is not a candidate for the period of its attested header.
func TestLightClientUpdateCandidatesSkipOtherPeriods(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	periodStart := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod

	f.recordLightClientUpdateLocked(common.Hash{1}, lightClientTestUpdate(cfg, 400, periodStart+5))
	f.recordLightClientUpdateLocked(common.Hash{2}, lightClientTestUpdate(cfg, int(cfg.SyncCommitteeSize), 2*periodStart-1))

	require.Len(t, f.lightClientUpdateCandidates, 1)
	require.Contains(t, f.lightClientUpdateCandidates, common.Hash{1})
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
	_, ok := f.GetLightClientUpdate(1, common.Hash{})
	require.False(t, ok)
	require.Len(t, f.lightClientUpdateCandidates, 1)
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
	f.recordLightClientUpdateLocked(canonical, canonicalUpdate)
	f.recordLightClientUpdateLocked(orphan, lightClientTestUpdate(cfg, 450, periodStart+10))
	// Signed after the finalized block, so finalization cannot settle it yet.
	later, nextFinalized := common.Hash{5}, common.Hash{6}
	f.headers.Store(later, &cltypes.BeaconBlockHeader{Slot: periodStart + 70, ParentRoot: finalized})
	f.headers.Store(nextFinalized, &cltypes.BeaconBlockHeader{Slot: periodStart + 96, ParentRoot: later})
	laterUpdate := lightClientTestUpdate(cfg, 500, periodStart+69)
	f.recordLightClientUpdateLocked(later, laterUpdate)
	_, ok := f.GetLightClientUpdate(1, common.Hash{})
	require.False(t, ok, "settled before finalization")

	f.KeepFinalizedLightClientUpdates(finalized)

	got, ok := f.GetLightClientUpdate(1, common.Hash{})
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got)
	require.Len(t, f.lightClientUpdateCandidates, 1, "only the candidate above the finalized slot waits")

	f.KeepFinalizedLightClientUpdates(nextFinalized)

	got, ok = f.GetLightClientUpdate(1, common.Hash{})
	require.True(t, ok)
	require.Same(t, laterUpdate, got, "the next finalization settles it")
	require.Empty(t, f.lightClientUpdateCandidates)
}

// Better updates on a branch that loses fork choice do not push the canonical candidate out before finalization.
func TestFinalizationKeepsTheCanonicalCandidateAmongManyBetterSideBranchOnes(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	periodStart := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	attested, canonical, finalized := common.Hash{1}, common.Hash{2}, common.Hash{3}
	f.headers.Store(attested, &cltypes.BeaconBlockHeader{Slot: periodStart + 10})
	f.headers.Store(canonical, &cltypes.BeaconBlockHeader{Slot: periodStart + 11, ParentRoot: attested})
	f.headers.Store(finalized, &cltypes.BeaconBlockHeader{Slot: periodStart + 64, ParentRoot: canonical})

	canonicalUpdate := lightClientTestUpdate(cfg, 400, periodStart+10)
	f.recordLightClientUpdateLocked(canonical, canonicalUpdate)
	for i := range 20 {
		f.recordLightClientUpdateLocked(common.Hash{0xee, byte(i)}, lightClientTestUpdate(cfg, 450, periodStart+10))
	}
	f.KeepFinalizedLightClientUpdates(finalized)

	got, ok := f.GetLightClientUpdate(1, common.Hash{})
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got)
}

// A canonical candidate that arrives after many side-branch ones, more than a period has slots, is still kept and wins
// at finalization.
func TestFinalizationKeepsACanonicalCandidateThatArrivesAfterManySideBranchOnes(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch, cfg.EpochsPerSyncCommitteePeriod = 4, 8
	f := &forkGraphDisk{beaconCfg: &cfg}
	attested, canonical, finalized := common.Hash{1}, common.Hash{2}, common.Hash{3}
	f.headers.Store(attested, &cltypes.BeaconBlockHeader{Slot: 40})
	f.headers.Store(canonical, &cltypes.BeaconBlockHeader{Slot: 41, ParentRoot: attested})
	f.headers.Store(finalized, &cltypes.BeaconBlockHeader{Slot: 50, ParentRoot: canonical})
	for i := range 40 {
		f.recordLightClientUpdateLocked(common.Hash{0xee, byte(i)}, lightClientTestUpdate(&cfg, 450, 40))
	}
	canonicalUpdate := lightClientTestUpdate(&cfg, 400, 40)
	f.recordLightClientUpdateLocked(canonical, canonicalUpdate)

	f.KeepFinalizedLightClientUpdates(finalized)

	got, ok := f.GetLightClientUpdate(1, common.Hash{})
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got)
}

// A period spans several finalization steps: a candidate above the first finalized slot waits, and the period keeps
// the better of the two canonical updates, whether the earlier or the later one is better.
func TestFinalizationKeepsTheBestCanonicalUpdateAcrossSteps(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		early, late, wantIdx int
	}{{"later is better", 400, 450, 1}, {"earlier is better", 450, 400, 0}} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &clparams.MainnetBeaconConfig
			f := &forkGraphDisk{beaconCfg: cfg}
			p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
			a, b, c, fin1, fin2 := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}, common.Hash{5}
			f.headers.Store(a, &cltypes.BeaconBlockHeader{Slot: p + 10})
			f.headers.Store(b, &cltypes.BeaconBlockHeader{Slot: p + 11, ParentRoot: a})
			f.headers.Store(fin1, &cltypes.BeaconBlockHeader{Slot: p + 32, ParentRoot: b})
			f.headers.Store(c, &cltypes.BeaconBlockHeader{Slot: p + 41, ParentRoot: fin1})
			f.headers.Store(fin2, &cltypes.BeaconBlockHeader{Slot: p + 64, ParentRoot: c})
			updates := []*cltypes.LightClientUpdate{lightClientTestUpdate(cfg, tt.early, p+10), lightClientTestUpdate(cfg, tt.late, p+40)}
			f.recordLightClientUpdateLocked(b, updates[0])
			f.recordLightClientUpdateLocked(c, updates[1])

			f.KeepFinalizedLightClientUpdates(fin1)
			got, ok := f.GetLightClientUpdate(1, common.Hash{})
			require.True(t, ok)
			require.Same(t, updates[0], got)
			f.KeepFinalizedLightClientUpdates(fin2)
			got, _ = f.GetLightClientUpdate(1, common.Hash{})
			require.Same(t, updates[tt.wantIdx], got)
		})
	}
}

// Before finalization settles a period, the best candidate on the head's chain is served for it, and a head change
// re-evaluates it; a better update on another branch is not served.
func TestGetLightClientUpdateServesTheBestCandidateOnTheHeadChain(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	attested, canonical, side, head, sideHead := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}, common.Hash{5}
	f.headers.Store(attested, &cltypes.BeaconBlockHeader{Slot: p + 10})
	f.headers.Store(canonical, &cltypes.BeaconBlockHeader{Slot: p + 11, ParentRoot: attested})
	f.headers.Store(side, &cltypes.BeaconBlockHeader{Slot: p + 12, ParentRoot: attested})
	f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: p + 20, ParentRoot: canonical})
	f.headers.Store(sideHead, &cltypes.BeaconBlockHeader{Slot: p + 20, ParentRoot: side})
	canonicalUpdate, sideUpdate := lightClientTestUpdate(cfg, 400, p+10), lightClientTestUpdate(cfg, 450, p+10)
	sideUpdate.SignatureSlot = p + 12
	f.recordLightClientUpdateLocked(canonical, canonicalUpdate)
	f.recordLightClientUpdateLocked(side, sideUpdate)

	got, ok := f.GetLightClientUpdate(1, head)
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got)
	got, ok = f.GetLightClientUpdate(1, sideHead)
	require.True(t, ok)
	require.Same(t, sideUpdate, got)
	_, ok = f.GetLightClientUpdate(2, head)
	require.False(t, ok, "no update of another period")

	next := common.Hash{6}
	f.headers.Store(next, &cltypes.BeaconBlockHeader{Slot: 2*p + 6, ParentRoot: head})
	f.recordLightClientUpdateLocked(next, lightClientTestUpdate(cfg, int(cfg.SyncCommitteeSize), 2*p+5))
	got, ok = f.GetLightClientUpdate(1, next)
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got, "a candidate of the next period on the way down is not one of this period")
}

// While finalization has settled part of a period, the period serves the better of the settled update and the best
// candidate on the head's chain above the finalized slot.
func TestGetLightClientUpdateComparesTheSettledUpdateWithTheHeadChain(t *testing.T) {
	for _, tt := range []struct {
		name               string
		settled, unsettled int
		wantUnsettled      bool
	}{{"unsettled is better", 400, 450, true}, {"settled is better", 450, 400, false}} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &clparams.MainnetBeaconConfig
			f := &forkGraphDisk{beaconCfg: cfg}
			p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
			a, b, fin, c, head := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}, common.Hash{5}
			f.headers.Store(a, &cltypes.BeaconBlockHeader{Slot: p + 10})
			f.headers.Store(b, &cltypes.BeaconBlockHeader{Slot: p + 11, ParentRoot: a})
			f.headers.Store(fin, &cltypes.BeaconBlockHeader{Slot: p + 32, ParentRoot: b})
			f.headers.Store(c, &cltypes.BeaconBlockHeader{Slot: p + 41, ParentRoot: fin})
			f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: p + 50, ParentRoot: c})
			settled, unsettled := lightClientTestUpdate(cfg, tt.settled, p+10), lightClientTestUpdate(cfg, tt.unsettled, p+40)
			f.recordLightClientUpdateLocked(b, settled)
			f.recordLightClientUpdateLocked(c, unsettled)
			f.KeepFinalizedLightClientUpdates(fin)

			got, ok := f.GetLightClientUpdate(1, head)
			require.True(t, ok)
			if tt.wantUnsettled {
				require.Same(t, unsettled, got)
			} else {
				require.Same(t, settled, got)
			}
		})
	}
}

// Among several candidates of a period on the head's chain, the best one is served, whether it is the older or the
// newer one.
func TestGetLightClientUpdateRanksTheCandidatesOnTheHeadChain(t *testing.T) {
	for _, tt := range []struct {
		name                 string
		early, late, wantIdx int
	}{{"later is better", 400, 450, 1}, {"earlier is better", 450, 400, 0}} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &clparams.MainnetBeaconConfig
			f := &forkGraphDisk{beaconCfg: cfg}
			p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
			a, b, c, head := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}
			f.headers.Store(a, &cltypes.BeaconBlockHeader{Slot: p + 10})
			f.headers.Store(b, &cltypes.BeaconBlockHeader{Slot: p + 11, ParentRoot: a})
			f.headers.Store(c, &cltypes.BeaconBlockHeader{Slot: p + 41, ParentRoot: b})
			f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: p + 50, ParentRoot: c})
			updates := []*cltypes.LightClientUpdate{lightClientTestUpdate(cfg, tt.early, p+10), lightClientTestUpdate(cfg, tt.late, p+40)}
			f.recordLightClientUpdateLocked(b, updates[0])
			f.recordLightClientUpdateLocked(c, updates[1])

			got, ok := f.GetLightClientUpdate(1, head)
			require.True(t, ok)
			require.Same(t, updates[tt.wantIdx], got)
		})
	}
}

// A lookup that runs while finalization moves the period's candidate into the period store still finds an update.
func TestGetLightClientUpdateDuringFinalization(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	attested, canonical, finalized := common.Hash{1}, common.Hash{2}, common.Hash{3}
	for range 2000 {
		f := &forkGraphDisk{beaconCfg: cfg}
		f.headers.Store(attested, &cltypes.BeaconBlockHeader{Slot: p + 10})
		f.headers.Store(canonical, &cltypes.BeaconBlockHeader{Slot: p + 11, ParentRoot: attested})
		f.headers.Store(finalized, &cltypes.BeaconBlockHeader{Slot: p + 64, ParentRoot: canonical})
		f.recordLightClientUpdateLocked(canonical, lightClientTestUpdate(cfg, 400, p+10))

		done := make(chan struct{})
		go func() {
			defer close(done)
			f.KeepFinalizedLightClientUpdates(finalized)
		}()
		for {
			_, ok := f.GetLightClientUpdate(1, finalized)
			require.True(t, ok)
			select {
			case <-done:
			default:
				continue
			}
			break
		}
	}
}
