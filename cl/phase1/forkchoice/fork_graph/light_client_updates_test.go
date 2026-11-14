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
	require.Len(t, f.lightClientUpdateCandidates[1], 1)
	require.Contains(t, f.lightClientUpdateCandidates[1], common.Hash{1})
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
	_, ok := lightClientUpdateAt(f, 1, common.Hash{})
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
	f.recordLightClientUpdateLocked(canonical, canonicalUpdate)
	f.recordLightClientUpdateLocked(orphan, lightClientTestUpdate(cfg, 450, periodStart+10))
	// Signed after the finalized block, so finalization cannot settle it yet.
	later, nextFinalized := common.Hash{5}, common.Hash{6}
	f.headers.Store(later, &cltypes.BeaconBlockHeader{Slot: periodStart + 70, ParentRoot: finalized})
	f.headers.Store(nextFinalized, &cltypes.BeaconBlockHeader{Slot: periodStart + 96, ParentRoot: later})
	laterUpdate := lightClientTestUpdate(cfg, 500, periodStart+69)
	f.recordLightClientUpdateLocked(later, laterUpdate)
	_, ok := lightClientUpdateAt(f, 1, common.Hash{})
	require.False(t, ok, "settled before finalization")

	f.KeepFinalizedLightClientUpdates(finalized)

	got, ok := lightClientUpdateAt(f, 1, common.Hash{})
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got)
	require.Len(t, f.lightClientUpdateCandidates[1], 1, "only the candidate above the finalized slot waits")

	f.KeepFinalizedLightClientUpdates(nextFinalized)

	got, ok = lightClientUpdateAt(f, 1, common.Hash{})
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

	got, ok := lightClientUpdateAt(f, 1, common.Hash{})
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

	got, ok := lightClientUpdateAt(f, 1, common.Hash{})
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
			got, ok := lightClientUpdateAt(f, 1, common.Hash{})
			require.True(t, ok)
			require.Same(t, updates[0], got)
			f.KeepFinalizedLightClientUpdates(fin2)
			got, _ = lightClientUpdateAt(f, 1, common.Hash{})
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

	got, ok := lightClientUpdateAt(f, 1, head)
	require.True(t, ok)
	require.Same(t, canonicalUpdate, got)
	got, ok = lightClientUpdateAt(f, 1, sideHead)
	require.True(t, ok)
	require.Same(t, sideUpdate, got)
	_, ok = lightClientUpdateAt(f, 2, head)
	require.False(t, ok, "no update of another period")

	next := common.Hash{6}
	f.headers.Store(next, &cltypes.BeaconBlockHeader{Slot: 2*p + 6, ParentRoot: head})
	f.recordLightClientUpdateLocked(next, lightClientTestUpdate(cfg, int(cfg.SyncCommitteeSize), 2*p+5))
	got, ok = lightClientUpdateAt(f, 1, next)
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

			got, ok := lightClientUpdateAt(f, 1, head)
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

			got, ok := lightClientUpdateAt(f, 1, head)
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
			_, ok := lightClientUpdateAt(f, 1, finalized)
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

// One lookup returns every period of the range against one head: settled updates, head-chain candidates of several
// periods, and nil for a period without either.
func TestGetLightClientUpdatesServesARangeAgainstOneHead(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	b1, b2, head := common.Hash{1}, common.Hash{2}, common.Hash{3}
	f.headers.Store(b1, &cltypes.BeaconBlockHeader{Slot: p + 11})
	f.headers.Store(b2, &cltypes.BeaconBlockHeader{Slot: 2*p + 11, ParentRoot: b1})
	f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: 4*p + 1, ParentRoot: b2})
	settled := lightClientTestUpdate(cfg, 400, 10)
	f.lightClientUpdates.Store(uint64(0), settled)
	first, second := lightClientTestUpdate(cfg, 400, p+10), lightClientTestUpdate(cfg, 400, 2*p+10)
	f.recordLightClientUpdateLocked(b1, first)
	f.recordLightClientUpdateLocked(b2, second)

	require.Equal(t, []*cltypes.LightClientUpdate{settled, first, second, nil}, f.GetLightClientUpdates(0, 4, head))
	require.Equal(t, []*cltypes.LightClientUpdate{first, second}, f.GetLightClientUpdates(1, 3, head))
	require.Equal(t, []*cltypes.LightClientUpdate{first}, f.GetLightClientUpdates(1, 2, head), "a candidate of a later period is not one of the range")
	require.Equal(t, []*cltypes.LightClientUpdate{settled, nil, nil, nil}, f.GetLightClientUpdates(0, 4, common.Hash{}))
	require.Nil(t, f.GetLightClientUpdates(2, 2, head))
}

// Block import records candidates while the Beacon API handlers look them up.
func TestLightClientUpdateCandidatesRecordAndLookupConcurrently(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	head := common.Hash{0xff}
	f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: p + 100})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			f.recordLightClientUpdateLocked(common.Hash{byte(i), 1}, lightClientTestUpdate(cfg, 400, p+uint64(i%50)))
		}
	}()
	for {
		f.GetLightClientUpdates(0, 3, head)
		select {
		case <-done:
			return
		default:
		}
	}
}

// A head read before finalization settled a competing branch is not on the finalized chain: its candidates are not
// served beside that branch's settled updates.
func TestGetLightClientUpdatesIgnoresAHeadOffTheFinalizedChain(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	fork, b1, b2, a1 := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}
	f.headers.Store(fork, &cltypes.BeaconBlockHeader{Slot: p + 5})
	f.headers.Store(b1, &cltypes.BeaconBlockHeader{Slot: p + 10, ParentRoot: fork})
	f.headers.Store(b2, &cltypes.BeaconBlockHeader{Slot: p + 40, ParentRoot: b1})
	f.headers.Store(a1, &cltypes.BeaconBlockHeader{Slot: p + 50, ParentRoot: fork})
	settled := lightClientTestUpdate(cfg, 400, p+9)
	f.recordLightClientUpdateLocked(b1, settled)
	f.recordLightClientUpdateLocked(a1, lightClientTestUpdate(cfg, int(cfg.SyncCommitteeSize), p+49))

	f.KeepFinalizedLightClientUpdates(b2)

	require.Equal(t, []*cltypes.LightClientUpdate{settled}, f.GetLightClientUpdates(1, 2, a1), "the stale head's candidate is not served")
	require.Equal(t, []*cltypes.LightClientUpdate{settled}, f.GetLightClientUpdates(1, 2, b2))
}

// Pruning can hide the finalized block's own header while the head still descends from it: the walk recognises the
// finalized root without loading its header.
func TestGetLightClientUpdatesServesCandidatesWhenTheFinalizedHeaderIsPruned(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	fin, c, head := common.Hash{1}, common.Hash{2}, common.Hash{3}
	f.headers.Store(fin, &cltypes.BeaconBlockHeader{Slot: p + 10})
	f.headers.Store(c, &cltypes.BeaconBlockHeader{Slot: p + 40, ParentRoot: fin})
	f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: p + 50, ParentRoot: c})
	candidate := lightClientTestUpdate(cfg, 400, p+39)
	f.recordLightClientUpdateLocked(c, candidate)
	f.KeepFinalizedLightClientUpdates(fin)

	f.lowestAvailableBlock.Store(p + 20)
	_, visible := f.GetHeader(fin)
	require.False(t, visible, "the finalized header is below the pruned boundary")

	require.Equal(t, []*cltypes.LightClientUpdate{candidate}, f.GetLightClientUpdates(1, 2, head))
}

// The walk goes on below the lowest candidate until it reaches the finalized block, so blocks without a candidate
// between them do not hide that the head is on the finalized chain.
func TestGetLightClientUpdatesServesCandidatesAboveBlocksWithoutOne(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	fin, x, c, head := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}
	f.headers.Store(fin, &cltypes.BeaconBlockHeader{Slot: p + 10})
	f.headers.Store(x, &cltypes.BeaconBlockHeader{Slot: p + 20, ParentRoot: fin})
	f.headers.Store(c, &cltypes.BeaconBlockHeader{Slot: p + 40, ParentRoot: x})
	f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: p + 50, ParentRoot: c})
	candidate := lightClientTestUpdate(cfg, 400, p+39)
	f.recordLightClientUpdateLocked(c, candidate)
	f.KeepFinalizedLightClientUpdates(fin)

	require.Equal(t, []*cltypes.LightClientUpdate{candidate}, f.GetLightClientUpdates(1, 2, head))
}

// lightClientUpdateAt returns the best update of one period, as a range lookup of that period does.
func lightClientUpdateAt(f *forkGraphDisk, period uint64, head common.Hash) (*cltypes.LightClientUpdate, bool) {
	update := f.GetLightClientUpdates(period, period+1, head)[0]
	return update, update != nil
}

// A stale head on a branch that conflicts with the finalized block serves no candidates, also when pruning hides the
// finalized header, so the walk ends at a hidden header of its own branch before it passes the finalized slot.
func TestGetLightClientUpdatesIgnoresAConflictingHeadWhenTheFinalizedHeaderIsPruned(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	fin, fork, b1, b2 := common.Hash{1}, common.Hash{2}, common.Hash{3}, common.Hash{4}
	f.headers.Store(fin, &cltypes.BeaconBlockHeader{Slot: p + 10})
	f.headers.Store(fork, &cltypes.BeaconBlockHeader{Slot: p + 2})
	f.headers.Store(b1, &cltypes.BeaconBlockHeader{Slot: p + 5, ParentRoot: fork})
	f.headers.Store(b2, &cltypes.BeaconBlockHeader{Slot: p + 50, ParentRoot: b1})
	settled := lightClientTestUpdate(cfg, 400, p+9)
	f.recordLightClientUpdateLocked(fin, settled)
	f.recordLightClientUpdateLocked(b2, lightClientTestUpdate(cfg, int(cfg.SyncCommitteeSize), p+49))
	f.KeepFinalizedLightClientUpdates(fin)
	f.lowestAvailableBlock.Store(p + 20)

	require.Equal(t, []*cltypes.LightClientUpdate{settled}, f.GetLightClientUpdates(1, 2, b2))
}

// Before any finalization is recorded, the candidates on the head's chain are served, also when the chain ends at an
// unknown parent, as the anchor's does.
func TestGetLightClientUpdatesServesCandidatesBeforeTheFirstFinalization(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	anchor, head := common.Hash{1}, common.Hash{2}
	f.headers.Store(anchor, &cltypes.BeaconBlockHeader{Slot: p + 10, ParentRoot: common.Hash{0xee}})
	f.headers.Store(head, &cltypes.BeaconBlockHeader{Slot: p + 20, ParentRoot: anchor})
	candidate := lightClientTestUpdate(cfg, 400, p+19)
	f.recordLightClientUpdateLocked(head, candidate)

	require.Equal(t, []*cltypes.LightClientUpdate{candidate}, f.GetLightClientUpdates(1, 2, head))
}

// The newest finality update is the last recorded update that has finality; a later update without it is only the
// newest update.
func TestNewestLightClientFinalityUpdateKeepsTheLastUpdateWithFinality(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	f := &forkGraphDisk{beaconCfg: cfg}
	p := cfg.SlotsPerEpoch * cfg.EpochsPerSyncCommitteePeriod
	withFinality := lightClientTestUpdate(cfg, 400, p+10)
	withFinality.FinalityBranch.Set(0, common.Hash{1})
	withoutFinality := lightClientTestUpdate(cfg, 400, p+20)

	f.recordLightClientUpdateLocked(common.Hash{1}, withFinality)
	f.recordLightClientUpdateLocked(common.Hash{2}, withoutFinality)

	require.Same(t, withoutFinality, f.NewestLightClientUpdate())
	require.Same(t, withFinality, f.NewestLightClientFinalityUpdate())
}
