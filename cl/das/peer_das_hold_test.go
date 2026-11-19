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

package das

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

const (
	// wantHold repeats blobRecoveryHold on purpose, so the expected value does not follow the code.
	wantHold = time.Second
	// holdBlobs is the smallest blob count for which a recovery holds for the column sync.
	holdBlobs = 9
	// firstAttemptAhead keeps the first by-root attempt of a queued root out of reach unless a test moves it.
	firstAttemptAhead = time.Hour
)

// queueForColumnSync queues the root of the recovery with its first by-root attempt at firstAttempt.
func (g *gossipRecovery) queueForColumnSync(firstAttempt time.Time) {
	g.d.blocksToCheckSync.add(g.root, g.block, firstAttempt)
}

func (g *gossipRecovery) awaitHoldStart(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		g.d.blocksToCheckSync.mu.Lock()
		defer g.d.blocksToCheckSync.mu.Unlock()
		entry := g.d.blocksToCheckSync.entries[g.root]
		return entry != nil && entry.prioritised
	}, 5*time.Second, time.Millisecond, "the recovery did not hold for the column sync")
}

func (g *gossipRecovery) requireColumnSyncUntouched(t *testing.T, firstAttempt time.Time) {
	t.Helper()
	if !queued(&g.d.blocksToCheckSync, g.root) {
		return
	}
	require.Equal(t, firstAttempt, nextAttempt(&g.d.blocksToCheckSync, g.root), "the first attempt of the root")
	g.d.blocksToCheckSync.mu.Lock()
	defer g.d.blocksToCheckSync.mu.Unlock()
	require.False(t, g.d.blocksToCheckSync.entries[g.root].prioritised)
	require.Empty(t, g.d.blocksToCheckSync.wake)
}

// A supernode that misses columns of a block with many blobs gives the by-root sync a hold before it starts FK20: the
// first by-root attempt is due at the decision time, the worker is woken, and no column is read until the hold ends.
func TestBlobRecoveryHoldsForTheColumnSyncBeforeReadingColumns(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	firstAttempt := time.Now().Add(firstAttemptAhead)
	g.queueForColumnSync(firstAttempt)

	call := g.schedule(t)
	started := g.awaitFirstRead(t)

	require.GreaterOrEqual(t, started.Sub(call.begin), wantQuietPeriod+wantHold, "the worker read columns during the hold")
	decision := nextAttempt(&g.d.blocksToCheckSync, g.root)
	require.False(t, decision.Before(call.begin.Add(wantQuietPeriod)), "the first attempt moved before the decision")
	require.GreaterOrEqual(t, started.Sub(decision), wantHold)
	require.Len(t, g.d.blocksToCheckSync.wake, 1)
}

// A queued Gloas root is held like a Fulu one.
func TestBlobRecoveryHoldsForTheColumnSyncFromGloas(t *testing.T) {
	slotEpoch := recoveryTestSlot / clparams.MainnetBeaconConfig.SlotsPerEpoch
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs, func(r *recoveryRun) { forkToGloasAt(t, r, slotEpoch) })
	block := cltypes.NewSignedBeaconBlock(g.d.beaconConfig, clparams.GloasVersion)
	for range holdBlobs {
		block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	}
	g.d.blocksToCheckSync.add(g.root, block, time.Now().Add(firstAttemptAhead))

	call := g.schedule(t)
	started := g.awaitFirstRead(t)

	require.GreaterOrEqual(t, started.Sub(call.begin), wantQuietPeriod+wantHold, "the worker read columns during the hold")
	decision := nextAttempt(&g.d.blocksToCheckSync, g.root)
	require.False(t, decision.Before(call.begin.Add(wantQuietPeriod)), "the first attempt moved before the decision")
	require.GreaterOrEqual(t, started.Sub(decision), wantHold)
	require.Len(t, g.d.blocksToCheckSync.wake, 1)
}

// When nothing arrives during the hold, the recovery reads the stored columns once and recovers the missing ones once.
func TestBlobRecoveryRunsFK20OnceAfterTheHold(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	g.queueForColumnSync(time.Now().Add(firstAttemptAhead))
	stored := len(g.stored)

	scheduled := g.schedule(t)
	started := g.awaitFirstRead(t)
	call := receiveRecoveredColumnsPublish(t, g.published)

	require.GreaterOrEqual(t, started.Sub(scheduled.begin), wantQuietPeriod+wantHold, "the worker read columns during the hold")
	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireBlobsRecovered(t)
	missing := publishedColumnIndices(g.unstoredColumns())
	require.ElementsMatch(t, missing, publishedColumnIndices(call.sidecars))
	require.Equal(t, int32(len(missing)), g.workerWrites.writes.Load(), "column writes by the worker")
	require.Equal(t, int32(stored), g.columnReads.reads.Load(), "column reads by the worker")
}

// Columns that complete the matrix during the hold are found by the recovery after it: it writes the blobs and no
// column.
func TestBlobRecoveryFindsCompleteMatrixWhenColumnsArriveDuringTheHold(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	g.queueForColumnSync(time.Now().Add(firstAttemptAhead))
	g.schedule(t)
	g.awaitHoldStart(t)

	g.storeColumnsUpTo(t, int(g.d.beaconConfig.NumberOfColumns), func() { g.schedule(t) })
	g.awaitFirstRead(t)

	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireBlobsRecovered(t)
	require.Zero(t, g.workerWrites.writes.Load(), "column writes by the worker")
	require.Equal(t, int32(g.d.beaconConfig.NumberOfColumns), g.columnReads.reads.Load(), "column reads by the worker")
	require.Never(t, func() bool { return len(g.published) > 0 }, 100*time.Millisecond, 10*time.Millisecond, "published columns")
}

// The deadline of the recovery caps the hold.
func TestBlobRecoveryHoldEndsAtTheRecoveryDeadline(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	g.queueForColumnSync(time.Now().Add(firstAttemptAhead))
	fuluDeadline := time.Duration(clparams.MainnetBeaconConfig.SecondsPerSlot) * time.Second / 4
	deadline := time.Now().Add(600 * time.Millisecond)
	g.slotStart.Store(deadline.Add(-fuluDeadline).UnixNano())

	g.schedule(t)
	started := g.awaitFirstRead(t)

	require.False(t, started.Before(deadline), "the worker read columns before the deadline")
	require.Less(t, started.Sub(deadline), startSlack)
	g.awaitHoldStart(t)
}

// A column stored during the hold does not start a second hold.
func TestBlobRecoveryHoldsOnceWhenAColumnArrivesDuringTheHold(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	g.queueForColumnSync(time.Now().Add(firstAttemptAhead))
	first := g.schedule(t)
	g.awaitHoldStart(t)

	g.storeColumnsUpTo(t, len(g.stored)+1, func() { g.schedule(t) })
	started := g.awaitFirstRead(t)

	require.GreaterOrEqual(t, started.Sub(first.begin), wantQuietPeriod+wantHold)
	require.Less(t, started.Sub(first.end), wantQuietPeriod+wantHold+startSlack, "the worker held a second time")
}

// The hold belongs to the queued root: a later recovery of the same root, here after another path stored the blobs and
// then lost them, starts without one.
func TestBlobRecoveryHoldsOncePerQueuedRootAcrossRecoveries(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	g.queueForColumnSync(time.Now().Add(firstAttemptAhead))
	g.schedule(t)
	g.awaitHoldStart(t)
	require.NoError(t, g.d.blobStorage.WriteBlobSidecars(t.Context(), g.root, g.blobs))
	requireBlobRecoveryReleased(t, g.d, g.root)
	require.Empty(t, g.firstRead, "the first recovery read columns")
	require.NoError(t, g.d.blobStorage.RemoveBlobSidecars(t.Context(), recoveryTestSlot, g.root))

	second := g.schedule(t)
	started := g.awaitFirstRead(t)

	require.Less(t, started.Sub(second.end), wantQuietPeriod+startSlack, "the second recovery held")
}

// The recovery goes on at once when the hold has passed by the time the queue is updated.
func TestBlobRecoveryContinuesWhenTheHoldExpiredBeforeItParks(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	const queueUpdateDelay = 400 * time.Millisecond
	g.d.blocksToCheckSync.add(g.root, &slowCommitmentsBlock{ColumnSyncableSignedBlock: g.block, delay: queueUpdateDelay}, time.Now().Add(firstAttemptAhead))
	fuluDeadline := time.Duration(clparams.MainnetBeaconConfig.SecondsPerSlot) * time.Second / 4
	scheduled := time.Now()
	g.slotStart.Store(scheduled.Add(450*time.Millisecond - fuluDeadline).UnixNano())

	g.schedule(t)
	started := g.awaitFirstRead(t)

	g.awaitHoldStart(t)
	require.Len(t, g.d.blocksToCheckSync.wake, 1)
	require.GreaterOrEqual(t, started.Sub(scheduled), wantQuietPeriod+queueUpdateDelay)
	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireBlobsRecovered(t)
}

// slowCommitmentsBlock stands for a queue update that takes a while.
type slowCommitmentsBlock struct {
	cltypes.ColumnSyncableSignedBlock
	delay time.Duration
}

func (b *slowCommitmentsBlock) GetBlobKzgCommitments() *solid.ListSSZ[*cltypes.KZGCommitment] {
	time.Sleep(b.delay)
	return b.ColumnSyncableSignedBlock.GetBlobKzgCommitments()
}

// Unless every condition holds, the recovery starts after the quiet period and leaves the deferred column sync alone.
func TestBlobRecoveryDoesNotHoldWithoutTheConditions(t *testing.T) {
	archive := func(r *recoveryRun) { r.d.caplinConfig = &clparams.CaplinConfig{ArchiveBlobs: true} }
	for _, tc := range []struct {
		name      string
		blobs     int
		queued    bool
		configure []func(*recoveryRun)
		slotAge   time.Duration
		allStored bool
	}{
		{name: "eight blobs", blobs: holdBlobs - 1, queued: true},
		{name: "root not queued", blobs: holdBlobs},
		{name: "archive node", blobs: holdBlobs, queued: true, configure: []func(*recoveryRun){archive}},
		{name: "every column stored", blobs: holdBlobs, queued: true, allStored: true},
		{name: "deadline passed", blobs: holdBlobs, queued: true, slotAge: time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := startGossipRecoveryWithBlobs(t, false, tc.blobs, tc.configure...)
			firstAttempt := time.Now().Add(firstAttemptAhead)
			if tc.queued {
				g.queueForColumnSync(firstAttempt)
			}
			if tc.slotAge > 0 {
				g.slotStart.Store(time.Now().Add(-tc.slotAge).UnixNano())
			}
			if tc.allStored {
				g.storeColumnsUpTo(t, int(g.d.beaconConfig.NumberOfColumns), nil)
			}

			call := g.schedule(t)
			started := g.awaitFirstRead(t)

			require.Less(t, started.Sub(call.end), wantQuietPeriod+startSlack, "the worker held")
			g.requireColumnSyncUntouched(t, firstAttempt)
		})
	}
}

// A request with metadata starts at once, also for a queued root of many blobs.
func TestBlobRecoveryWithMetadataIsNotHeldForTheColumnSync(t *testing.T) {
	g := startGossipRecoveryWithBlobs(t, false, holdBlobs)
	firstAttempt := time.Now().Add(firstAttemptAhead)
	g.queueForColumnSync(firstAttempt)

	scheduled := time.Now()
	require.NoError(t, g.d.tryScheduleRecover(recoveryTestSlot, g.root, g.metadata))
	started := g.awaitFirstRead(t)

	require.Less(t, started.Sub(scheduled), 100*time.Millisecond)
	g.requireColumnSyncUntouched(t, firstAttempt)
}
