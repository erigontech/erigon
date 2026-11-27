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
	"context"
	"maps"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func (r *recoveryRun) blobsFromColumns(indices []uint64, withSidecars bool) ([]*cltypes.Blob, []*cltypes.BlobSidecar, error) {
	return r.d.BlobsFromColumns(context.Background(), recoveryTestSlot, r.root, indices, withSidecars)
}

func TestBlobsFromColumnsMatchesTheEagerBlobSidecars(t *testing.T) {
	r := startRecoveryWithPublisher(t, recoveryOptions{caplinConfig: &clparams.CaplinConfig{}, supernode: true, unscheduled: true, blobs: 3})
	r.scheduleWithoutMetadata(t)
	requireBlobRecoveryReleased(t, r.d, r.root)
	eager, found, err := r.d.blobStorage.ReadBlobSidecars(t.Context(), recoveryTestSlot, r.root)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, eager, 3)
	receiveRecoveredColumnsPublish(t, r.published)
	atFuluFromGenesis(r)

	for _, tc := range []struct {
		name    string
		indices []uint64
		want    []int
	}{
		{"all", []uint64{0, 1, 2}, []int{0, 1, 2}},
		{"subset", []uint64{0, 2}, []int{0, 2}},
		{"indices from the blob count are skipped", []uint64{1, 3, 9}, []int{1}},
		{"nil indices mean every blob", nil, []int{0, 1, 2}},
		{"empty indices", []uint64{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			blobs, sidecars, err := r.blobsFromColumns(tc.indices, true)

			require.NoError(t, err)
			require.Len(t, blobs, len(tc.want))
			require.Len(t, sidecars, len(tc.want))
			for k, index := range tc.want {
				require.True(t, *blobs[k] == eager[index].Blob, "blob %d", index)
				wantSSZ, err := eager[index].EncodeSSZ(nil)
				require.NoError(t, err)
				gotSSZ, err := sidecars[k].EncodeSSZ(nil)
				require.NoError(t, err)
				require.Equal(t, wantSSZ, gotSSZ, "sidecar %d", index)
			}
			require.NoError(t, blob_storage.VerifyBlobSidecars(sidecars, clparams.FuluVersion, nil))
		})
	}

	t.Run("without sidecars", func(t *testing.T) {
		blobs, sidecars, err := r.blobsFromColumns([]uint64{0, 1, 2}, false)

		require.NoError(t, err)
		require.Len(t, blobs, 3)
		require.Nil(t, sidecars)
	})
}

func TestBlobsFromColumnsOfAGloasBlockBuildsNoSidecars(t *testing.T) {
	g := startGossipRecovery(t, false, func(r *recoveryRun) {
		forkToGloasAt(t, r, recoveryTestSlot/clparams.MainnetBeaconConfig.SlotsPerEpoch)
	})

	blobs, sidecars, err := g.blobsFromColumns([]uint64{0, 1}, false)

	require.NoError(t, err)
	require.Nil(t, sidecars)
	require.Len(t, blobs, 2)
	for i := range blobs {
		require.True(t, *blobs[i] == g.blobs[i].Blob, "blob %d", i)
	}
	reads := g.columnReads.reads.Load()

	_, _, err = g.blobsFromColumns([]uint64{0, 1}, true)

	require.Error(t, err)
	require.NotErrorIs(t, err, ErrColumnsNotKept)
	require.NotErrorIs(t, err, ErrBlobsUnavailable)
	require.Equal(t, reads, g.columnReads.reads.Load(), "column reads of the refused request")
}

func TestBlobsFromColumnsServesASupernodeOutsideLazyMode(t *testing.T) {
	for name, caplinConfig := range map[string]clparams.CaplinConfig{
		"archive blobs":               {ArchiveBlobs: true},
		"immediate blobs backfilling": {ImmediateBlobsBackfilling: true},
		"column keep slots":           {ColumnKeepSlots: 1},
		"blob pruning disabled":       {BlobPruningDisabled: true},
	} {
		t.Run(name, func(t *testing.T) {
			r := startRecoveryWithPublisher(t, recoveryOptions{caplinConfig: &caplinConfig, supernode: true, unscheduled: true, instrument: atFuluFromGenesis})
			require.False(t, r.d.lazyBlobSidecars(recoveryTestSlot))
			r.requireNoBlobSidecars(t)

			blobs, sidecars, err := r.blobsFromColumns([]uint64{0, 1}, true)

			require.NoError(t, err)
			require.Len(t, blobs, 2)
			require.Len(t, sidecars, 2)
			for i := range blobs {
				require.True(t, *blobs[i] == r.blobs[i].Blob, "blob %d", i)
				require.Equal(t, r.blobs[i].KzgCommitment, sidecars[i].KzgCommitment, "commitment %d", i)
			}
			require.NoError(t, blob_storage.VerifyBlobSidecars(sidecars, clparams.FuluVersion, nil))
		})
	}
}

// Outside lazy mode the count row is no proof that the blob files exist: a column that a blob request needs is rebuilt
// when a file is missing, and the file is written again. While one recovery owns the root, a repeated request that finds the
// column still missing neither queues a second recovery nor changes the stored columns.
func TestBlobsFromColumnsRepairsAColumnOutsideLazyModeWhenABlobFileIsMissing(t *testing.T) {
	var removeFile func(index uint64) error
	g := startGossipRecovery(t, true, atFuluFromGenesis, func(r *recoveryRun) {
		r.d.caplinConfig = &clparams.CaplinConfig{BlobPruningDisabled: true}
		removeFile = useBlobFiles(t, r)
	})
	require.False(t, g.d.lazyBlobSidecars(recoveryTestSlot))
	g.scheduleWithoutMetadata(t)
	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireBlobsRecovered(t)
	g.requireAllColumnsStored(t)
	receiveRecoveredColumnsPublish(t, g.published)
	require.NoError(t, removeFile(1))
	var kept []uint64
	var removed []int64
	for column := range g.d.beaconConfig.NumberOfColumns {
		if column == 63 || column > 64 {
			removed = append(removed, int64(column))
		} else {
			kept = append(kept, column)
		}
	}
	require.NoError(t, g.d.columnStorage.RemoveColumnSidecars(t.Context(), recoveryTestSlot, g.root, removed...))
	g.requireNoBlobSidecars(t)
	count, err := g.d.blobStorage.KzgCommitmentsCount(t.Context(), g.root)
	require.NoError(t, err)
	require.Positive(t, count)
	saved, err := g.d.columnStorage.GetSavedColumnIndex(t.Context(), recoveryTestSlot, g.root)
	require.NoError(t, err)
	require.ElementsMatch(t, kept, saved)
	require.True(t, g.d.IsColumnOverHalf(recoveryTestSlot, g.root))

	_, _, err = g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	owned := g.ownedRequest()
	require.Equal(t, g.root, owned.blockRoot, "the recovery that owns the root")
	g.awaitClaim(t)

	_, _, err = g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	require.Equal(t, owned, g.ownedRequest())
	g.d.recoveringMutex.Lock()
	recovering := maps.Clone(g.d.isRecovering)
	g.d.recoveringMutex.Unlock()
	require.Equal(t, map[common.Hash]bool{g.root: true}, recovering)
	require.Empty(t, g.d.recoverBlobsQueue)
	saved, err = g.d.columnStorage.GetSavedColumnIndex(t.Context(), recoveryTestSlot, g.root)
	require.NoError(t, err)
	require.ElementsMatch(t, kept, saved)

	g.releaseClock()

	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireAllColumnsStored(t)
	g.requireBlobsRecovered(t)
	g.requireBlobsRebuilt(t)
}

func TestBlobsFromColumnsServesFromTheCustodyRaiseSlot(t *testing.T) {
	r := startRecoveryWithPublisher(t, recoveryOptions{caplinConfig: &clparams.CaplinConfig{}, supernode: true, unscheduled: true, instrument: func(r *recoveryRun) {
		atFuluFromGenesis(r)
		r.d.state.SetCustodyRaiseSlot(recoveryTestSlot)
	}})

	blobs, _, err := r.blobsFromColumns([]uint64{0, 1}, false)

	require.NoError(t, err)
	require.Len(t, blobs, 2)
}

func TestBlobsFromColumnsRefusesANodeThatKeepsNoColumnsOfTheSlot(t *testing.T) {
	for name, tc := range map[string]struct {
		supernode  bool
		instrument func(r *recoveryRun)
	}{
		"not a supernode":  {supernode: false, instrument: atFuluFromGenesis},
		"slot before fulu": {supernode: true},
		"slot before the custody raise": {supernode: true, instrument: func(r *recoveryRun) {
			atFuluFromGenesis(r)
			r.d.state.SetCustodyRaiseSlot(recoveryTestSlot + 1)
		}},
	} {
		t.Run(name, func(t *testing.T) {
			r := startRecoveryWithPublisher(t, recoveryOptions{caplinConfig: &clparams.CaplinConfig{}, supernode: tc.supernode, unscheduled: true, instrument: tc.instrument})

			_, _, err := r.blobsFromColumns([]uint64{0}, false)

			require.ErrorIs(t, err, ErrColumnsNotKept)
		})
	}
}

func TestBlobsFromColumnsRefusesASlotBelowTheEarliestAvailableSlotWithoutWaiting(t *testing.T) {
	h := holdReconstructions(t, 1)
	h.start(t.Context(), h.root)
	h.awaitAdmission(t, 1, 0)
	require.NoError(t, h.d.PruneBelow(recoveryTestSlot+1))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, _, err := h.d.BlobsFromColumns(ctx, recoveryTestSlot, h.root, []uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrColumnsNotKept)
	require.Zero(t, h.d.reconstructions.waiting.Load(), "waiting requests")
}

func (g *gossipRecovery) requireBlobsRebuilt(t *testing.T) {
	t.Helper()
	blobs, _, err := g.blobsFromColumns([]uint64{0, 1}, false)
	require.NoError(t, err)
	require.Len(t, blobs, 2)
	for i := range blobs {
		require.True(t, *blobs[i] == g.blobs[i].Blob, "blob %d", i)
	}
}

// TestBlobsFromColumnsRebuildsAMissingColumn checks that a missing column among the needed ones fails the request and
// asks recovery to rebuild it from the others.
func TestBlobsFromColumnsRebuildsAMissingColumn(t *testing.T) {
	g := startGossipRecovery(t, false, atFuluFromGenesis)
	require.NoError(t, g.d.columnStorage.RemoveColumnSidecars(t.Context(), recoveryTestSlot, g.root, 5))
	g.storeColumn(t, 64)

	_, _, err := g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireAllColumnsStored(t)
	g.requireBlobsRebuilt(t)
}

func (g *gossipRecovery) ownedRequest() recoverBlobsRequest {
	g.d.recoveringMutex.Lock()
	defer g.d.recoveringMutex.Unlock()
	return g.d.recoveryRequests[g.root]
}

// missingColumnOfAHoldableRoot starts a gated recovery of a block with many blobs that is queued for the column sync, so
// a scheduled root is holdable. Column 5 is missing from the 64 stored columns, so a blob request fails on it.
func missingColumnOfAHoldableRoot(t *testing.T) *gossipRecovery {
	t.Helper()
	g := startGossipRecoveryWithBlobs(t, true, holdBlobs, atFuluFromGenesis)
	g.queueForColumnSync(time.Now().Add(firstAttemptAhead))
	require.NoError(t, g.d.columnStorage.RemoveColumnSidecars(t.Context(), recoveryTestSlot, g.root, 5))
	g.storeColumn(t, 64)
	return g
}

// A blob request that fails brings no column, so it must not restart the quiet period of a recovery that waits.
func TestBlobsFromColumnsDoesNotRestartTheWaitOfAnOwnedRecovery(t *testing.T) {
	g := missingColumnOfAHoldableRoot(t)
	g.schedule(t)
	owned := g.ownedRequest()
	require.True(t, owned.holdable)

	_, _, err := g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	require.Equal(t, owned, g.ownedRequest())
}

func TestBlobsFromColumnsSchedulesANewRecoveryThatCannotRestartAWait(t *testing.T) {
	g := missingColumnOfAHoldableRoot(t)
	begin := time.Now()

	_, _, err := g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	owned := g.ownedRequest()
	require.Equal(t, g.root, owned.blockRoot)
	require.False(t, owned.waitFrom.Before(begin), "the quiet period starts at the request")
	require.False(t, owned.holdable)
}

// damagedUntilRemovedStorage fails the reads of one column until the column is removed: with readErr, or by damaging
// the sidecar it returns.
type damagedUntilRemovedStorage struct {
	blob_storage.DataColumnStorage
	column    int64
	readErr   error
	damage    func(*cltypes.DataColumnSidecar)
	removeErr error
	removed   atomic.Bool
}

func (s *damagedUntilRemovedStorage) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	if columnIndex == s.column && s.readErr != nil && !s.removed.Load() {
		return nil, s.readErr
	}
	sidecar, err := s.DataColumnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, columnIndex)
	if err == nil && columnIndex == s.column && !s.removed.Load() && s.damage != nil {
		s.damage(sidecar)
	}
	return sidecar, err
}

func (s *damagedUntilRemovedStorage) RemoveColumnSidecars(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndices ...int64) error {
	if s.removeErr != nil {
		return s.removeErr
	}
	s.removed.Store(true)
	return s.DataColumnStorage.RemoveColumnSidecars(ctx, slot, blockRoot, columnIndices...)
}

func dropLastProof(s *cltypes.DataColumnSidecar) { s.KzgProofs.Truncate(s.KzgProofs.Len() - 1) }

// TestBlobsFromColumnsRemovesAMalformedColumnAndRebuildsIt checks that a malformed column is removed, so recovery can
// rebuild it, and that the request fails.
func TestBlobsFromColumnsRemovesAMalformedColumnAndRebuildsIt(t *testing.T) {
	var columns *damagedUntilRemovedStorage
	g := startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
		columns = &damagedUntilRemovedStorage{DataColumnStorage: r.d.columnStorage, column: 3, damage: dropLastProof}
		r.d.columnStorage = columns
	})
	g.storeColumn(t, 64)

	_, _, err := g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	require.True(t, columns.removed.Load())
	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireAllColumnsStored(t)
	g.requireBlobsRebuilt(t)
}

// TestBlobsFromColumnsRemovesAGloasColumnOfAnotherBlock checks that a Gloas column that names another block is malformed.
func TestBlobsFromColumnsRemovesAGloasColumnOfAnotherBlock(t *testing.T) {
	var columns *damagedUntilRemovedStorage
	g := startGossipRecovery(t, false, func(r *recoveryRun) {
		forkToGloasAt(t, r, recoveryTestSlot/clparams.MainnetBeaconConfig.SlotsPerEpoch)
		columns = &damagedUntilRemovedStorage{DataColumnStorage: r.d.columnStorage, column: 3, damage: func(s *cltypes.DataColumnSidecar) {
			s.BeaconBlockRoot = common.HexToHash("0xabc")
		}}
		r.d.columnStorage = columns
	})
	g.storeColumn(t, 64)

	_, _, err := g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	require.True(t, columns.removed.Load())
	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireAllColumnsStored(t)
}

// TestBlobsFromColumnsRemovesAnIndexedColumnThatCannotBeRead checks that a column that the index lists but the storage
// cannot read is removed too. Recovery rebuilds no column that the index lists, so it would otherwise leave the block
// without the column for good.
func TestBlobsFromColumnsRemovesAnIndexedColumnThatCannotBeRead(t *testing.T) {
	var columns *damagedUntilRemovedStorage
	g := startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
		columns = &damagedUntilRemovedStorage{DataColumnStorage: r.d.columnStorage, column: 7, readErr: os.ErrNotExist}
		r.d.columnStorage = columns
	})
	for _, column := range g.unstoredColumns() {
		g.storeColumn(t, column.Index)
	}

	_, _, err := g.blobsFromColumns([]uint64{0, 1}, false)

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	require.True(t, columns.removed.Load())
	requireBlobRecoveryReleased(t, g.d, g.root)
	g.requireAllColumnsStored(t)
	g.requireBlobsRebuilt(t)
}

// TestBlobsFromColumnsKeepsAColumnThatAppearsAfterTheFirstFailedRead checks that a column that is there when the request
// looks again is used, not removed.
func TestBlobsFromColumnsKeepsAColumnThatAppearsAfterTheFirstFailedRead(t *testing.T) {
	var columns *transientColumnSidecarReadStorage
	g := startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
		columns = &transientColumnSidecarReadStorage{DataColumnStorage: r.d.columnStorage, columnIndex: 3, readErr: os.ErrNotExist, failed: make(chan struct{})}
		r.d.columnStorage = columns
	})

	g.requireBlobsRebuilt(t)

	require.Equal(t, int32(2), columns.targetReads.Load(), "reads of the column")
	require.Zero(t, columns.targetRemoves.Load(), "removals of the column")
	saved, err := g.d.columnStorage.GetSavedColumnIndex(t.Context(), recoveryTestSlot, g.root)
	require.NoError(t, err)
	require.Contains(t, saved, uint64(3))
	g.d.recoveringMutex.Lock()
	defer g.d.recoveringMutex.Unlock()
	require.NotContains(t, g.d.isRecovering, g.root)
}

// TestBlobsFromColumnsDoesNotScheduleRecoveryWhenTheRemovalFails checks that recovery is not scheduled when the
// malformed column cannot be removed, as recovery would not rebuild it.
func TestBlobsFromColumnsDoesNotScheduleRecoveryWhenTheRemovalFails(t *testing.T) {
	g := startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
		r.d.columnStorage = &damagedUntilRemovedStorage{DataColumnStorage: r.d.columnStorage, column: 3, damage: dropLastProof, removeErr: errColumnSingleFailed}
	})

	var err error
	records := capturePeerDasLogs(func() { _, _, err = g.blobsFromColumns([]uint64{0, 1}, false) })

	require.ErrorIs(t, err, ErrBlobsUnavailable)
	require.Len(t, records, 2)
	require.Equal(t, log.LvlWarn, records[0].Lvl)
	require.Equal(t, "[peerdas] removing malformed column sidecar", records[0].Msg)
	require.Equal(t, recoveryTestSlot, logContextValue(records[0], "slot"))
	require.Equal(t, g.root, logContextValue(records[0], "blockRoot"))
	require.Equal(t, uint64(3), logContextValue(records[0], "column"))
	require.Equal(t, log.LvlWarn, records[1].Lvl)
	require.Equal(t, errColumnSingleFailed, logContextValue(records[1], "err"))
	g.d.recoveringMutex.Lock()
	defer g.d.recoveringMutex.Unlock()
	require.NotContains(t, g.d.isRecovering, g.root)
}

// gatedColumnReads holds every column read except those of column 0 until the gate is closed. A request reads column 0 before it
// takes a reconstruction permit, so only the later reads show who holds a permit.
type gatedColumnReads struct {
	blob_storage.DataColumnStorage
	gate        chan struct{}
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	reads       atomic.Int32
}

func (s *gatedColumnReads) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	if columnIndex == 0 {
		return s.DataColumnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, columnIndex)
	}
	inFlight := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		peak := s.maxInFlight.Load()
		if inFlight <= peak || s.maxInFlight.CompareAndSwap(peak, inFlight) {
			break
		}
	}
	s.reads.Add(1)
	<-s.gate
	return s.DataColumnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, columnIndex)
}

type heldReconstructions struct {
	*gossipRecovery
	columns *gatedColumnReads
	release func()
	results chan error
}

// holdReconstructions admits permits reconstructions at a time and holds their column reads until release is called.
func holdReconstructions(t *testing.T, permits int) *heldReconstructions {
	t.Helper()
	h := &heldReconstructions{results: make(chan error, 16)}
	h.columns = &gatedColumnReads{gate: make(chan struct{})}
	h.gossipRecovery = startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
		h.columns.DataColumnStorage = r.d.columnStorage
		r.d.columnStorage = h.columns
		r.d.reconstructions = newReconstructionAdmission(permits)
	})
	h.release = sync.OnceFunc(func() { close(h.columns.gate) })
	t.Cleanup(h.release)
	return h
}

func (h *heldReconstructions) start(ctx context.Context, root common.Hash) {
	go func() {
		_, _, err := h.d.BlobsFromColumns(ctx, recoveryTestSlot, root, []uint64{0, 1}, false)
		h.results <- err
	}()
}

func (h *heldReconstructions) awaitAdmission(t *testing.T, running, waiting int32) {
	t.Helper()
	require.Eventually(t, func() bool {
		return h.columns.inFlight.Load() == running && h.d.reconstructions.waiting.Load() == waiting
	}, 30*time.Second, time.Millisecond)
}

func (h *heldReconstructions) requireResult(t *testing.T) error {
	t.Helper()
	select {
	case err := <-h.results:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("a reconstruction did not return")
		return nil
	}
}

func holdEveryPermit(d *peerdas) {
	for range cap(d.reconstructions.permits) {
		d.reconstructions.permits <- struct{}{}
	}
	d.reconstructions.waiting.Store(int32(cap(d.reconstructions.permits)))
}

func TestBlobsFromColumnsRunsAtMostThePermittedReconstructionsAtOnceAndRefusesWhenTheWaitersAreFull(t *testing.T) {
	h := holdReconstructions(t, 2)
	for range 4 {
		h.start(t.Context(), h.root)
	}
	h.awaitAdmission(t, 2, 2)

	h.start(t.Context(), h.root)

	require.ErrorIs(t, h.requireResult(t), ErrReconstructionBusy)
	h.release()
	for range 4 {
		require.NoError(t, h.requireResult(t))
	}
	require.Equal(t, int32(2), h.columns.maxInFlight.Load())
}

func TestBlobsFromColumnsReadsNoMoreColumnsForARequestCancelledWhileWaiting(t *testing.T) {
	h := holdReconstructions(t, 1)
	h.start(t.Context(), h.root)
	h.awaitAdmission(t, 1, 0)
	ctx, cancel := context.WithCancel(t.Context())
	h.start(ctx, h.root)
	h.awaitAdmission(t, 1, 1)

	cancel()

	require.ErrorIs(t, h.requireResult(t), context.Canceled)
	require.Equal(t, int32(0), h.d.reconstructions.waiting.Load())
	require.Equal(t, int32(1), h.columns.reads.Load(), "held column reads")
	h.release()
	require.NoError(t, h.requireResult(t))
}

func TestBlobsFromColumnsAnswersWithoutAPermitWhenNoBlobIsSelected(t *testing.T) {
	h := holdReconstructions(t, 1)
	h.start(t.Context(), h.root)
	h.awaitAdmission(t, 1, 0)
	h.start(t.Context(), h.root)
	h.awaitAdmission(t, 1, 1)

	for _, tc := range []struct {
		name    string
		root    common.Hash
		indices []uint64
	}{
		{"a block with no stored column", common.Hash{0x42}, nil},
		{"an index beyond the blob count", h.root, []uint64{7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			blobs, sidecars, err := h.d.BlobsFromColumns(ctx, recoveryTestSlot, tc.root, tc.indices, true)

			require.NoError(t, err)
			require.Empty(t, blobs)
			require.Empty(t, sidecars)
		})
	}
}

func TestBlobsFromColumnsOfABlockWithNoStoredColumnReadsNoColumnAndSchedulesNoRecovery(t *testing.T) {
	root := common.Hash{0x42}
	var reads *firstColumnReadStorage
	g := startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
		reads = &firstColumnReadStorage{DataColumnStorage: r.d.columnStorage, root: root, firstRead: make(chan time.Time, 1)}
		r.d.columnStorage = reads
	})

	blobs, sidecars, err := g.d.BlobsFromColumns(t.Context(), recoveryTestSlot, root, nil, true)

	require.NoError(t, err)
	require.Empty(t, blobs)
	require.Empty(t, sidecars)
	require.Zero(t, reads.reads.Load(), "column reads")
	g.d.recoveringMutex.Lock()
	defer g.d.recoveringMutex.Unlock()
	require.NotContains(t, g.d.isRecovering, root)
	require.Empty(t, g.d.recoverBlobsQueue)
}

func TestBlobsFromColumnsStopsWhenTheRequestIsCancelled(t *testing.T) {
	const lastNeededColumn = 64
	for _, tc := range []struct {
		name         string
		at           int32
		withSidecars bool
	}{
		{"before the next column", 1, false},
		{"before the first proof", lastNeededColumn, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := startSharedBuilds(t, 2, 2)
			var reads atomic.Int32
			s.seams.setBeforeRead(func(context.Context, int64) error {
				if reads.Add(1) == tc.at {
					cancel()
				}
				return nil
			})

			blobs, sidecars, err := s.d.BlobsFromColumns(ctx, recoveryTestSlot, s.root, []uint64{0, 1}, tc.withSidecars)

			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, blobs)
			require.Nil(t, sidecars)
			require.Equal(t, int(tc.at), s.seams.allReads(), "column reads")
		})
	}
}

func TestBlobsFromColumnsDoesNotShareReconstructionPermitsBetweenInstances(t *testing.T) {
	busy := startGossipRecovery(t, false, atFuluFromGenesis)
	other := startGossipRecovery(t, false, atFuluFromGenesis)
	holdEveryPermit(busy.d)

	_, _, err := busy.blobsFromColumns([]uint64{0, 1}, false)
	require.ErrorIs(t, err, ErrReconstructionBusy)

	blobs, _, err := other.blobsFromColumns([]uint64{0, 1}, false)

	require.NoError(t, err)
	require.Len(t, blobs, 2)
}

type columnRead struct {
	root   common.Hash
	column int64
}

type seamStorage struct {
	blob_storage.DataColumnStorage
	mu          sync.Mutex
	indexReads  int
	columnReads map[columnRead]int
	beforeIndex func()
	beforeRead  func(ctx context.Context, column int64) error
}

func (s *seamStorage) GetSavedColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash) ([]uint64, error) {
	s.mu.Lock()
	s.indexReads++
	s.mu.Unlock()
	if s.beforeIndex != nil {
		s.beforeIndex()
	}
	return s.DataColumnStorage.GetSavedColumnIndex(ctx, slot, blockRoot)
}

func (s *seamStorage) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	s.mu.Lock()
	s.columnReads[columnRead{blockRoot, columnIndex}]++
	before := s.beforeRead
	s.mu.Unlock()
	if before != nil {
		if err := before(ctx, columnIndex); err != nil {
			return nil, err
		}
	}
	return s.DataColumnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, columnIndex)
}

func (s *seamStorage) setBeforeRead(before func(ctx context.Context, column int64) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beforeRead = before
}

func (s *seamStorage) reads(root common.Hash, column int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.columnReads[columnRead{root, column}]
}

func (s *seamStorage) allReads() (total int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, reads := range s.columnReads {
		total += reads
	}
	return total
}

func (s *seamStorage) indexLookups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.indexReads
}

func (s *seamStorage) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.indexReads = 0
	clear(s.columnReads)
}

type blobsResult struct {
	sidecars []*cltypes.BlobSidecar
	err      error
}

func awaitBlobs(t *testing.T, results <-chan blobsResult) blobsResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(30 * time.Second):
		t.Fatal("a request did not return")
		return blobsResult{}
	}
}

type sharedBuilds struct {
	*gossipRecovery
	seams *seamStorage
}

func startSharedBuilds(t *testing.T, blobs, permits int, configure ...func(*recoveryRun)) *sharedBuilds {
	t.Helper()
	s := &sharedBuilds{seams: &seamStorage{columnReads: make(map[columnRead]int)}}
	s.gossipRecovery = startGossipRecoveryWithBlobs(t, false, blobs, append([]func(*recoveryRun){atFuluFromGenesis, func(r *recoveryRun) {
		s.seams.DataColumnStorage = r.d.columnStorage
		r.d.columnStorage = s.seams
		r.d.reconstructions = newReconstructionAdmission(permits)
	}}, configure...)...)
	return s
}

func withSlotSeconds(seconds uint64) func(*recoveryRun) {
	return func(r *recoveryRun) {
		cfg := *r.d.beaconConfig
		cfg.SecondsPerSlot = seconds
		r.d.beaconConfig = &cfg
	}
}

func withBlobLimit(limit uint64) func(*recoveryRun) {
	return func(r *recoveryRun) {
		cfg := *r.d.beaconConfig
		cfg.BlobSchedule = []clparams.BlobParameters{{Epoch: 0, MaxBlobsPerBlock: limit}}
		r.d.beaconConfig = &cfg
	}
}

func (s *sharedBuilds) startFullRequest(ctx context.Context) <-chan blobsResult {
	results := make(chan blobsResult, 1)
	go func() {
		var result blobsResult
		_, result.sidecars, result.err = s.d.BlobsFromColumns(ctx, recoveryTestSlot, s.root, nil, true)
		results <- result
	}()
	return results
}

func newGate(t *testing.T) (wait, release func()) {
	t.Helper()
	gate := make(chan struct{})
	release = sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	return func() { <-gate }, release
}

func (s *sharedBuilds) holdColumn(t *testing.T, column int64) (release func()) {
	t.Helper()
	wait, release := newGate(t)
	s.seams.setBeforeRead(func(_ context.Context, read int64) error {
		if read == column {
			wait()
		}
		return nil
	})
	return release
}

func (s *sharedBuilds) awaitRead(t *testing.T, column int64) {
	t.Helper()
	require.Eventually(t, func() bool { return s.seams.reads(s.root, column) > 0 }, 30*time.Second, time.Millisecond)
}

func (s *sharedBuilds) awaitJoined(t *testing.T, joiners int32) {
	t.Helper()
	require.Eventually(t, func() bool { return s.d.reconstructions.joined.Load() == joiners }, 30*time.Second, time.Millisecond)
}

func (s *sharedBuilds) requireNoBuildLeft(t *testing.T, heldPermits int) {
	t.Helper()
	s.d.blobBuildsMu.Lock()
	defer s.d.blobBuildsMu.Unlock()
	require.Empty(t, s.d.blobBuilds, "builds in flight")
	require.Len(t, s.d.reconstructions.permits, heldPermits, "held permits")
	require.Zero(t, s.d.reconstructions.waiting.Load(), "waiting requests")
	require.Zero(t, s.d.reconstructions.joined.Load(), "joined requests")
}

func (s *sharedBuilds) requireNothingCached(t *testing.T) {
	t.Helper()
	require.Zero(t, s.d.builtBlobSidecars.Len(), "cached blocks")
}

func (s *sharedBuilds) requireCacheHit(t *testing.T, root common.Hash, want []*cltypes.BlobSidecar) {
	t.Helper()
	reads := s.seams.allReads()
	_, got, err := s.d.BlobsFromColumns(t.Context(), recoveryTestSlot, root, nil, true)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range got {
		require.Same(t, want[i], got[i], "sidecar %d", i)
	}
	require.Equal(t, reads, s.seams.allReads(), "column reads of the repeated request")
}

func requireSameSidecarData(t *testing.T, want, got []*cltypes.BlobSidecar) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		wantSSZ, err := want[i].EncodeSSZ(nil)
		require.NoError(t, err)
		gotSSZ, err := got[i].EncodeSSZ(nil)
		require.NoError(t, err)
		require.Equal(t, wantSSZ, gotSSZ, "sidecar %d", i)
	}
}

func (s *sharedBuilds) storeBlock(t *testing.T, executionBlockHash common.Hash) common.Hash {
	t.Helper()
	_, root, _, columns := recoverableFuluDataWithBlobs(t, s.d.beaconConfig, recoveryTestSlot, executionBlockHash, len(s.blobs))
	s.storeFirstHalf(t, root, columns)
	return root
}

func (s *sharedBuilds) storeFirstHalf(t *testing.T, root common.Hash, columns []*cltypes.DataColumnSidecar) {
	t.Helper()
	for index := range s.d.beaconConfig.NumberOfColumns / 2 {
		require.NoError(t, s.d.columnStorage.WriteColumnSidecars(t.Context(), root, int64(index), columns[index]))
	}
}

func TestBlobsFromColumnsSharesOneBuildBetweenIdenticalRequests(t *testing.T) {
	s := startSharedBuilds(t, 2, 4)
	_, want, err := s.blobsFromColumns([]uint64{0, 1}, true)
	require.NoError(t, err)
	s.seams.reset()
	wait, release := newGate(t)
	s.seams.beforeIndex = wait
	const requests = 4
	var results [requests]<-chan blobsResult
	for i := range results {
		results[i] = s.startFullRequest(t.Context())
	}
	s.awaitJoined(t, requests-1)

	release()

	first := awaitBlobs(t, results[0])
	require.NoError(t, first.err)
	requireSameSidecarData(t, want, first.sidecars)
	for _, pending := range results[1:] {
		got := awaitBlobs(t, pending)
		require.NoError(t, got.err)
		require.Len(t, got.sidecars, len(first.sidecars))
		for k := range got.sidecars {
			require.Same(t, first.sidecars[k], got.sidecars[k], "sidecar %d", k)
		}
	}
	require.Equal(t, 1, s.seams.indexLookups(), "saved-index lookups")
	for column := int64(0); column < int64(s.d.beaconConfig.NumberOfColumns/2); column++ {
		require.Equal(t, 1, s.seams.reads(s.root, column), "reads of column %d", column)
	}
}

func TestBlobsFromColumnsFinishesTheSharedBuildWhenTheStarterIsCancelled(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	release := s.holdColumn(t, 1)
	starterCtx, cancelStarter := context.WithCancel(t.Context())
	starter := s.startFullRequest(starterCtx)
	s.awaitRead(t, 1)
	waiter := s.startFullRequest(t.Context())
	s.awaitJoined(t, 1)

	cancelStarter()
	require.ErrorIs(t, awaitBlobs(t, starter).err, context.Canceled)
	release()

	got := awaitBlobs(t, waiter)
	require.NoError(t, got.err)
	require.Len(t, got.sidecars, 2)
	require.Equal(t, 1, s.seams.reads(s.root, 1), "reads of column 1")
	s.requireCacheHit(t, s.root, got.sidecars)
	s.awaitNoBuildLeft(t, 0)
}

func (s *sharedBuilds) awaitNoBuildLeft(t *testing.T, heldPermits int) {
	t.Helper()
	require.Eventually(t, func() bool {
		s.d.blobBuildsMu.Lock()
		defer s.d.blobBuildsMu.Unlock()
		return len(s.d.blobBuilds) == 0
	}, 30*time.Second, time.Millisecond)
	s.requireNoBuildLeft(t, heldPermits)
}

func TestBlobsFromColumnsFreesTheWaiterPlaceOfACancelledWaiter(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	release := s.holdColumn(t, 1)
	starter := s.startFullRequest(t.Context())
	s.awaitRead(t, 1)
	waiterCtx, cancelWaiter := context.WithCancel(t.Context())
	waiter := s.startFullRequest(waiterCtx)
	s.awaitJoined(t, 1)

	cancelWaiter()

	require.ErrorIs(t, awaitBlobs(t, waiter).err, context.Canceled)
	s.awaitJoined(t, 0)
	release()
	got := awaitBlobs(t, starter)
	require.NoError(t, got.err)
	require.Len(t, got.sidecars, 2)
	require.Equal(t, 1, s.seams.reads(s.root, 1), "reads of column 1")
	s.requireCacheHit(t, s.root, got.sidecars)
}

func TestBlobsFromColumnsCompletesAndCachesTheBuildWhenEveryRequestIsCancelled(t *testing.T) {
	s := startSharedBuilds(t, 2, 4)
	release := s.holdColumn(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	starter := s.startFullRequest(ctx)
	s.awaitRead(t, 1)
	waiters := []<-chan blobsResult{s.startFullRequest(ctx), s.startFullRequest(ctx)}
	s.awaitJoined(t, 2)

	cancel()
	for _, waiter := range waiters {
		require.ErrorIs(t, awaitBlobs(t, waiter).err, context.Canceled)
	}
	require.ErrorIs(t, awaitBlobs(t, starter).err, context.Canceled)
	s.awaitJoined(t, 0)
	release()

	s.awaitNoBuildLeft(t, 0)
	require.Equal(t, 1, s.seams.reads(s.root, 1), "reads of column 1")
	cached, ok := s.d.builtBlobSidecars.Peek(s.root)
	require.True(t, ok, "the build is cached")
	s.requireCacheHit(t, s.root, cached)
	s.requireNoBuildLeft(t, 0)
}

func TestBlobsFromColumnsEndsABuildThatIsBlockedPastTheOwnerDeadline(t *testing.T) {
	s := startSharedBuilds(t, 2, 2, withSlotSeconds(1))
	var blocked atomic.Bool
	blocked.Store(true)
	s.seams.setBeforeRead(func(ctx context.Context, column int64) error {
		if column == 1 && blocked.Load() {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})

	got := awaitBlobs(t, s.startFullRequest(t.Context()))

	require.ErrorIs(t, got.err, ErrReconstructionBusy)
	require.Nil(t, got.sidecars)
	s.awaitNoBuildLeft(t, 0)
	s.requireNothingCached(t)
	blocked.Store(false)
	_, sidecars, err := s.blobsFromColumns(nil, true)
	require.NoError(t, err)
	require.Len(t, sidecars, 2)
}

func TestBlobsFromColumnsEndsABuildThatWaitsForAPermitPastTheOwnerDeadline(t *testing.T) {
	s := startSharedBuilds(t, 2, 1, withSlotSeconds(1))
	s.d.reconstructions.permits <- struct{}{}

	got := awaitBlobs(t, s.startFullRequest(t.Context()))

	require.ErrorIs(t, got.err, ErrReconstructionBusy)
	require.Nil(t, got.sidecars)
	s.awaitNoBuildLeft(t, 1)
	s.requireNothingCached(t)
}

func TestBlobsFromColumnsAnswersARequestBusyWhenTheBuildItWaitsForPassesItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := startSharedBuilds(t, 2, 2, withSlotSeconds(1))
		release := s.holdColumn(t, 1)
		starter := s.startFullRequest(t.Context())
		s.awaitRead(t, 1)
		joiner := s.startFullRequest(t.Context())
		s.awaitJoined(t, 1)

		got := awaitBlobs(t, joiner)

		require.ErrorIs(t, got.err, ErrReconstructionBusy)
		require.Nil(t, got.sidecars)
		require.ErrorIs(t, awaitBlobs(t, starter).err, ErrReconstructionBusy)
		late := awaitBlobs(t, s.startFullRequest(t.Context()))
		require.ErrorIs(t, late.err, ErrReconstructionBusy)
		require.Nil(t, late.sidecars)
		require.Equal(t, 1, s.seams.indexLookups(), "saved-index lookups")
		require.Equal(t, 1, s.seams.reads(s.root, 1), "reads of column 1")
		s.d.blobBuildsMu.Lock()
		_, inFlight := s.d.blobBuilds[s.root]
		s.d.blobBuildsMu.Unlock()
		require.True(t, inFlight, "the build is no longer in flight")

		synctest.Wait()
		release()
		synctest.Wait()

		s.requireNoBuildLeft(t, 0)
		s.requireNothingCached(t)
		_, sidecars, err := s.blobsFromColumns(nil, true)
		require.NoError(t, err)
		require.Len(t, sidecars, 2)
	})
}

func TestBlobsFromColumnsGivesTheJoinerPlacesBackWhenTheBuildPassesItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const permits = 1
		s := startSharedBuilds(t, 2, permits, withSlotSeconds(1))
		for range 2 {
			s.seams.reset()
			release := s.holdColumn(t, 1)
			starter := s.startFullRequest(t.Context())
			s.awaitRead(t, 1)
			joiners := make([]<-chan blobsResult, 2*permits)
			for i := range joiners {
				joiners[i] = s.startFullRequest(t.Context())
			}
			s.awaitJoined(t, 2*permits)

			for _, joiner := range joiners {
				require.ErrorIs(t, awaitBlobs(t, joiner).err, ErrReconstructionBusy)
			}
			require.Zero(t, s.d.reconstructions.joined.Load(), "joined requests")
			require.ErrorIs(t, awaitBlobs(t, s.startFullRequest(t.Context())).err, ErrReconstructionBusy)
			require.Zero(t, s.d.reconstructions.joined.Load(), "joined requests")

			require.ErrorIs(t, awaitBlobs(t, starter).err, ErrReconstructionBusy)
			synctest.Wait()
			release()
			synctest.Wait()
			s.requireNoBuildLeft(t, 0)
		}
	})
}

func TestBlobsFromColumnsRunsTheBuildUntilTheDeadlineOfItsEntry(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	var ownerDeadline, entryDeadline time.Time
	s.seams.setBeforeRead(func(ctx context.Context, column int64) error {
		if column == 1 {
			ownerDeadline, _ = ctx.Deadline()
			s.d.blobBuildsMu.Lock()
			defer s.d.blobBuildsMu.Unlock()
			entryDeadline = s.d.blobBuilds[s.root].deadline
		}
		return nil
	})
	before := time.Now()

	_, sidecars, err := s.blobsFromColumns(nil, true)

	after := time.Now()
	require.NoError(t, err)
	require.Len(t, sidecars, 2)
	require.True(t, ownerDeadline.Equal(entryDeadline), "the deadline of the build context %v and of its entry %v", ownerDeadline, entryDeadline)
	require.WithinRange(t, entryDeadline, before.Add(s.d.slotDuration()), after.Add(s.d.slotDuration()))
}

func TestBlobsFromColumnsAnswersAJoinerBusyWhenTheDeadlineOfTheBuildComes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remaining time.Duration
	}{
		{"deadline ahead", 7 * time.Second},
		{"deadline passed", -3 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d := &peerdas{beaconConfig: &clparams.BeaconChainConfig{SecondsPerSlot: 12}}
				d.initBlobBuilds()
				build := &blobBuild{done: make(chan struct{}), deadline: time.Now().Add(tc.remaining)}
				start := time.Now()

				_, err := d.joinBlobBuild(t.Context(), build)

				require.ErrorIs(t, err, ErrReconstructionBusy)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, max(tc.remaining, 0), time.Since(start))
			})
		})
	}
}

func TestBlobsFromColumnsGivesAFinishedBuildToAJoinerAfterTheBuildDeadline(t *testing.T) {
	d := &peerdas{}
	d.initBlobBuilds()
	built := []*cltypes.BlobSidecar{{Index: 7}}
	build := &blobBuild{done: make(chan struct{}), deadline: time.Now().Add(-time.Hour), sidecars: built}
	close(build.done)

	// With the build done and the deadline passed, select picks either ready case at random.
	for range 100 {
		got, err := d.joinBlobBuild(t.Context(), build)

		require.NoError(t, err)
		require.Equal(t, built, got)
	}
}

func TestBlobsFromColumnsRefusesAJoinerWhenTheJoinerPlacesAreFull(t *testing.T) {
	s := startSharedBuilds(t, 2, 1)
	release := s.holdColumn(t, 1)
	starter := s.startFullRequest(t.Context())
	s.awaitRead(t, 1)
	cancellableCtx, cancelJoiner := context.WithCancel(t.Context())
	cancellable := s.startFullRequest(cancellableCtx)
	steady := s.startFullRequest(t.Context())
	s.awaitJoined(t, 2)

	require.ErrorIs(t, awaitBlobs(t, s.startFullRequest(t.Context())).err, ErrReconstructionBusy)

	cancelJoiner()
	require.ErrorIs(t, awaitBlobs(t, cancellable).err, context.Canceled)
	s.awaitJoined(t, 1)
	replacement := s.startFullRequest(t.Context())
	s.awaitJoined(t, 2)
	release()
	require.NoError(t, awaitBlobs(t, starter).err)
	require.NoError(t, awaitBlobs(t, steady).err)
	require.NoError(t, awaitBlobs(t, replacement).err)
	require.Equal(t, 1, s.seams.reads(s.root, 1), "reads of column 1")
}

func TestBlobsFromColumnsLetsTheOwnerWaitForItsPermitWhileJoinersWait(t *testing.T) {
	const permits = 2
	s := startSharedBuilds(t, 2, permits)
	for range permits {
		s.d.reconstructions.permits <- struct{}{}
	}
	wait, releasePreflight := newGate(t)
	s.seams.beforeIndex = wait
	owner := s.startFullRequest(t.Context())
	require.Eventually(t, func() bool { return s.seams.indexLookups() == 1 }, 30*time.Second, time.Millisecond)
	joiners := make([]<-chan blobsResult, permits)
	for i := range joiners {
		joiners[i] = s.startFullRequest(t.Context())
	}
	s.awaitJoined(t, permits)

	releasePreflight()
	require.Eventually(t, func() bool { return s.d.reconstructions.waiting.Load() == 1 || len(owner) == 1 }, 30*time.Second, time.Millisecond)
	<-s.d.reconstructions.permits

	ownerResult := awaitBlobs(t, owner)
	require.NoError(t, ownerResult.err)
	require.Len(t, ownerResult.sidecars, 2)
	for _, pending := range joiners {
		got := awaitBlobs(t, pending)
		require.NoError(t, got.err)
		require.Len(t, got.sidecars, len(ownerResult.sidecars))
		for i := range got.sidecars {
			require.Same(t, ownerResult.sidecars[i], got.sidecars[i], "sidecar %d", i)
		}
	}
	s.requireNoBuildLeft(t, 1)
}

func TestBlobsFromColumnsDoesNotCacheABlockWithNoStoredColumn(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	_, root, _, columns := recoverableFuluDataWithBlobs(t, s.d.beaconConfig, recoveryTestSlot, common.Hash{1}, len(s.blobs))

	_, sidecars, err := s.d.BlobsFromColumns(t.Context(), recoveryTestSlot, root, nil, true)

	require.NoError(t, err)
	require.Empty(t, sidecars)
	s.requireNothingCached(t)
	s.storeFirstHalf(t, root, columns)

	_, sidecars, err = s.d.BlobsFromColumns(t.Context(), recoveryTestSlot, root, nil, true)

	require.NoError(t, err)
	require.Len(t, sidecars, len(s.blobs))
}

func TestBlobsFromColumnsTakesABuildThatEndedJustBeforeTheLockedLookupFromTheCache(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	_, built, err := s.blobsFromColumns(nil, true)
	require.NoError(t, err)
	s.seams.reset()

	got, err := s.d.sharedBlobSidecars(t.Context(), recoveryTestSlot, s.root)

	require.NoError(t, err)
	require.Len(t, got, len(built))
	for i := range got {
		require.Same(t, built[i], got[i], "sidecar %d", i)
	}
	require.Zero(t, s.seams.allReads(), "column reads")
	require.Zero(t, s.seams.indexLookups(), "saved-index lookups")
}

func TestBlobsFromColumnsAnswersARepeatedRequestFromTheCache(t *testing.T) {
	s := startSharedBuilds(t, 3, 2)
	_, built, err := s.blobsFromColumns(nil, true)
	require.NoError(t, err)
	require.Len(t, built, 3)
	s.seams.reset()
	holdEveryPermit(s.d)

	s.requireCacheHit(t, s.root, built)

	_, selected, err := s.blobsFromColumns([]uint64{0, 2, 9}, true)
	require.NoError(t, err)
	require.Equal(t, []*cltypes.BlobSidecar{built[0], built[2]}, selected)
	for _, tc := range []struct {
		name    string
		indices []uint64
		want    []int
	}{
		{"a selection", []uint64{0, 2, 9}, []int{0, 2}},
		{"every blob", nil, []int{0, 1, 2}},
	} {
		blobs, sidecars, err := s.blobsFromColumns(tc.indices, false)

		require.NoError(t, err, tc.name)
		require.Nil(t, sidecars, tc.name)
		require.Len(t, blobs, len(tc.want), tc.name)
		for k, index := range tc.want {
			require.True(t, *blobs[k] == built[index].Blob, "%s: blob %d", tc.name, index)
		}
	}
	require.Zero(t, s.seams.allReads(), "column reads")
	require.Zero(t, s.seams.indexLookups(), "saved-index lookups")
}

func TestBlobsFromColumnsSharesAFailedBuildAndDoesNotCacheIt(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	require.NoError(t, s.d.columnStorage.RemoveColumnSidecars(t.Context(), recoveryTestSlot, s.root, 5))
	s.storeColumn(t, 64)
	release := s.holdColumn(t, 1)
	first := s.startFullRequest(t.Context())
	s.awaitRead(t, 1)
	second := s.startFullRequest(t.Context())
	s.awaitJoined(t, 1)

	release()

	require.ErrorIs(t, awaitBlobs(t, first).err, ErrBlobsUnavailable)
	require.ErrorIs(t, awaitBlobs(t, second).err, ErrBlobsUnavailable)
	require.Equal(t, 1, s.seams.reads(s.root, 5), "reads of the missing column")
	s.requireNoBuildLeft(t, 0)
	s.requireNothingCached(t)
	requireBlobRecoveryReleased(t, s.d, s.root)
	s.requireAllColumnsStored(t)

	_, built, err := s.blobsFromColumns(nil, true)
	require.NoError(t, err)
	require.Len(t, built, 2)
	s.requireCacheHit(t, s.root, built)
}

func TestBlobsFromColumnsAnswersEveryRequestWithAnInternalErrorWhenABuildPanics(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	wait, release := newGate(t)
	var failing atomic.Bool
	failing.Store(true)
	s.seams.setBeforeRead(func(_ context.Context, column int64) error {
		if column == 1 && failing.Load() {
			wait()
			panic("column storage failed")
		}
		return nil
	})
	starter := s.startFullRequest(t.Context())
	s.awaitRead(t, 1)
	joiner := s.startFullRequest(t.Context())
	s.awaitJoined(t, 1)

	release()

	for _, pending := range []<-chan blobsResult{starter, joiner} {
		got := awaitBlobs(t, pending)
		require.Error(t, got.err)
		require.NotErrorIs(t, got.err, ErrReconstructionBusy)
		require.NotErrorIs(t, got.err, ErrBlobsUnavailable)
		require.Nil(t, got.sidecars)
	}
	s.awaitNoBuildLeft(t, 0)
	s.requireNothingCached(t)
	failing.Store(false)
	_, sidecars, err := s.blobsFromColumns(nil, true)
	require.NoError(t, err)
	require.Len(t, sidecars, 2)
}

func TestBlobsFromColumnsCachesTheLastFourBlocks(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	roots := []common.Hash{s.root}
	for i := 1; i < 5; i++ {
		roots = append(roots, s.storeBlock(t, common.Hash{byte(i)}))
	}
	build := func(root common.Hash) {
		_, sidecars, err := s.d.BlobsFromColumns(t.Context(), recoveryTestSlot, root, nil, true)
		require.NoError(t, err)
		require.Len(t, sidecars, 2)
	}
	for _, root := range roots[:4] {
		build(root)
	}
	build(roots[0])
	build(roots[4])
	s.seams.reset()

	for _, root := range []common.Hash{roots[0], roots[2], roots[3], roots[4]} {
		build(root)
	}
	require.Zero(t, s.seams.allReads(), "column reads of the four newest blocks")

	build(roots[1])
	require.Equal(t, 1, s.seams.reads(roots[1], 0), "reads of column 0 of the oldest block")
}

func TestBlobsFromColumnsRefusesMoreBlobsThanTheSlotAllows(t *testing.T) {
	s := startSharedBuilds(t, 2, 2, withBlobLimit(1))
	holdEveryPermit(s.d)
	for _, tc := range []struct {
		name         string
		indices      []uint64
		withSidecars bool
	}{
		{"every blob with sidecars", nil, true},
		{"a selection", []uint64{0}, false},
	} {
		_, _, err := s.d.BlobsFromColumns(t.Context(), recoveryTestSlot, s.root, tc.indices, tc.withSidecars)

		require.ErrorContains(t, err, "more than the 1 that slot", tc.name)
		require.NotErrorIs(t, err, ErrBlobsUnavailable, tc.name)
		require.NotErrorIs(t, err, ErrReconstructionBusy, tc.name)
		require.Zero(t, s.seams.reads(s.root, 1), "%s: reads of column 1", tc.name)
	}
	s.requireNothingCached(t)
}

func TestBlobsFromColumnsReadsNothingForACancelledRequest(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		indices      []uint64
		withSidecars bool
	}{
		{nil, true},
		{nil, false},
		{[]uint64{0, 1}, true},
		{[]uint64{0, 1}, false},
	} {
		_, _, err := s.d.BlobsFromColumns(ctx, recoveryTestSlot, s.root, tc.indices, tc.withSidecars)

		require.ErrorIs(t, err, context.Canceled)
	}
	require.Zero(t, s.seams.indexLookups(), "saved-index lookups")
	require.Zero(t, s.seams.allReads(), "column reads")
}

func TestBlobsFromColumnsAnswersAPrunedSlotWithColumnsNotKeptEvenWhenCached(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	_, _, err := s.blobsFromColumns(nil, true)
	require.NoError(t, err)
	require.NoError(t, s.d.PruneBelow(recoveryTestSlot+1))

	_, _, err = s.blobsFromColumns(nil, true)

	require.ErrorIs(t, err, ErrColumnsNotKept)
}

func TestBlobsFromColumnsCachesTwoBlocksOfOneSlotSeparately(t *testing.T) {
	s := startSharedBuilds(t, 2, 2)
	other := s.storeBlock(t, common.Hash{1})
	var built [2][]*cltypes.BlobSidecar
	for i, root := range []common.Hash{s.root, other} {
		_, sidecars, err := s.d.BlobsFromColumns(t.Context(), recoveryTestSlot, root, nil, true)
		require.NoError(t, err)
		built[i] = sidecars
	}

	for i, root := range []common.Hash{s.root, other} {
		s.requireCacheHit(t, root, built[i])
		headerRoot, err := built[i][0].SignedBlockHeader.Header.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, root, common.Hash(headerRoot), "block %d", i)
	}
}

func TestBlobsFromColumnsOfAGloasBlockNeitherReadsNorFillsTheCache(t *testing.T) {
	s := startSharedBuilds(t, 2, 2, func(r *recoveryRun) { forkToGloasAt(t, r, recoveryTestSlot/clparams.MainnetBeaconConfig.SlotsPerEpoch) })
	blobs, _, err := s.blobsFromColumns([]uint64{0, 1}, false)
	require.NoError(t, err)
	require.Len(t, blobs, 2)
	s.requireNothingCached(t)

	planted := make([]*cltypes.BlobSidecar, 2)
	for i := range planted {
		planted[i] = &cltypes.BlobSidecar{Blob: cltypes.Blob{0xff}}
	}
	s.d.builtBlobSidecars.Add(s.root, planted)

	blobs, _, err = s.blobsFromColumns([]uint64{0, 1}, false)

	require.NoError(t, err)
	require.Len(t, blobs, 2)
	for i := range blobs {
		require.True(t, *blobs[i] == s.blobs[i].Blob, "blob %d", i)
	}
}

func TestBlobsFromColumnsAcceptsExactlyTheMaximumBlobCount(t *testing.T) {
	s := startSharedBuilds(t, 2, 2, withBlobLimit(2))

	blobs, _, err := s.blobsFromColumns([]uint64{0, 1}, false)
	require.NoError(t, err)
	require.Len(t, blobs, 2)

	_, sidecars, err := s.blobsFromColumns(nil, true)
	require.NoError(t, err)
	require.Len(t, sidecars, 2)
}
