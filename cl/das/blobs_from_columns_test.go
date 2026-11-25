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
	"os"
	"sync"
	"sync/atomic"
	"testing"
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
		{"no indices", nil, nil},
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
	require.Zero(t, blobReconstructions.waiting.Load(), "waiting requests")
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
	require.Len(t, records, 1)
	require.Equal(t, log.LvlWarn, records[0].Lvl)
	require.Equal(t, errColumnSingleFailed, logContextValue(records[0], "err"))
	g.d.recoveringMutex.Lock()
	defer g.d.recoveringMutex.Unlock()
	require.NotContains(t, g.d.isRecovering, g.root)
}

// gatedColumnReads holds every column read until the gate is closed.
type gatedColumnReads struct {
	blob_storage.DataColumnStorage
	gate        chan struct{}
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	mu          sync.Mutex
	reads       map[common.Hash]int
}

func (s *gatedColumnReads) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	inFlight := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)
	for {
		peak := s.maxInFlight.Load()
		if inFlight <= peak || s.maxInFlight.CompareAndSwap(peak, inFlight) {
			break
		}
	}
	s.mu.Lock()
	s.reads[blockRoot]++
	s.mu.Unlock()
	<-s.gate
	return s.DataColumnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, columnIndex)
}

func (s *gatedColumnReads) readsOf(blockRoot common.Hash) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads[blockRoot]
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
	previous := blobReconstructions
	blobReconstructions = newReconstructionAdmission(permits)
	t.Cleanup(func() { blobReconstructions = previous })
	h := &heldReconstructions{results: make(chan error, 16)}
	h.columns = &gatedColumnReads{gate: make(chan struct{}), reads: make(map[common.Hash]int)}
	h.gossipRecovery = startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
		h.columns.DataColumnStorage = r.d.columnStorage
		r.d.columnStorage = h.columns
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
		return h.columns.inFlight.Load() == running && blobReconstructions.waiting.Load() == waiting
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

func TestBlobsFromColumnsRunsAtMostThePermittedReconstructionsAtOnce(t *testing.T) {
	h := holdReconstructions(t, 2)
	for range 4 {
		h.start(t.Context(), h.root)
	}
	h.awaitAdmission(t, 2, 2)
	require.Never(t, func() bool { return h.columns.inFlight.Load() > 2 }, 100*time.Millisecond, time.Millisecond)

	h.release()

	for range 4 {
		require.NoError(t, h.requireResult(t))
	}
	require.Equal(t, int32(2), h.columns.maxInFlight.Load())
}

func TestBlobsFromColumnsRefusesARequestWhenTheWaitersAreFull(t *testing.T) {
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
}

func TestBlobsFromColumnsReadsNoColumnForARequestCancelledWhileWaiting(t *testing.T) {
	h := holdReconstructions(t, 1)
	h.start(t.Context(), h.root)
	h.awaitAdmission(t, 1, 0)
	otherRoot := common.Hash{0x42}
	ctx, cancel := context.WithCancel(t.Context())
	h.start(ctx, otherRoot)
	h.awaitAdmission(t, 1, 1)

	cancel()

	require.ErrorIs(t, h.requireResult(t), context.Canceled)
	require.Equal(t, int32(0), blobReconstructions.waiting.Load())
	require.Zero(t, h.columns.readsOf(otherRoot))
	h.release()
	require.NoError(t, h.requireResult(t))
}

// cancelOnColumnRead cancels a context at its at-th column read.
type cancelOnColumnRead struct {
	blob_storage.DataColumnStorage
	cancel context.CancelFunc
	at     int32
	reads  atomic.Int32
}

func (s *cancelOnColumnRead) ReadColumnSidecarByColumnIndex(ctx context.Context, slot uint64, blockRoot common.Hash, columnIndex int64) (*cltypes.DataColumnSidecar, error) {
	if s.reads.Add(1) == s.at {
		s.cancel()
	}
	return s.DataColumnStorage.ReadColumnSidecarByColumnIndex(ctx, slot, blockRoot, columnIndex)
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
			var columns *cancelOnColumnRead
			g := startGossipRecovery(t, false, atFuluFromGenesis, func(r *recoveryRun) {
				columns = &cancelOnColumnRead{DataColumnStorage: r.d.columnStorage, cancel: cancel, at: tc.at}
				r.d.columnStorage = columns
			})

			blobs, sidecars, err := g.d.BlobsFromColumns(ctx, recoveryTestSlot, g.root, []uint64{0, 1}, tc.withSidecars)

			require.ErrorIs(t, err, context.Canceled)
			require.Nil(t, blobs)
			require.Nil(t, sidecars)
			require.Equal(t, tc.at, columns.reads.Load(), "column reads")
		})
	}
}
