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

package state

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type snapshotTestEntry struct {
	key   byte
	txNum uint64
}

func writeSnapshotTestValues(ctx context.Context, db kv.RwDB, dt *DomainRoTx, entries ...snapshotTestEntry) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		w := dt.NewWriter(db)
		defer w.Close()
		for _, e := range entries {
			if err := w.PutWithPrev([]byte{e.key}, []byte("receipt data"), e.txNum, nil); err != nil {
				return err
			}
		}
		return w.Flush(ctx, tx)
	})
}

func requireSnapshotTestValues(t *testing.T, db kv.RoDB, agg *Aggregator, domain kv.Domain, entries ...snapshotTestEntry) {
	t.Helper()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		at := agg.BeginFilesRo()
		defer at.Close()
		for _, e := range entries {
			got, _, found, err := at.GetLatest(domain, []byte{e.key}, tx, kv.GetLatestOptions{})
			require.NoError(t, err)
			require.Truef(t, found, "missing %s key %d written at txNum %d", domain, e.key, e.txNum)
			require.Equal(t, []byte("receipt data"), got)
		}
		return nil
	}))
}

func TestAggregatorBuildKeepsBackfilledReceiptVisible(t *testing.T) {
	const stepSize = uint64(10)
	const buildEnd = 2 * stepSize
	for _, tc := range []struct {
		name      string
		domain    kv.Domain
		receipts  []snapshotTestEntry
		legacy    bool
		mustBuild bool
	}{
		{
			name: "older_receipt_sorts_after_newer_key", domain: kv.ReceiptDomain,
			receipts: []snapshotTestEntry{{key: 2, txNum: 1}, {key: 1, txNum: stepSize}},
		},
		{
			name: "backfilled_receipt_cache", domain: kv.RCacheDomain,
			receipts: []snapshotTestEntry{{key: 0, txNum: 1}},
		},
		{
			name: "receipt_at_start_of_build_step", domain: kv.ReceiptDomain,
			receipts: []snapshotTestEntry{{key: 1, txNum: stepSize}}, mustBuild: true,
		},
		{
			name: "existing_receipts_before_upgrade", domain: kv.ReceiptDomain, legacy: true,
			receipts: []snapshotTestEntry{{key: 2, txNum: 1}, {key: 1, txNum: stepSize}},
		},
		{
			name: "late_receipt_cache_before_upgrade", domain: kv.RCacheDomain, legacy: true,
			receipts: []snapshotTestEntry{{key: 0, txNum: stepSize}}, mustBuild: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			db, agg := testDbAndAggregatorv3(t, stepSize)
			tdb := asTemporalRoDB(db)
			agg.d[tc.domain].Enabled = false
			require.NoError(t, agg.buildFiles(ctx, tdb, 0, unboundedFinalityCtx, nil))
			agg.EnableDomain(tc.domain)
			require.Equal(t, stepSize, agg.EndTxNumMinimax())

			at := agg.BeginFilesRo()
			require.Zero(t, at.TxNumsInFiles(tc.domain))
			require.NoError(t, writeSnapshotTestValues(ctx, db, at.d[tc.domain], tc.receipts...))
			// A write in the next step lets the background builder collate the target step.
			require.NoError(t, writeSnapshotTestValues(ctx, db, at.d[kv.AccountsDomain],
				snapshotTestEntry{1, stepSize}, snapshotTestEntry{2, buildEnd}))
			at.Close()
			if tc.legacy {
				// Simulate rows written before the writer recorded a domain's first step.
				require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error { return tx.ClearTable(kv.TblPruningValsProg) }))
			}

			requireSnapshotTestValues(t, db, agg, tc.domain, tc.receipts...)
			done, started := agg.buildFilesInBackground(tdb, buildEnd, false, unboundedFinalityCtx)
			require.True(t, started)
			<-done
			requireSnapshotTestValues(t, db, agg, tc.domain, tc.receipts...)
			testAggregatorPrune(t, db, agg)
			requireSnapshotTestValues(t, db, agg, tc.domain, tc.receipts...)

			at = agg.BeginFilesRo()
			defer at.Close()
			require.Equal(t, buildEnd, at.TxNumsInFiles(kv.AccountsDomain))
			if tc.mustBuild {
				require.Equal(t, buildEnd, at.TxNumsInFiles(tc.domain), "all receipt data fits in the new snapshot")
			}
		})
	}
}

func TestReceiptSnapshotCannotSkipBackfill(t *testing.T) {
	const stepSize = uint64(10)
	for _, tc := range []struct {
		name          string
		partial       bool
		explicitBuild bool
	}{
		{name: "write_before_publication"},
		{name: "write_before_publication_explicit", explicitBuild: true},
		{name: "partial_backfill", partial: true},
		{name: "partial_backfill_explicit", partial: true, explicitBuild: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			db, agg := testDbAndAggregatorv3(t, stepSize)
			tdb := asTemporalRoDB(db)
			// Receipts being backfilled may lag the state snapshots used for replay.
			defer agg.Unalign(kv.ReceiptDomain)()
			if tc.partial {
				require.NoError(t, agg.buildFiles(ctx, tdb, 0, unboundedFinalityCtx, nil))
			}
			agg.d[kv.ReceiptDomain].Enabled = false
			for step := range kv.Step(2) {
				require.NoError(t, agg.buildFiles(ctx, tdb, step, unboundedFinalityCtx, nil))
			}
			agg.EnableDomain(kv.ReceiptDomain)
			require.Equal(t, 2*stepSize, agg.EndTxNumMinimax())

			at := agg.BeginFilesRo()
			defer at.Close()
			require.NoError(t, writeSnapshotTestValues(ctx, db, at.d[kv.AccountsDomain],
				snapshotTestEntry{1, 2 * stepSize}, snapshotTestEntry{2, 3 * stepSize}))
			entry := snapshotTestEntry{key: 2, txNum: 1}
			if tc.partial {
				entry.txNum += stepSize
				require.NoError(t, writeSnapshotTestValues(ctx, db, at.d[kv.ReceiptDomain], entry))
			}
			hold, release := context.WithTimeout(ctx, time.Minute)
			defer release()
			built := make(chan struct{})
			if !tc.partial {
				agg.SetMaintenanceObserver(func(ctx context.Context, ev MaintenanceEvent) {
					if ev.Point == MaintenanceFilesBuilt {
						close(built)
						select {
						case <-hold.Done():
						case <-ctx.Done():
						}
					}
				})
			}

			if tc.explicitBuild {
				require.NoError(t, agg.BuildFiles2(ctx, tdb, 2, 3, unboundedFinalityCtx, false))
			} else {
				_, started := agg.buildFilesInBackground(tdb, 3*stepSize, false, unboundedFinalityCtx)
				require.True(t, started)
			}
			if !tc.partial {
				select {
				case <-built:
				case <-hold.Done():
					t.Fatal("snapshot build did not reach publication")
				}
				require.NoError(t, writeSnapshotTestValues(ctx, db, at.d[kv.ReceiptDomain], entry))
				release()
			}
			agg.background.Wait()
			requireSnapshotTestValues(t, db, agg, kv.ReceiptDomain, entry)
			testAggregatorPrune(t, db, agg)
			requireSnapshotTestValues(t, db, agg, kv.ReceiptDomain, entry)

			files := agg.BeginFilesRo()
			next := files.d[kv.ReceiptDomain].FirstStepNotInFiles()
			require.Equal(t, kv.Step(entry.txNum/stepSize), next)
			require.Equal(t, 3*stepSize, files.TxNumsInFiles(kv.AccountsDomain))
			files.Close()

			agg.SetMaintenanceObserver(nil)
			require.NoError(t, agg.BuildFiles2(ctx, tdb, next, 3, unboundedFinalityCtx, false))
			agg.background.Wait()
			testAggregatorPrune(t, db, agg)
			requireSnapshotTestValues(t, db, agg, kv.ReceiptDomain, entry)
			files = agg.BeginFilesRo()
			defer files.Close()
			require.Equal(t, 3*stepSize, files.TxNumsInFiles(kv.ReceiptDomain))
		})
	}
}

func TestLateFirstSnapshotProtectsAgainstStaleWriter(t *testing.T) {
	const stepSize = uint64(10)
	ctx := t.Context()
	db, agg := testDbAndAggregatorv3(t, stepSize)
	// A second writer can still have a view with no files after the builder publishes.
	before := agg.BeginFilesRo()
	defer before.Close()
	require.NoError(t, writeSnapshotTestValues(ctx, db, before.d[kv.ReceiptDomain], snapshotTestEntry{1, 24}))
	require.NoError(t, agg.buildFiles(ctx, asTemporalRoDB(db), 2, unboundedFinalityCtx, nil))
	files := agg.BeginFilesRo()
	require.Equal(t, 3*stepSize, files.TxNumsInFiles(kv.ReceiptDomain))
	files.Close()

	for _, txNum := range []uint64{1, 20, 29} {
		err := writeSnapshotTestValues(ctx, db, before.d[kv.ReceiptDomain], snapshotTestEntry{2, txNum})
		require.ErrorContains(t, err, "first snapshot")
	}
	next := snapshotTestEntry{2, 30}
	require.NoError(t, writeSnapshotTestValues(ctx, db, before.d[kv.ReceiptDomain], next))
	requireSnapshotTestValues(t, db, agg, kv.ReceiptDomain, snapshotTestEntry{1, 24}, next)
}

func TestFirstSnapshotReservationSurvivesRestart(t *testing.T) {
	const stepSize = uint64(10)
	ctx := t.Context()
	db, agg := testDbAndAggregatorv3(t, stepSize)
	tdb := asTemporalRoDB(db)
	at := agg.BeginFilesRo()
	require.NoError(t, writeSnapshotTestValues(ctx, db, at.d[kv.ReceiptDomain], snapshotTestEntry{1, 24}))
	at.Close()

	// A crash can leave the MDBX reservation durable before the first file exists.
	ready, err := agg.d[kv.ReceiptDomain].prepareSnapshotStep(ctx, tdb, 0, 2)
	require.NoError(t, err)
	require.True(t, ready)
	dirs := agg.Dirs()
	agg.Close()
	reopened := NewTest(dirs).StepSize(stepSize).MustOpen(ctx)
	t.Cleanup(reopened.Close)
	require.NoError(t, reopened.OpenFolder(db))

	require.NoError(t, reopened.buildFiles(ctx, tdb, 0, unboundedFinalityCtx, nil))
	at = reopened.BeginFilesRo()
	require.Zero(t, at.TxNumsInFiles(kv.ReceiptDomain), "the reserved domain must keep its original start")
	err = writeSnapshotTestValues(ctx, db, at.d[kv.ReceiptDomain], snapshotTestEntry{2, 1})
	require.ErrorContains(t, err, "first snapshot")
	at.Close()
	require.NoError(t, reopened.BuildFiles2(ctx, tdb, 1, 3, unboundedFinalityCtx, false))
	reopened.background.Wait()
	at = reopened.BeginFilesRo()
	require.Equal(t, 3*stepSize, at.TxNumsInFiles(kv.ReceiptDomain))
	at.Close()
	requireSnapshotTestValues(t, db, reopened, kv.ReceiptDomain, snapshotTestEntry{1, 24})
}

type backfillBeforeSnapshotReservation struct {
	temporalRoDBStub
	backfill func() error
}

func (db backfillBeforeSnapshotReservation) Update(ctx context.Context, f func(kv.RwTx) error) error {
	if err := db.backfill(); err != nil {
		return err
	}
	return db.RwDB.Update(ctx, f)
}

func TestSnapshotStartMovesToEarlierCommittedBatch(t *testing.T) {
	const stepSize = uint64(10)
	ctx := t.Context()
	db, agg := testDbAndAggregatorv3(t, stepSize)
	at := agg.BeginFilesRo()
	defer at.Close()
	storage := at.d[kv.StorageDomain]
	require.NoError(t, writeSnapshotTestValues(ctx, db, storage, snapshotTestEntry{1, 24}))

	// Step 2 is eligible in the initial read. Commit an earlier batch immediately
	// before the builder gets the MDBX writer lock to reserve it.
	racingDB := backfillBeforeSnapshotReservation{
		temporalRoDBStub: temporalRoDBStub{RwDB: db},
		backfill: func() error {
			return writeSnapshotTestValues(ctx, db, storage, snapshotTestEntry{2, 11})
		},
	}
	ready, err := storage.d.prepareSnapshotStep(ctx, racingDB, 0, 2)
	require.NoError(t, err)
	require.False(t, ready)
	requireSnapshotTestValues(t, db, agg, kv.StorageDomain, snapshotTestEntry{2, 11})
	tdb := asTemporalRoDB(db)
	ready, err = storage.d.prepareSnapshotStep(ctx, tdb, 0, 1)
	require.NoError(t, err)
	require.True(t, ready)

	// Resetting the source tables also releases a reservation left before file creation.
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for _, table := range storage.d.Tables() {
			if err := tx.ClearTable(table); err != nil {
				return err
			}
			if err := ResetSnapshotProgress(tx, table); err != nil {
				return err
			}
		}
		return nil
	}))
	entry := snapshotTestEntry{2, 1}
	require.NoError(t, writeSnapshotTestValues(ctx, db, storage, entry))
	requireSnapshotTestValues(t, db, agg, kv.StorageDomain, entry)
}
