// Copyright 2025 The Erigon Authors
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
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// TestDomain_RmStateLatestHole reproduces the silent files<->DB gap that
// `snapshots rm-state --latest` opens when run without `stage_exec --reset`:
// once a value is collated into a file and pruned from the DB, deleting that
// newest file leaves the value in neither files nor DB. GetLatest then silently
// returns a STALE older value instead of surfacing the gap.
func TestDomain_RmStateLatestHole(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := t.Context()
	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	const aggStep = uint64(4)
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.AccountsDomain, aggStep, logger)

	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	k := []byte("hole-key")
	v1, v2 := []byte("v1-old"), []byte("v2-latest")

	drt := d.beginForTests()
	w := drt.NewWriter()
	require.NoError(t, w.PutWithPrev(k, v1, 1, nil))        // step 0
	require.NoError(t, w.PutWithPrev(k, v2, aggStep+1, v1)) // step 1 (the true latest)
	require.NoError(t, w.Flush(ctx, tx))
	w.Close()
	drt.Close()

	// Collate both steps into files, then prune the DB so the values live only in files.
	require.NoError(t, d.collateBuildIntegrate(ctx, 0, tx, background.NewProgressSet()))
	require.NoError(t, d.collateBuildIntegrate(ctx, 1, tx, background.NewProgressSet()))
	drt = d.beginForTests()
	_, err = drt.Prune(ctx, tx, 0, 0, 2*aggStep, math.MaxUint64, logEvery)
	require.NoError(t, err)
	drt.Close()

	// Sanity: latest is v2 (from the step-1 file), and no gap is reported.
	drt = d.beginForTests()
	got, _, found, err := drt.GetLatest(k, tx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, v2, got)
	require.NoError(t, drt.checkFilesDBGap(tx), "no gap before the file is removed")
	drt.Close()

	// `rm-state --latest`: drop the newest file (step 1), the one holding v2.
	d.closeFilesAfterStep(1)

	drt = d.beginForTests()
	defer drt.Close()

	// The read itself is silently wrong: v2 is gone from both files and DB, yet
	// GetLatest returns the stale v1 with found=true and no error.
	got, _, found, err = drt.GetLatest(k, tx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, v1, got, "stale value served from below the hole")

	// The gap check catches what GetLatest cannot.
	require.ErrorContains(t, drt.checkFilesDBGap(tx), "gap between snapshot files and DB")
}

// TestInvertedIndex_RmStateLatestHole verifies the standalone-index gap check with
// a real prune. The II has no internal TxTo clamp (unlike a domain); its safety is
// the aggregator invariant that it prunes every index to EndTxNumMinimax, which for
// an aligned index equals its files' end — so a healthy index has TxTo == filesEnd
// and never false-positives. Removing the newest file breaks that and is caught.
func TestInvertedIndex_RmStateLatestHole(t *testing.T) {
	t.Parallel()
	logger := log.New()
	ctx := t.Context()
	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	const aggStep = uint64(4)
	db, ii := testDbAndInvertedIndex(t, aggStep, logger)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	ic := ii.beginForTests()
	w := ic.NewWriter()
	require.NoError(t, w.Add([]byte("k"), 1))         // step 0
	require.NoError(t, w.Add([]byte("k"), aggStep+1)) // step 1
	require.NoError(t, w.Flush(ctx, tx))
	w.close()
	ic.Close()

	require.NoError(t, ii.collateBuildIntegrate(ctx, 0, tx, background.NewProgressSet()))
	require.NoError(t, ii.collateBuildIntegrate(ctx, 1, tx, background.NewProgressSet()))

	ic = ii.beginForTests()
	filesEnd := ic.files.EndTxNum() // = 2*aggStep
	// Prune exactly as the aggregator does: txTo = EndTxNumMinimax, which for an
	// aligned index equals filesEnd. This persists TxTo = filesEnd.
	_, err = ic.TableScanningPrune(ctx, tx, 0, filesEnd, math.MaxUint64, logEvery, false, nil, nil, nil, prune.DefaultStorageMode)
	require.NoError(t, err)
	require.NoError(t, ic.checkFilesDBGap(tx), "healthy: pruned to filesEnd, no gap")
	ic.Close()

	// `rm-state --latest`: drop the newest index file (step 1).
	ii.dirtyFiles.CloseIf(func(item *FilesItem) bool { return item.StartStep(aggStep) >= 1 })

	ic = ii.beginForTests()
	defer ic.Close()
	require.Less(t, ic.files.EndTxNum(), filesEnd, "newest file dropped")
	require.ErrorContains(t, ic.checkFilesDBGap(tx), "gap between snapshot files and DB")
}
