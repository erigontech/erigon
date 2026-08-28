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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

// TestDomain_PrunableStepsGauge pins domain_prunable to the number of steps the
// values table lags the snapshot files by. Commitment is the domain whose history
// is off by default: its history keys table stays empty, so a backlog derived from
// that table counts every step ever written.
func TestDomain_PrunableStepsGauge(t *testing.T) {
	// No t.Parallel: the assertions read process-global metric gauges.
	commitment := statecfg.Schema.CommitmentDomain
	require.True(t, commitment.Hist.HistoryDisabled, "commitment history is off by default")

	withHistory := commitment
	withHistory.Hist.HistoryDisabled = false
	withHistory.Hist.SnapshotsDisabled = false

	t.Run("commitment", func(t *testing.T) {
		requirePrunableGauge(t, commitment, mxPrunableDComm)
	})
	t.Run("commitment with history", func(t *testing.T) {
		requirePrunableGauge(t, withHistory, mxPrunableDComm)
	})
	t.Run("accounts", func(t *testing.T) {
		requirePrunableGauge(t, statecfg.Schema.AccountsDomain, mxPrunableDAcc)
	})
}

// requirePrunableGauge writes 5 steps, collates and prunes the first 3, then
// collates the remaining 2 without pruning them, asserting the gauge after each
// stage. The sentinel makes a gauge that is never written fail loudly.
func requirePrunableGauge(t *testing.T, cfg statecfg.DomainCfg, gauge metrics.Gauge) {
	t.Helper()

	const aggStep = uint64(4)
	const sentinel = float64(999)
	const prunedSteps, totalSteps = kv.Step(3), kv.Step(5)

	ctx := t.Context()
	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	db, d := testDbAndDomainOfStep(t, cfg, aggStep, log.New())
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	drt := d.beginForTests()
	w := drt.NewWriter()
	prev := map[string][]byte{}
	for txNum := range uint64(totalSteps) * aggStep {
		k := fmt.Appendf(nil, "key-%d", txNum%3)
		v := fmt.Appendf(nil, "val-%d", txNum)
		require.NoError(t, w.PutWithPrev(k, v, txNum, prev[string(k)]))
		prev[string(k)] = v
	}
	require.NoError(t, w.Flush(ctx, tx))
	w.Close()
	drt.Close()

	for step := range prunedSteps {
		require.NoError(t, d.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}
	drt = d.beginForTests()
	_, err = drt.Prune(ctx, tx, prunedSteps-1, 0, uint64(prunedSteps)*aggStep, math.MaxUint64, logEvery)
	require.NoError(t, err)
	drt.Close()

	gauge.Set(sentinel)
	drt = d.beginForTests()
	drt.canScanPruneDomainTables(tx, uint64(prunedSteps)*aggStep)
	drt.Close()
	require.Equal(t, uint64(0), gauge.GetValueUint64(), "values table pruned up to the files' end")

	for step := prunedSteps; step < totalSteps; step++ {
		require.NoError(t, d.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}
	gauge.Set(sentinel)
	drt = d.beginForTests()
	defer drt.Close()
	drt.canScanPruneDomainTables(tx, uint64(totalSteps)*aggStep)
	require.Equal(t, uint64(totalSteps-prunedSteps), gauge.GetValueUint64(), "collated but not pruned")
}

// TestDomain_PrunableGaugeInterruptedRotation pins the reading when a value scan
// is cut short. prg.TxTo stores the rotation's target even then, so trusting it
// would report a cleared backlog while the whole span is still in the table.
func TestDomain_PrunableGaugeInterruptedRotation(t *testing.T) {
	const aggStep = uint64(4)
	const totalSteps = kv.Step(5)

	ctx := t.Context()
	logEvery := time.NewTicker(time.Hour)
	defer logEvery.Stop()

	db, d := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, aggStep, log.New())
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	drt := d.beginForTests()
	w := drt.NewWriter()
	prev := map[string][]byte{}
	for txNum := range uint64(totalSteps) * aggStep {
		k := fmt.Appendf(nil, "key-%d", txNum%3)
		v := fmt.Appendf(nil, "val-%d", txNum)
		require.NoError(t, w.PutWithPrev(k, v, txNum, prev[string(k)]))
		prev[string(k)] = v
	}
	require.NoError(t, w.Flush(ctx, tx))
	w.Close()
	drt.Close()

	for step := range totalSteps {
		require.NoError(t, d.collateBuildIntegrate(ctx, step, tx, background.NewProgressSet()))
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	drt = d.beginForTests()
	_, err = drt.Prune(cancelled, tx, totalSteps-1, 0, uint64(totalSteps)*aggStep, math.MaxUint64, logEvery)
	require.NoError(t, err)
	drt.Close()

	prg, err := GetPruneValProgress(tx, []byte(d.ValuesTable))
	require.NoError(t, err)
	require.NotEqual(t, prune.Done, prg.ValueProgress, "scan was cut short")
	require.Equal(t, uint64(totalSteps)*aggStep, prg.TxTo, "target is stored regardless")

	mxPrunableDComm.Set(999)
	drt = d.beginForTests()
	defer drt.Close()
	drt.canScanPruneDomainTables(tx, uint64(totalSteps)*aggStep)
	require.Equal(t, uint64(totalSteps), mxPrunableDComm.GetValueUint64(), "unfinished rotation bounds nothing")
}
