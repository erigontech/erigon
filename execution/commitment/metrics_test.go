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

package commitment

import (
	"sync"
	"testing"
	"time"
)

// A Metrics object enabled for CSV then re-applied with an empty prefix (as
// happens when a pooled HexPatriciaHashed is reused with default config) must
// stop writing CSV, otherwise it keeps appending to a stale file.
func TestMetrics_SetCsvMetricsClearsStaleState(t *testing.T) {
	m := NewMetrics("")
	m.EnableCsvMetrics("/tmp/erigon-commitment-metrics-test")
	if !m.writeCommitmentMetrics || !m.Accounts.writeCommitmentMetrics || !m.Branches.writeCommitmentMetrics {
		t.Fatal("EnableCsvMetrics should turn on write flags")
	}

	m.SetCsvMetrics("")

	wantEnabled := csvMetricsEnvPrefix() != ""
	if m.writeCommitmentMetrics != wantEnabled {
		t.Errorf("after SetCsvMetrics(\"\"): writeCommitmentMetrics=%v, want %v", m.writeCommitmentMetrics, wantEnabled)
	}
	if m.Accounts.writeCommitmentMetrics != wantEnabled || m.Branches.writeCommitmentMetrics != wantEnabled {
		t.Errorf("after SetCsvMetrics(\"\"): account/branch write flags not reset to %v", wantEnabled)
	}
	if !wantEnabled && m.metricsFilePrefix != "" {
		t.Errorf("after SetCsvMetrics(\"\"): prefix should be cleared, got %q", m.metricsFilePrefix)
	}
}

// Concurrent sub-tries share a single Metrics object, so the duration
// accumulators must be safe under concurrent writes (run with -race).
func TestMetrics_ConcurrentDurationAccumulation(t *testing.T) {
	m := NewMetrics("")
	m.collectCommitmentMetrics = true

	const goroutines, perGoroutine = 8, 2000
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range perGoroutine {
				m.StartUnfolding(nil)()
				m.StartFolding(nil)()
				m.TotalProcessingTimeInc(time.Now())
			}
		})
	}
	wg.Wait()
}
