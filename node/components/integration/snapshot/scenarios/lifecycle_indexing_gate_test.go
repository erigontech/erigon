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

package scenarios

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/lifecycle"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// countingBuilder stands in for productionIndexBuilder: it records how often it
// was asked to work and produces the accessors the primaries are missing.
type countingBuilder struct {
	snapDir   string
	accessors map[string]string // primary name -> accessor name it produces
	calls     atomic.Int32
}

func (b *countingBuilder) BuildMissedIndices(_ context.Context, primary *snapshot.FileEntry) error {
	b.calls.Add(1)
	if primary == nil {
		return nil
	}
	if acc, ok := b.accessors[primary.Name]; ok {
		return os.WriteFile(filepath.Join(b.snapDir, acc), []byte("accessor"), 0o644)
	}
	return nil
}

// buildIndexingStack is buildPublisherStack with the REAL OnIndexing handler.
// The shared harness stubs that handler out — it advances straight to Indexed
// to "simulate deps already local" — so no scenario covers the production
// indexing path. Both of the regressions this file guards slipped through
// because of that.
func buildIndexingStack(t *testing.T, ctx context.Context, snapDir string, builder lifecycle.IndexBuilder) (*lifecycle.Driver, *snapshot.Inventory) {
	t.Helper()
	inv := snapshot.NewInventory()
	driver := &lifecycle.Driver{
		Inv:           inv,
		SnapDir:       snapDir,
		SweepInterval: 50 * time.Millisecond,
		OnValidation: lifecycle.BuildOnBatchValidation(
			validation.StepChain{validation.AllFilesPresent{SnapDir: snapDir}}, inv, nil),
	}
	driver.OnIndexing = lifecycle.BuildOnIndexing(builder, inv, snapDir, driver.SweepEpoch, nil)
	require.NoError(t, driver.Start(ctx))
	t.Cleanup(driver.Stop)
	return driver, inv
}

// A primary whose accessor is absent must not be stranded. Whatever the
// indexing handler decides, the file either gets its accessor and moves on or
// stays retryable — it must never sit quarantined, because a quarantined file
// never advances and the post-Indexed gate waits on every phase-1 file.
func TestIndexingGate_MissingAccessorIsNotStranded(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	snapDir := t.TempDir()

	primary := "v1.1-025020-025030-headers.seg"
	builder := &countingBuilder{
		snapDir:   snapDir,
		accessors: map[string]string{primary: "v2.0-025020-025030-headers.idx"},
	}
	driver, inv := buildIndexingStack(t, ctx, snapDir, builder)

	require.NoError(t, os.WriteFile(filepath.Join(snapDir, primary), []byte("x"), 0o644))

	require.Eventually(t, func() bool {
		state, ok := inv.LifecycleState(primary)
		return ok && state >= snapshot.LifecycleIndexed
	}, 5*time.Second, 25*time.Millisecond,
		"primary must reach Indexed; it is stuck at %v", func() string {
			state, ok := inv.LifecycleState(primary)
			if !ok {
				return "not-in-inventory"
			}
			return state.String()
		}())

	require.False(t, driver.IsQuarantined(primary),
		"a primary whose accessor was missing must not be quarantined")
}

// The index builder is a global scan over every entity, so the indexing handler
// must not treat it as per-file work. Four primaries landing together must not
// cost four scans each sweep.
func TestIndexingGate_BuilderIsNotInvokedPerFilePerSweep(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	snapDir := t.TempDir()

	primaries := []string{
		"v1.1-025020-025030-headers.seg",
		"v1.1-025020-025030-bodies.seg",
		"v1.1-025030-025040-headers.seg",
		"v1.1-025030-025040-bodies.seg",
	}
	accessors := map[string]string{}
	for _, p := range primaries {
		accessors[p] = snapshot.RelPathForName(p[:len(p)-len(".seg")] + ".idx")
	}
	builder := &countingBuilder{snapDir: snapDir, accessors: accessors}
	_, inv := buildIndexingStack(t, ctx, snapDir, builder)

	for _, p := range primaries {
		require.NoError(t, os.WriteFile(filepath.Join(snapDir, p), []byte("x"), 0o644))
	}

	require.Eventually(t, func() bool {
		for _, p := range primaries {
			state, ok := inv.LifecycleState(p)
			if !ok || state < snapshot.LifecycleIndexed {
				return false
			}
		}
		return true
	}, 5*time.Second, 25*time.Millisecond, "all primaries should reach Indexed")

	// Let several more sweeps run: a handler that asks the builder on every
	// sweep keeps climbing after the files have settled.
	settled := builder.calls.Load()
	time.Sleep(500 * time.Millisecond)
	require.LessOrEqual(t, builder.calls.Load(), settled,
		"builder must not be re-invoked once the files have settled")
	require.LessOrEqual(t, int(settled), len(primaries),
		"builder invoked %d times for %d primaries — that is per-file-per-sweep work against a global scan",
		settled, len(primaries))
}
