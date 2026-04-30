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

// Storage views contract — scenario suite. Each test corresponds to one
// row in the spec's scenario matrix
// (docs/plans/20260430-storage-views-spec.md, §4). The scenarios drive
// inventory mutations on a deterministic schedule and assert that the
// consumer-facing read API behaves per the contract.
//
// Tests run against the harness's StubInventory + StubReadHandle. Item
// #1 (held-view discipline on the real snapshot.Inventory) and the
// follow-on read-handle work will swap the stubs for the production
// implementations; the scenarios themselves stay unchanged.

package scenarios_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/integration/storage/harness"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/views"
)

// Latency bands. Lower bound is conservative — wall-clock timer
// imprecision plus channel-receive overhead can drop a few ms below the
// scheduled offset on faster systems. Upper bound matches the read's
// ctx deadline.
const (
	scheduleSkew = 15 * time.Millisecond
)

// fileEntry is a small constructor; reduces per-scenario boilerplate.
func fileEntry(name string, domain snapshot.Domain, fromStep, toStep uint64, local bool) *snapshot.FileEntry {
	return &snapshot.FileEntry{
		Name:     name,
		Domain:   domain,
		FromStep: fromStep,
		ToStep:   toStep,
		Local:    local,
	}
}

// 1. Read of fully-local file → Ready immediately.
func TestScenario01_LocalFileReadsImmediately(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	inv.AddFile(fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, true))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	obs := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-accounts.0-256.kv")
	})

	require.NoError(t, obs.Err)
	require.Equal(t, "v1.0-accounts.0-256.kv", obs.Value)
	require.Less(t, obs.Latency, 50*time.Millisecond,
		"local read must return immediately; got %v", obs.Latency)
}

// 2. Read of declared-but-not-local file: file lands inside ctx → Ready.
func TestScenario02_PendingResolvesWhenFileLandsInCtx(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	inv.AddFile(fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, false))

	sched := harness.NewSchedule()
	sched.At(100*time.Millisecond, func() {
		inv.MarkLocal("v1.0-accounts.0-256.kv")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	obs := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-accounts.0-256.kv")
	})
	sched.Wait()

	require.NoError(t, obs.Err)
	require.Equal(t, "v1.0-accounts.0-256.kv", obs.Value)
	require.GreaterOrEqual(t, obs.Latency, 100*time.Millisecond-scheduleSkew,
		"read must wait at least until file lands; got %v", obs.Latency)
	require.Less(t, obs.Latency, 500*time.Millisecond,
		"read must return well before ctx deadline; got %v", obs.Latency)
}

// 3. Read of declared-but-not-local file: ctx expires before file lands → ErrPending.
func TestScenario03_PendingTimesOutWithErrPending(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	inv.AddFile(fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, false))

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	obs := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-accounts.0-256.kv")
	})

	require.Error(t, obs.Err)
	require.True(t, errors.Is(obs.Err, views.ErrPending),
		"expected ErrPending for declared-but-not-local read past ctx; got %v", obs.Err)
	require.GreaterOrEqual(t, obs.Latency, 80*time.Millisecond-scheduleSkew,
		"read must wait until ctx deadline before returning Pending; got %v", obs.Latency)
}

// 4. Read of not-declared file → not-found error, no wait.
func TestScenario04_MissingReturnsNotFoundImmediately(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	obs := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-mystery.0-256.kv")
	})

	require.Error(t, obs.Err)
	require.True(t, errors.Is(obs.Err, harness.ErrNotFound),
		"expected ErrNotFound for missing file; got %v", obs.Err)
	require.Less(t, obs.Latency, 50*time.Millisecond,
		"missing-file read must not wait; got %v", obs.Latency)
}

// 5. View V held during merge: V continues to read constituents from the
// captured pre-merge state; new view sees merged file.
func TestScenario05_HeldViewSeesPreMergeStateThroughTransition(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	inv.AddFile(fileEntry("v1.0-accounts.0-128.kv", snapshot.DomainAccounts, 0, 128, true))
	inv.AddFile(fileEntry("v1.0-accounts.128-256.kv", snapshot.DomainAccounts, 128, 256, true))

	v := inv.View()
	defer v.Close()

	// Mid-test: replace constituents with merged file.
	merged := fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, true)
	require.True(t, inv.ReplaceWithMerge(merged, []string{
		"v1.0-accounts.0-128.kv",
		"v1.0-accounts.128-256.kv",
	}))

	// Held view still reads constituent.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	obs := harness.Observe(func() (string, error) {
		return h.Read(ctx, v, "v1.0-accounts.0-128.kv")
	})
	require.NoError(t, obs.Err, "held view must still see constituent post-merge")
	require.Equal(t, "v1.0-accounts.0-128.kv", obs.Value)

	// Fresh view (no held reference) sees the merged file but not constituents.
	v2 := inv.View()
	defer v2.Close()
	_, hasConstituent := v2.Get("v1.0-accounts.0-128.kv")
	require.False(t, hasConstituent, "fresh view post-merge must NOT see the constituent")
	merged2, hasMerged := v2.Get("v1.0-accounts.0-256.kv")
	require.True(t, hasMerged, "fresh view post-merge must see the merged file")
	require.NotNil(t, merged2)
}

// 6. View V held during eviction: file remains visible to V; on Close,
// pending-delete clears.
func TestScenario06_EvictionDefersUntilHeldViewCloses(t *testing.T) {
	inv := harness.NewStubInventory()

	inv.AddFile(fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, true))

	v := inv.View()

	// Remove while held — pending-delete should accumulate.
	inv.RemoveFile("v1.0-accounts.0-256.kv")
	require.Contains(t, inv.PendingDeletes(), "v1.0-accounts.0-256.kv",
		"removal of held file must enter pending-deletes, not unlink")

	// Held view still references it.
	require.Equal(t, 1, v.RefCount("v1.0-accounts.0-256.kv"),
		"refcount must reflect the one held view")
	e, ok := v.Get("v1.0-accounts.0-256.kv")
	require.True(t, ok)
	require.True(t, e.Local, "captured copy retains its Local=true state")

	// Close → pending-delete clears.
	v.Close()
	require.NotContains(t, inv.PendingDeletes(), "v1.0-accounts.0-256.kv",
		"close of last held view must clear pending-delete")
	require.Equal(t, 0, v.RefCount("v1.0-accounts.0-256.kv"),
		"refcount must drop to zero after close")
}

// 7. Phase-0 latest local; Phase-1 backfill not yet local. Reads of
// latest succeed; reads of older Pending.
func TestScenario07_PhaseBoundaryReadsSplitReadyAndPending(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	// Phase-0 latest: local. Phase-1 older: declared, not local.
	inv.AddFile(fileEntry("v1.0-accounts.512-768.kv", snapshot.DomainAccounts, 512, 768, true))
	inv.AddFile(fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, false))

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	latest := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-accounts.512-768.kv")
	})
	older := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-accounts.0-256.kv")
	})

	require.NoError(t, latest.Err, "Phase-0 latest must read Ready")
	require.Equal(t, "v1.0-accounts.512-768.kv", latest.Value)

	require.Error(t, older.Err)
	require.True(t, errors.Is(older.Err, views.ErrPending),
		"Phase-1 older must read Pending until backfill lands")
}

// 8. After backfill: every range local → all reads Ready.
func TestScenario08_BackfillCompleteAllRangesReady(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	for from, to := uint64(0), uint64(256); to <= 768; from, to = from+256, to+256 {
		inv.AddFile(fileEntry(
			"v1.0-accounts."+itoaRange(from, to)+".kv",
			snapshot.DomainAccounts, from, to, true,
		))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	for from, to := uint64(0), uint64(256); to <= 768; from, to = from+256, to+256 {
		name := "v1.0-accounts." + itoaRange(from, to) + ".kv"
		obs := harness.Observe(func() (string, error) {
			return h.Read(ctx, nil, name)
		})
		require.NoError(t, obs.Err, "post-backfill read of %s must be Ready", name)
		require.Equal(t, name, obs.Value)
		require.Less(t, obs.Latency, 50*time.Millisecond,
			"post-backfill reads must not wait; got %v for %s", obs.Latency, name)
	}
}

// 9. File local but not advertisable → reads Pending until MarkAdvertisable.
func TestScenario09_AdvertisableGateReadsPendingUntilPromoted(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv, RequireAdvertisable: true}

	e := fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, true)
	e.Advertisable = false
	inv.AddFile(e)

	sched := harness.NewSchedule()
	sched.At(100*time.Millisecond, func() {
		inv.MarkAdvertisable("v1.0-accounts.0-256.kv")
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	obs := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-accounts.0-256.kv")
	})
	sched.Wait()

	require.NoError(t, obs.Err, "advertisable promotion must resolve the pending read")
	require.Equal(t, "v1.0-accounts.0-256.kv", obs.Value)
	require.GreaterOrEqual(t, obs.Latency, 100*time.Millisecond-scheduleSkew,
		"read must wait at least until MarkAdvertisable fires; got %v", obs.Latency)
	require.Less(t, obs.Latency, 500*time.Millisecond)
}

// 10. File was local, marked not-local (corruption-detected re-download),
// then local again → reads Pending across the gap, Ready when re-download lands.
func TestScenario10_ReDownloadAfterCorruptionResolvesPending(t *testing.T) {
	inv := harness.NewStubInventory()
	h := &harness.StubReadHandle{Inventory: inv}

	inv.AddFile(fileEntry("v1.0-accounts.0-256.kv", snapshot.DomainAccounts, 0, 256, true))

	sched := harness.NewSchedule()
	// Detection: at t=0 it's still local; corruption marks not-local at t=20ms,
	// re-download lands at t=200ms.
	sched.At(20*time.Millisecond, func() {
		inv.MarkNotLocal("v1.0-accounts.0-256.kv")
	})
	sched.At(200*time.Millisecond, func() {
		inv.MarkLocal("v1.0-accounts.0-256.kv")
	})

	// Read with deadline that survives both transitions.
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	// Wait until mid-gap to issue the read so it definitely starts during
	// the not-local window. This makes the assertion deterministic.
	time.Sleep(60 * time.Millisecond)

	obs := harness.Observe(func() (string, error) {
		return h.Read(ctx, nil, "v1.0-accounts.0-256.kv")
	})
	sched.Wait()

	require.NoError(t, obs.Err, "re-download must resolve the pending read")
	require.Equal(t, "v1.0-accounts.0-256.kv", obs.Value)
	// The read started ~60ms in; lands at ~200ms. So latency ~140ms.
	require.GreaterOrEqual(t, obs.Latency, 100*time.Millisecond-scheduleSkew,
		"read must wait through the not-local window; got %v", obs.Latency)
}

// itoaRange — small helper so scenario 8's loop stays readable.
func itoaRange(from, to uint64) string {
	return strconv.FormatUint(from, 10) + "-" + strconv.FormatUint(to, 10)
}
