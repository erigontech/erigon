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

package lifecycle

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// fakeBuilder implements IndexBuilder by adding the named dependency
// files to an inventory as if they had been built and propagated.
// onBuild is called before the deps are added so tests can inject
// failures.
type fakeBuilder struct {
	inv     *snapshot.Inventory
	onBuild func(*snapshot.FileEntry) error
	calls   int
}

func (f *fakeBuilder) BuildMissedIndices(_ context.Context, primary *snapshot.FileEntry) error {
	f.calls++
	if f.onBuild != nil {
		if err := f.onBuild(primary); err != nil {
			return err
		}
	}
	for _, depName := range primary.Dependencies {
		_ = f.inv.AddFile(&snapshot.FileEntry{
			Name:   depName,
			Domain: primary.Domain,
			Local:  true,
		})
	}
	return nil
}

func TestBuildOnIndexing_BuildsThenAdvancesWhenDepsMissing(t *testing.T) {
	// Deps are NOT pre-added to the inventory, so the handler invokes
	// the builder. fakeBuilder simulates production behaviour by
	// adding the deps as Local during the call. Post-build the
	// pre-check sees them present and advances.
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:         "v1.0-accounts.0-256.kv",
		Domain:       snapshot.DomainAccounts,
		Local:        true,
		Dependencies: []string{"v1.0-accounts.0-256.kvi"},
	}
	_ = inv.AddFile(primary)

	builder := &fakeBuilder{inv: inv}
	handler := BuildOnIndexing(builder, inv, "", nil, nil)

	require.NoError(t, handler(context.Background(), primary))
	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state,
		"primary advances after the build call propagates deps")
	require.Equal(t, 1, builder.calls,
		"missing deps require a build invocation")
}

func TestBuildOnIndexing_DepsAlreadyLocal_SkipsBuild(t *testing.T) {
	// §5c pre-check: when all deps are already Local in the
	// inventory, the handler advances WITHOUT invoking the global
	// builder. Eliminates the 776→22 redundant invocations from the
	// pre-fix hoodi run.
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:         "v1.0-accounts.0-256.kv",
		Domain:       snapshot.DomainAccounts,
		Local:        true,
		Dependencies: []string{"v1.0-accounts.0-256.kvi"},
	}
	_ = inv.AddFile(primary)
	_ = inv.AddFile(&snapshot.FileEntry{
		Name:   "v1.0-accounts.0-256.kvi",
		Domain: snapshot.DomainAccounts,
		Local:  true,
	})

	builder := &fakeBuilder{inv: inv}
	require.NoError(t, BuildOnIndexing(builder, inv, "", nil, nil)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state)
	require.Equal(t, 0, builder.calls,
		"pre-check must skip the builder when deps already Local")
}

func TestBuildOnIndexing_HandlesNoDependencies(t *testing.T) {
	// Files with no deps (caplin / meta / salt) advance directly.
	// The pre-check trivially passes for an empty Dependencies slice,
	// so the builder is not called.
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:  "salt-state.txt",
		Kind:  snapshot.KindSalt,
		Local: true,
	}
	_ = inv.AddFile(primary)

	builder := &fakeBuilder{inv: inv}
	require.NoError(t, BuildOnIndexing(builder, inv, "", nil, nil)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state)
	require.Equal(t, 0, builder.calls,
		"no deps means no build call")
}

// Without a snapshot dir the handler cannot ask the disk, so it falls back to
// Dependencies — which block .seg files never carry: preverified.toml lists
// primaries only, and entryFromPreverifiedItem populates nothing. Production
// passes a snapshot dir and takes the disk path instead; see
// TestBuildOnIndexing_AccessorMissingDoesNotAdvance for what it decides there.
func TestBuildOnIndexing_HandlesNoDependencies_BlockFile(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:  "v1.1-025050-025060-headers.seg",
		Local: true,
	}
	_ = inv.AddFile(primary)

	builder := &fakeBuilder{inv: inv}
	require.NoError(t, BuildOnIndexing(builder, inv, "", nil, nil)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state,
		"with no snapshot dir to ask, empty Dependencies is all the handler has to go on")
	require.Equal(t, 0, builder.calls, "nothing to build for satisfied deps")
}

func TestBuildOnIndexing_BuilderErrorPropagates(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:         "v1.0-accounts.0-256.kv",
		Domain:       snapshot.DomainAccounts,
		Local:        true,
		Dependencies: []string{"v1.0-accounts.0-256.kvi"},
	}
	_ = inv.AddFile(primary)

	wantErr := errors.New("simulated build failure")
	builder := &fakeBuilder{
		inv: inv,
		onBuild: func(_ *snapshot.FileEntry) error {
			return wantErr
		},
	}

	err := BuildOnIndexing(builder, inv, "", nil, nil)(context.Background(), primary)
	require.ErrorIs(t, err, wantErr)

	// State stays at Downloaded — driver leaves it there for retry.
	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleDownloaded, state)
}

func TestBuildOnIndexing_DepsNotYetPropagatedNoOps(t *testing.T) {
	// Build "succeeds" but the dep is NOT added to inventory (e.g.
	// production's OnFilesChange path hasn't run yet). Handler must
	// return nil without advancing — it will retry on next sweep.
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:         "v1.0-accounts.0-256.kv",
		Domain:       snapshot.DomainAccounts,
		Local:        true,
		Dependencies: []string{"v1.0-accounts.0-256.kvi"},
	}
	_ = inv.AddFile(primary)

	// noopIndexBuilder reports success without producing the dep.
	// Handler should return nil but NOT advance.
	require.NoError(t, BuildOnIndexing(noopIndexBuilder{}, inv, "", nil, nil)(
		context.Background(), primary,
	))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleDownloaded, state,
		"missing dep means handler does not advance; next sweep retries")
}

func TestBuildOnIndexing_PartialDepsNoOps(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:   "v1.0-history.0-256.v",
		Domain: snapshot.DomainAccounts,
		Local:  true,
		// Two deps; only one will be present.
		Dependencies: []string{
			"v1.0-history.0-256.ef",
			"v1.0-history.0-256.efi",
		},
	}
	_ = inv.AddFile(primary)
	// Pre-add only one of the two deps.
	_ = inv.AddFile(&snapshot.FileEntry{
		Name:   "v1.0-history.0-256.ef",
		Domain: snapshot.DomainAccounts,
		Local:  true,
	})

	// Builder no-ops (doesn't add anything). Use a builder with no
	// auto-add behaviour.
	noopBuilder := noopIndexBuilder{}
	require.NoError(t, BuildOnIndexing(noopBuilder, inv, "", nil, nil)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleDownloaded, state,
		"partial deps must not advance; both must be Local")
}

type noopIndexBuilder struct{}

func (noopIndexBuilder) BuildMissedIndices(_ context.Context, _ *snapshot.FileEntry) error {
	return nil
}

// countingNoopBuilder reports success without producing anything, and counts
// how often it was asked.
type countingNoopBuilder struct{ calls int }

func (b *countingNoopBuilder) BuildMissedIndices(_ context.Context, _ *snapshot.FileEntry) error {
	b.calls++
	return nil
}

func writeFile(t *testing.T, snapDir, name string) {
	t.Helper()
	path := filepath.Join(snapDir, filepath.FromSlash(name))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("x"), 0o644))
}

// Indexed is the gate the post-Indexed readiness check waits on, so it must
// mean the accessors exist — not merely that the primary landed. Dependencies
// cannot carry that: an accessor takes its own version prefix, so its name is
// not derivable from the primary's, and nothing populates the field.
func TestBuildOnIndexing_AccessorMissingDoesNotAdvance(t *testing.T) {
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{Name: "v1.1-025050-025060-headers.seg", Local: true}
	require.NoError(t, inv.AddFile(primary))
	writeFile(t, snapDir, primary.Name)

	builder := &countingNoopBuilder{}
	require.NoError(t, BuildOnIndexing(builder, inv, snapDir, nil, nil)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleDownloaded, state,
		"a primary whose accessor was never built must not count as Indexed")
}

func TestBuildOnIndexing_AccessorPresentAdvances(t *testing.T) {
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{Name: "v1.1-025050-025060-headers.seg", Local: true}
	require.NoError(t, inv.AddFile(primary))
	writeFile(t, snapDir, primary.Name)
	writeFile(t, snapDir, "v2.0-025050-025060-headers.idx")

	builder := &countingNoopBuilder{}
	require.NoError(t, BuildOnIndexing(builder, inv, snapDir, nil, nil)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state)
	require.Zero(t, builder.calls, "accessor already on disk — no scan needed")
}

// The builder is one global scan over every entity, so a second call within the
// same sweep runs over inputs the first already covered. Files are dispatched
// one at a time, so without a per-sweep latch a bootstrap with hundreds of
// missing accessors costs hundreds of scans.
func TestBuildOnIndexing_OneScanPerSweep(t *testing.T) {
	snapDir := t.TempDir()
	inv := snapshot.NewInventory()
	primaries := []*snapshot.FileEntry{
		{Name: "v1.1-025050-025060-headers.seg", Local: true},
		{Name: "v1.1-025050-025060-bodies.seg", Local: true},
		{Name: "v1.1-025060-025070-headers.seg", Local: true},
		{Name: "v1.1-025060-025070-bodies.seg", Local: true},
	}
	for _, p := range primaries {
		require.NoError(t, inv.AddFile(p))
		writeFile(t, snapDir, p.Name)
	}

	builder := &countingNoopBuilder{}
	epoch := uint64(1)
	handler := BuildOnIndexing(builder, inv, snapDir, func() uint64 { return epoch }, nil)

	for _, p := range primaries {
		require.NoError(t, handler(context.Background(), p))
	}
	require.Equal(t, 1, builder.calls,
		"four primaries in one sweep must cost one global scan, not four")

	epoch = 2
	for _, p := range primaries {
		require.NoError(t, handler(context.Background(), p))
	}
	require.Equal(t, 2, builder.calls,
		"the next sweep gets its own scan — accessors are still missing")
}
