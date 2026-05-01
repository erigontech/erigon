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
		f.inv.AddFile(&snapshot.FileEntry{
			Name:   depName,
			Domain: primary.Domain,
			Local:  true,
		})
	}
	return nil
}

func TestBuildOnIndexing_AdvancesToIndexedWhenDepsPresent(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:         "v1.0-accounts.0-256.kv",
		Domain:       snapshot.DomainAccounts,
		Local:        true, // → derives LifecycleDownloaded
		Dependencies: []string{"v1.0-accounts.0-256.kvi"},
	}
	inv.AddFile(primary)

	builder := &fakeBuilder{inv: inv}
	handler := BuildOnIndexing(builder, inv)

	require.NoError(t, handler(context.Background(), primary))
	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state,
		"primary must advance to Indexed once its deps are Local")
	require.Equal(t, 1, builder.calls)
}

func TestBuildOnIndexing_HandlesNoDependencies(t *testing.T) {
	// Files with no deps (caplin / meta / salt) advance directly.
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:  "salt-state.txt",
		Kind:  snapshot.KindSalt,
		Local: true,
	}
	inv.AddFile(primary)

	builder := &fakeBuilder{inv: inv}
	require.NoError(t, BuildOnIndexing(builder, inv)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleIndexed, state,
		"file with no deps advances immediately after the build call")
}

func TestBuildOnIndexing_BuilderErrorPropagates(t *testing.T) {
	inv := snapshot.NewInventory()
	primary := &snapshot.FileEntry{
		Name:         "v1.0-accounts.0-256.kv",
		Domain:       snapshot.DomainAccounts,
		Local:        true,
		Dependencies: []string{"v1.0-accounts.0-256.kvi"},
	}
	inv.AddFile(primary)

	wantErr := errors.New("simulated build failure")
	builder := &fakeBuilder{
		inv: inv,
		onBuild: func(_ *snapshot.FileEntry) error {
			return wantErr
		},
	}

	err := BuildOnIndexing(builder, inv)(context.Background(), primary)
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
	inv.AddFile(primary)

	// noopIndexBuilder reports success without producing the dep.
	// Handler should return nil but NOT advance.
	require.NoError(t, BuildOnIndexing(noopIndexBuilder{}, inv)(
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
	inv.AddFile(primary)
	// Pre-add only one of the two deps.
	inv.AddFile(&snapshot.FileEntry{
		Name:   "v1.0-history.0-256.ef",
		Domain: snapshot.DomainAccounts,
		Local:  true,
	})

	// Builder no-ops (doesn't add anything). Use a builder with no
	// auto-add behaviour.
	noopBuilder := noopIndexBuilder{}
	require.NoError(t, BuildOnIndexing(noopBuilder, inv)(context.Background(), primary))

	state, _ := inv.LifecycleState(primary.Name)
	require.Equal(t, snapshot.LifecycleDownloaded, state,
		"partial deps must not advance; both must be Local")
}

type noopIndexBuilder struct{}

func (noopIndexBuilder) BuildMissedIndices(_ context.Context, _ *snapshot.FileEntry) error {
	return nil
}
