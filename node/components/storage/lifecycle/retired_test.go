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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	dirutil "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// A merge retires the files it consolidated: the downloader drops their
// registration and sidecar, and the merger unlinks them a moment later. The
// scan runs throughout that gap and sees the file still on disk, so without a
// mark it adds it back as newly discovered — it is re-seeded, its sidecar is
// rebuilt, and the manifest goes on advertising a file that is about to
// disappear. Peers then ask for what nobody has.
func TestSweepDoesNotResurrectRetiredFile(t *testing.T) {
	dir := t.TempDir()
	const name = "v1.1-003600-003628-transactions.seg"
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte("merged away"), 0o644))

	inv := snapshot.NewInventory()
	d := &Driver{Inv: inv, SnapDir: dir}

	d.Sweep(context.Background(), nil)
	_, ok := inv.LifecycleState(name)
	require.True(t, ok, "a local file is discovered normally")

	inv.RetireFile(name)
	d.Sweep(context.Background(), nil)
	_, ok = inv.LifecycleState(name)
	require.False(t, ok, "retired: still on disk, but the merger is about to unlink it")

	// Once it is gone the mark has served its purpose, so a file that later
	// takes the same name is discovered like any other.
	require.NoError(t, dirutil.RemoveFile(path))
	d.Sweep(context.Background(), nil)
	require.NoError(t, os.WriteFile(path, []byte("refetched"), 0o644))
	d.Sweep(context.Background(), nil)
	_, ok = inv.LifecycleState(name)
	require.True(t, ok, "the name is free again once the retired file is off disk")
}
