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

package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A producer's own output is what triggers AddFile, so if the build mark
// shared storage with the removed-with-intent-to-rebuild mark, the file's
// appearance on disk would clear the mark while the bytes were still moving —
// which is the window a torrent got minted in.
func TestInventory_AddFileDoesNotClearABuildMark(t *testing.T) {
	inv := NewInventory()
	const name = "v2.0-000000-000001-headers.idx"

	inv.MarkProducing(name)
	require.True(t, inv.IsProducing(name))

	require.NoError(t, inv.AddFile(&FileEntry{Name: name, Local: true}))
	require.True(t, inv.IsProducing(name),
		"the file appearing on disk does not mean its producer has finished writing it")

	inv.ClearProducing(name)
	require.False(t, inv.IsProducing(name))
}

// Overlapping producers of one name must not release the mark early.
func TestInventory_BuildMarkIsRefcounted(t *testing.T) {
	inv := NewInventory()
	const name = "v2.0-000000-000001-headers.idx"

	inv.MarkProducing(name)
	inv.MarkProducing(name)
	inv.ClearProducing(name)
	require.True(t, inv.IsProducing(name), "one producer finishing does not free the file")

	inv.ClearProducing(name)
	require.False(t, inv.IsProducing(name))
}
