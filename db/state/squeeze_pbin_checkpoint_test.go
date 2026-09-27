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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

func TestReadPBinRebuildCheckpointRejectsChangedSpill(t *testing.T) {
	dir := t.TempDir()
	spillPath := filepath.Join(dir, "rows")
	checkpointPath := filepath.Join(dir, "checkpoint")
	require.NoError(t, os.WriteFile(spillPath, []byte{1, 2, 3}, 0o644))
	overlay := newPBinRebuildOverlay().withSpill(spillPath)
	target := RebuildTarget{Variant: commitment.VariantBinPatriciaTrie, HashName: commitment.PBinHashBlake3}
	require.NoError(t, writePBinRebuildCheckpoint(checkpointPath, []byte{1}, overlay, target))
	require.NoError(t, os.WriteFile(spillPath, []byte{4, 5, 6}, 0o644))
	_, err := readPBinRebuildCheckpoint(checkpointPath, target)
	require.ErrorContains(t, err, "checkpoint and spill disagree")
}
