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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
)

func TestPBTAttachMarkerRoundTripAndStartupRefusal(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	refs := false
	variant := TrieVariantHexBin
	marker := &PBTAttachMarker{
		PublishedPath: "/tmp/published",
		Settings: &ErigonDBSettings{
			StepSize:                       8,
			ReferencesInCommitmentBranches: &refs,
			TrieVariant:                    &variant,
		},
	}
	require.NoError(t, WritePBTAttachMarker(dirs, marker))
	got, err := ReadPBTAttachMarker(dirs)
	require.NoError(t, err)
	require.Equal(t, marker.PublishedPath, got.PublishedPath)
	require.Equal(t, marker.Settings.StepSize, got.Settings.StepSize)
	require.ErrorContains(t, RefusePBTAttachMarker(dirs), "rerun attach-pbt --from /tmp/published")
	require.NoError(t, RemovePBTAttachMarker(dirs))
	_, err = os.Stat(PBTAttachMarkerPath(dirs))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, RefusePBTAttachMarker(dirs))
}
