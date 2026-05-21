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

package snapshotsync

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestOpenRoSnapshotsWithOverrides builds a live block snapshot set with
// two headers segments, then overlays the second range with a segment
// from an isolated staging directory. The overlaid view must resolve the
// overridden range to the staging file and leave the other range — and
// the live directory — untouched.
func TestOpenRoSnapshotsWithOverrides(t *testing.T) {
	logger := log.New()
	liveDir := t.TempDir()
	stagingDir := t.TempDir()
	headers := snaptype2.Headers
	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}

	createTestSegmentFile(t, 0, 10_000, headers.Enum(), liveDir, version.V1_0, logger)
	createTestSegmentFile(t, 10_000, 20_000, headers.Enum(), liveDir, version.V1_0, logger)
	// The staged variant of the second range lives in an isolated dir.
	createTestSegmentFile(t, 10_000, 20_000, headers.Enum(), stagingDir, version.V1_0, logger)

	stagedSeg := filepath.Join(stagingDir, snaptype.SegmentFileName(version.V1_0, 10_000, 20_000, headers.Enum()))

	s, err := OpenRoSnapshotsWithOverrides(cfg, liveDir, []string{stagedSeg}, []snaptype.Type{headers}, true, logger)
	require.NoError(t, err)
	defer s.Close()

	visible := s.visible.Load().segments[headers.Enum()]
	require.Len(t, visible, 2)
	require.Equal(t, uint64(0), visible[0].from)
	require.True(t, strings.HasPrefix(visible[0].src.FilePath(), liveDir),
		"unoverridden range must resolve to the live directory")
	require.Equal(t, uint64(10_000), visible[1].from)
	require.True(t, strings.HasPrefix(visible[1].src.FilePath(), stagingDir),
		"overridden range must resolve to the staging directory")
}

func TestOpenRoSnapshotsWithOverrides_NoCoveringRange(t *testing.T) {
	logger := log.New()
	liveDir := t.TempDir()
	stagingDir := t.TempDir()
	headers := snaptype2.Headers
	cfg := ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}

	createTestSegmentFile(t, 0, 10_000, headers.Enum(), liveDir, version.V1_0, logger)
	// A staged segment for a range with no live counterpart.
	createTestSegmentFile(t, 20_000, 30_000, headers.Enum(), stagingDir, version.V1_0, logger)

	stagedSeg := filepath.Join(stagingDir, snaptype.SegmentFileName(version.V1_0, 20_000, 30_000, headers.Enum()))

	_, err := OpenRoSnapshotsWithOverrides(cfg, liveDir, []string{stagedSeg}, []snaptype.Type{headers}, true, logger)
	require.ErrorContains(t, err, "no live")
}
