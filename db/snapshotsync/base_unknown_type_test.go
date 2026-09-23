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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestBaseRoSnapshotsOpenFolderSkipsUnknownCaplinType(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	name := "v1.1-000000-000050-BlockProposers.seg"
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapCaplin, name), []byte("unknown"), 0o644))

	snapshots := NewBaseRoSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dirs.SnapCaplin,
		[]snaptype.Type{snaptype.PendingDepositsDump},
		snaptype.PendingDepositsDump,
		false,
		log.New(),
	)
	t.Cleanup(snapshots.Close)

	require.NoError(t, snapshots.OpenList([]string{name}, false))
	require.NoError(t, snapshots.OpenFolder())
}

func TestFindOverlapsSkipsUnknownCaplinType(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	unknownName := "v1.1-000000-000050-BlockProposers.seg"
	validNames := []string{
		snaptype.PendingDepositsDump.FileName(version.V1_1, 0, 50_000),
		snaptype.PendingDepositsDump.FileName(version.V1_1, 50_000, 100_000),
	}
	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapCaplin, unknownName), []byte("unknown"), 0o644))
	for _, name := range validNames {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapCaplin, name), []byte("valid"), 0o644))
	}

	segments, err := snaptype.Segments(dirs.SnapCaplin)
	require.NoError(t, err)
	kept, overlapped := findOverlaps(segments)
	require.Len(t, kept, len(validNames))
	require.Empty(t, overlapped)
	for i, name := range validNames {
		require.Equal(t, name, kept[i].Name())
		require.Equal(t, kv.PendingDepositsDump+"_"+kv.PendingDepositsDump+"_.seg", kept[i].GetGrouping())
	}
}
