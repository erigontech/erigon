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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// setManifest replaces the chain's manifest for the duration of the test.
func setManifest(t *testing.T, chain string, toml string) {
	t.Helper()
	was := snapcfg.GetToml(chain)
	t.Cleanup(func() { snapcfg.SetToml(chain, was, false) })
	snapcfg.SetToml(chain, []byte(toml), false)
}

// unindexedSnapshotsWithSeg gives a view holding one .seg whose index is absent,
// which is what makes buildMissedIndices consider the range missing. The
// returned path is where a local build would put the index — at the index
// type's current version, which need not be the .seg's.
func unindexedSnapshotsWithSeg(t *testing.T, dirs datadir.Dirs, logger log.Logger) (*BaseRoSnapshots, string) {
	t.Helper()
	_, err := snaptype.LoadSalt(dirs.Snap, true, logger)
	require.NoError(t, err)

	createTestSegmentFile(t, 0, 1_000, snaptype2.Headers.Enum(), dirs.Snap, version.V1_0, logger)
	require.NoError(t, dir.RemoveFile(filepath.Join(dirs.Snap,
		snaptype.IdxFileName(version.V1_0, 0, 1_000, snaptype2.Headers.Enum().String()))))

	built := snaptype2.Headers.IdxFileNames(0, 1_000)
	require.Len(t, built, 1)

	s := NewBaseRoSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dirs.Snap, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())
	return s, filepath.Join(dirs.Snap, built[0])
}

// Building an index the manifest already names produces a second, divergent
// copy: the index embeds this node's salt and is written at this binary's
// current index version, so neither its bytes nor necessarily its name match
// what the manifest's hash was taken over. A publisher then serves a file that
// fails its own infohash check. Where a remote source exists, it is the source.
func TestBuildMissedIndices_SkipsWhatTheManifestNames(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	logger := log.New()
	dirs := datadir.New(t.TempDir())

	s, idxPath := unindexedSnapshotsWithSeg(t, dirs, logger)
	setManifest(t, networkname.Mainnet, `"v1.0-000000-000001-headers.seg" = "aa"
"`+filepath.Base(idxPath)+`" = "bb"
`)

	_, err := s.buildMissedIndices("test", t.Context(), dirs, nil, 1, logger)
	require.NoError(t, err)

	require.NoFileExists(t, idxPath,
		"an index the manifest names must be downloaded, not built: a local build diverges from the hash")
}

// The converse, so the skip cannot be a blanket refusal to index: with no
// remote source named, local building is the only way the index exists.
func TestBuildMissedIndices_BuildsWhatTheManifestDoesNotName(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	logger := log.New()
	dirs := datadir.New(t.TempDir())

	s, idxPath := unindexedSnapshotsWithSeg(t, dirs, logger)
	setManifest(t, networkname.Mainnet, `"v1.0-000000-000001-headers.seg" = "aa"
`)

	_, err := s.buildMissedIndices("test", t.Context(), dirs, nil, 1, logger)
	require.NoError(t, err)

	require.FileExists(t, idxPath,
		"with no remote source for the index, it has to be built locally")
}
