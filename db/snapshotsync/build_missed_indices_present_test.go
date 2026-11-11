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
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// openUnindexedSegment gives a view that has opened a .seg while its index was
// absent, so the segment carries no index in memory. The returned path is where
// the index belongs — at the index type's current version, which need not be
// the .seg's.
func openUnindexedSegment(t *testing.T, dirs datadir.Dirs, logger log.Logger) (*BaseRoSnapshots, string) {
	t.Helper()
	_, err := snaptype.LoadSalt(dirs.Snap, true, logger)
	require.NoError(t, err)

	createTestSegmentFile(t, 0, 1_000, snaptype2.Headers.Enum(), dirs.Snap, version.V1_0, logger)
	require.NoError(t, dir.RemoveFile(filepath.Join(dirs.Snap,
		snaptype.IdxFileName(version.V1_0, 0, 1_000, snaptype2.Headers.Enum().String()))))

	names := snaptype2.Headers.IdxFileNames(0, 1_000)
	require.Len(t, names, 1)
	idxPath := filepath.Join(dirs.Snap, names[0])
	require.NoFileExists(t, idxPath)

	s := NewBaseRoSnapshots(
		ethconfig.BlocksFreezing{ChainName: networkname.Mainnet},
		dirs.Snap, snaptype2.BlockSnapshotTypes, snaptype2.Transactions, true, logger)
	t.Cleanup(s.Close)
	require.NoError(t, s.OpenFolder())
	require.NoFileExists(t, idxPath, "OpenFolder must not have built anything")
	return s, idxPath
}

// writeIndexWithKeys puts a valid, openable index at path. keys picks how many
// entries it holds, which is what makes its bytes distinguishable from the
// one-key index a local rebuild of the test .seg would produce.
func writeIndexWithKeys(t *testing.T, path, tmpDir string, keys int, logger log.Logger) {
	t.Helper()
	idx, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   keys,
		BucketSize: 10,
		TmpDir:     tmpDir,
		IndexFile:  path,
		LeafSize:   8,
	}, logger)
	require.NoError(t, err)
	defer idx.Close()
	idx.DisableFsync()
	for i := range keys {
		require.NoError(t, idx.AddKey([]byte{byte(i + 1)}, uint64(i)))
	}
	require.NoError(t, idx.Build(t.Context()))
}

func sha256File(t *testing.T, path string) [32]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return sha256.Sum256(b)
}

// An index that is present and opens is not a missed index. The segment holds
// no index in memory only because the file arrived after the folder was opened;
// rebuilding it replaces bytes a torrent was already minted over, which is what
// makes a publisher fail its own infohash check.
func TestBuildMissedIndices_AdoptsAnIndexAlreadyOnDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	logger := log.New()
	dirs := datadir.New(t.TempDir())

	s, idxPath := openUnindexedSegment(t, dirs, logger)
	writeIndexWithKeys(t, idxPath, dirs.Tmp, 2, logger)
	before := sha256File(t, idxPath)

	built, err := s.buildMissedIndices("test", t.Context(), dirs, nil, 1, logger)
	require.NoError(t, err)

	require.Equal(t, before, sha256File(t, idxPath),
		"the index on disk was rewritten; a hash already taken over it no longer describes it")
	require.False(t, built, "nothing was missing, so nothing was built")
}

// The converse, so adoption cannot become a blanket refusal to index: with no
// index on disk, building it locally is the only way it comes to exist.
func TestBuildMissedIndices_BuildsAnIndexThatIsAbsent(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	logger := log.New()
	dirs := datadir.New(t.TempDir())

	s, idxPath := openUnindexedSegment(t, dirs, logger)

	built, err := s.buildMissedIndices("test", t.Context(), dirs, nil, 1, logger)
	require.NoError(t, err)

	require.FileExists(t, idxPath, "an absent index has to be built locally")
	require.True(t, built)
}

// Present is not enough — the file must open. A truncated or otherwise unusable
// index is still a missed one.
func TestBuildMissedIndices_RebuildsAnIndexThatDoesNotOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	logger := log.New()
	dirs := datadir.New(t.TempDir())

	s, idxPath := openUnindexedSegment(t, dirs, logger)
	require.NoError(t, os.WriteFile(idxPath, []byte("not an index"), 0o644))
	before := sha256File(t, idxPath)

	built, err := s.buildMissedIndices("test", t.Context(), dirs, nil, 1, logger)
	require.NoError(t, err)

	require.NotEqual(t, before, sha256File(t, idxPath),
		"an index that does not open must be replaced by a real one")
	require.True(t, built)
}
