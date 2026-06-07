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

package snaptype

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/version"
)

func TestIsCorrectFileName(t *testing.T) {
	t.Parallel()
	require.True(t, IsCorrectFileName("v1.0-000500-001000-headers"))
	require.False(t, IsCorrectFileName("v1.0-000500-headers"))
	require.False(t, IsCorrectFileName("invalid-file-name-too-many-parts"))
}

func TestIsCaplin(t *testing.T) {
	t.Parallel()
	require.True(t, IsCaplin("snapshots/caplin", "v1.0-beaconblocks.0-1.seg"))
	require.True(t, IsCaplin("snapshots", "caplin-something.seg"))
	require.False(t, IsCaplin("snapshots", "v1.0-000500-001000-headers.seg"))
}

func TestIsTorrentPartial(t *testing.T) {
	t.Parallel()
	require.True(t, IsTorrentPartial(".torrent.part"))
	require.False(t, IsTorrentPartial(".torrent"))
	require.False(t, IsTorrentPartial(".seg"))
}

func TestSeedableExtensions(t *testing.T) {
	t.Parallel()
	require.Equal(t, []string{".seg"}, SeedableV2Extensions())
	require.Contains(t, AllV2Extensions(), ".idx")
	require.Contains(t, SeedableV3Extensions(), ".kv")
	require.Contains(t, AllV3Extensions(), ".bt")
}

func TestIsSeedableExtension(t *testing.T) {
	t.Parallel()
	require.True(t, IsSeedableExtension("foo.seg"))
	require.True(t, IsSeedableExtension("foo.kv"))
	require.True(t, IsSeedableExtension("foo.txt"))
	require.False(t, IsSeedableExtension("foo.bogus"))
}

func TestHex2InfoHash(t *testing.T) {
	t.Parallel()
	h := Hex2InfoHash("0102030405060708090a0b0c0d0e0f1011121314")
	require.Equal(t, byte(0x01), h[0])
	require.Equal(t, byte(0x14), h[19])

	require.Panics(t, func() { Hex2InfoHash("not-hex") })
}

func TestFileNameBuilders(t *testing.T) {
	t.Parallel()
	v := version.Version{Major: 1, Minor: 0}

	require.Equal(t, v.String()+"-001000-002000-headers", FileName(v, 1_000_000, 2_000_000, "headers"))
	require.Equal(t, "*-001000-002000-headers", FileMask(1_000_000, 2_000_000, "headers"))

	enum := BeaconBlocks.Enum()
	require.True(t, strings.HasSuffix(SegmentFileName(v, 1_000_000, 2_000_000, enum), ".seg"))
	require.True(t, strings.HasSuffix(IdxFileName(v, 1_000_000, 2_000_000, "headers"), ".idx"))
	require.True(t, strings.HasSuffix(SegmentFileMask(1_000_000, 2_000_000, enum), ".seg"))
	require.True(t, strings.HasSuffix(IdxFileMask(1_000_000, 2_000_000, "headers"), ".idx"))
}

func TestIsStateFile(t *testing.T) {
	t.Parallel()
	require.True(t, IsStateFile("v2.0-accounts.2519-2520.kv"))
	require.True(t, IsStateFileV2("accounts.2519-2520")) // matches "<name>.<num>-<num>"
	require.False(t, IsStateFileV2("accounts.2519-2520.kv"))
	require.False(t, IsStateFile("v1.0-000500-001000-headers.seg"))
	require.False(t, IsStateFile("garbage"))
}

func TestFileInfoMethods(t *testing.T) {
	t.Parallel()
	v := version.Version{Major: 1, Minor: 0}
	fi := FileInfo{
		Version:    v,
		From:       1_000,
		To:         5_000,
		name:       "v1.0-headers.seg",
		Path:       "/snapshots/v1.0-headers.seg",
		Ext:        ".seg",
		Type:       BeaconBlocks,
		TypeString: "beaconblocks",
	}

	require.Equal(t, "v1.0-headers.seg", fi.Name())
	require.Equal(t, "/snapshots", fi.Dir())
	require.Equal(t, "v1.0-headers.seg", fi.Base())
	require.Equal(t, uint64(4_000), fi.Len())

	from, to := fi.GetRange()
	require.Equal(t, uint64(1_000), from)
	require.Equal(t, uint64(5_000), to)
	require.Equal(t, BeaconBlocks, fi.GetType())
	require.Contains(t, fi.GetGrouping(), "beaconblocks")
}

func TestFileInfo_CompareTo(t *testing.T) {
	t.Parallel()
	a := FileInfo{From: 0, To: 1_000, name: "a"}
	b := FileInfo{From: 1_000, To: 2_000, name: "b"}

	require.Negative(t, a.CompareTo(b))
	require.Positive(t, b.CompareTo(a))
	require.Zero(t, a.CompareTo(a)) //nolint:gocritic // intentional self-comparison
}

func TestFileInfo_As(t *testing.T) {
	t.Parallel()
	v := version.Version{Major: 1, Minor: 0}
	fi := FileInfo{Version: v, From: 1_000_000, To: 2_000_000, Ext: ".seg", Path: "/snapshots/x.seg"}

	got := fi.As(BeaconBlocks)
	require.Equal(t, BeaconBlocks, got.Type)
	require.Equal(t, fi.From, got.From)
	require.Equal(t, fi.To, got.To)
	require.True(t, strings.HasSuffix(got.Name(), ".seg"))
}

func TestParseFileName_Invalid(t *testing.T) {
	t.Parallel()
	_, _, ok := ParseFileName("", "invalid-file-name")
	require.False(t, ok)

	_, from, to, rangeOK := ParseRange("garbage")
	require.False(t, rangeOK)
	require.Zero(t, from)
	require.Zero(t, to)
}

func TestParseFileNameOld_StateFile(t *testing.T) {
	t.Parallel()
	_, isStateFile, ok := ParseFileNameOld("snapshots", "v2.0-accounts.2519-2520.kv")
	require.True(t, ok)
	require.True(t, isStateFile)
}

func TestParseDirAndListings(t *testing.T) {
	t.Parallel()
	d := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(d, "v2.0-accounts.2519-2520.kv"), []byte("data"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(d, "leftover.tmp"), []byte("x"), 0o644))

	files, err := ParseDir(d)
	require.NoError(t, err)
	require.NotEmpty(t, files)

	// Non-existent dir is not an error — returns an empty listing.
	missing, err := ParseDir(filepath.Join(d, "nope"))
	require.NoError(t, err)
	require.Empty(t, missing)

	tmps, err := TmpFiles(d)
	require.NoError(t, err)
	require.Len(t, tmps, 1)

	// Exercise the ext-filtered listing paths (FilterExt/FilesWithExt).
	_, err = Segments(d)
	require.NoError(t, err)
	_, err = IdxFiles(d)
	require.NoError(t, err)
}
