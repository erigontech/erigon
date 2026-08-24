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

package diskutils

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Sample lines from /proc/self/mountinfo: fields 6..n are optional and only
// present on some mounts, so the filesystem type has to be located after "-".
const mountinfoSample = `21 27 0:20 / /proc rw,relatime shared:5 - proc proc rw
26 27 0:24 / /run rw,nosuid,nodev shared:2 - tmpfs tmpfs rw,size=1608940k
27 1 8:2 / / rw,relatime shared:1 - ext4 /dev/sda2 rw
36 27 8:17 / /mnt/data rw,relatime shared:22 - xfs /dev/sdb1 rw,attr2
48 27 0:52 / /mnt/pool rw,relatime shared:31 - zfs pool/erigon rw,xattr
52 27 0:60 /subvol /mnt/btr rw,relatime shared:41 - btrfs /dev/sdc1 rw,ssd
64 27 0:71 / /var/lib/docker/overlay rw,relatime - overlay overlay rw,lowerdir=/a
70 27 253:0 / /mnt/noopt rw,relatime - ext4 /dev/dm-0 rw
`

func TestIsRecommendedFilesystem(t *testing.T) {
	for _, tc := range []struct {
		fsType string
		want   bool
	}{
		{"ext4", true},
		{"xfs", true},
		{"EXT4", true},
		{"XFS", true},
		{"zfs", false},
		{"btrfs", false},
		{"apfs", false},
		{"overlay", false},
		{"NTFS", false},
		{"ext3", false},
		{"", false},
	} {
		require.Equal(t, tc.want, IsRecommendedFilesystem(tc.fsType), tc.fsType)
	}
}

func TestFilesystemType(t *testing.T) {
	fsType, err := FilesystemType(t.TempDir())
	require.NoError(t, err)
	require.NotEmpty(t, fsType)
}

func TestUnrecommendedByType(t *testing.T) {
	fsTypeOf := func(dirPath string) (string, error) {
		switch dirPath {
		case "/data", "/data/chaindata":
			return "zfs", nil
		case "/snap", "/snap/domain":
			return "btrfs", nil
		case "/fast":
			return "ext4", nil
		case "/xfs":
			return "XFS", nil
		}
		return "", errors.New("no such mount")
	}

	groups, failed := unrecommendedByType(
		[]string{"/fast", "/data", "/snap", "/xfs", "/data/chaindata", "/snap/domain", "/gone"}, fsTypeOf)

	require.Equal(t, []fsGroup{
		{fsType: "zfs", paths: []string{"/data"}},
		{fsType: "btrfs", paths: []string{"/snap"}},
	}, groups)
	require.Len(t, failed, 1)
	require.Error(t, failed["/gone"])
}

func TestUnrecommendedByTypeKeepsSeparateMounts(t *testing.T) {
	fsTypeOf := func(dirPath string) (string, error) {
		switch dirPath {
		case "/data":
			return "ext4", nil
		case "/data/chaindata", "/mnt/snap":
			return "zfs", nil
		}
		return "", errors.New("no such mount")
	}

	groups, _ := unrecommendedByType([]string{"/data", "/data/chaindata", "/mnt/snap"}, fsTypeOf)

	require.Equal(t, []fsGroup{{fsType: "zfs", paths: []string{"/data/chaindata", "/mnt/snap"}}}, groups)
}

func TestUnrecommendedByTypeAncestorReplacesDescendant(t *testing.T) {
	fsTypeOf := func(string) (string, error) { return "zfs", nil }

	groups, _ := unrecommendedByType([]string{"/data/chaindata", "/data-other", "/data"}, fsTypeOf)

	require.Equal(t, []fsGroup{{fsType: "zfs", paths: []string{"/data-other", "/data"}}}, groups)
}

func TestUnrecommendedByTypeAllRecommended(t *testing.T) {
	fsTypeOf := func(string) (string, error) { return "ext4", nil }

	groups, failed := unrecommendedByType([]string{"/a", "/b"}, fsTypeOf)

	require.Empty(t, groups)
	require.Empty(t, failed)
}

func TestUnrecommendedByTypeSkipsDuplicatePaths(t *testing.T) {
	fsTypeOf := func(string) (string, error) { return "zfs", nil }

	groups, _ := unrecommendedByType([]string{"/data", "/data", ""}, fsTypeOf)

	require.Equal(t, []fsGroup{{fsType: "zfs", paths: []string{"/data"}}}, groups)
}

func TestFsTypeFromMountinfo(t *testing.T) {
	for _, tc := range []struct {
		devID string
		want  string
	}{
		{"8:2", "ext4"},
		{"8:17", "xfs"},
		{"0:52", "zfs"},
		{"0:60", "btrfs"},
		{"0:71", "overlay"},
		{"253:0", "ext4"},
	} {
		got, err := fsTypeFromMountinfo(strings.NewReader(mountinfoSample), tc.devID)
		require.NoError(t, err, tc.devID)
		require.Equal(t, tc.want, got, tc.devID)
	}
}

func TestFsTypeFromMountinfoUnknownDevice(t *testing.T) {
	_, err := fsTypeFromMountinfo(strings.NewReader(mountinfoSample), "99:99")
	require.Error(t, err)
}

func TestFilesystemTypeMissingPath(t *testing.T) {
	_, err := FilesystemType(t.TempDir() + "/does-not-exist")
	require.Error(t, err)
}
