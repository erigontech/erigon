package app

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/stretchr/testify/require"
)

// checks:
// - no gaps
// - starts from 0
// - more than 0 files
// - no overlaps
// - each data file has corresponding index/bt etc.1
// - versions: between min supported version and max
// - lastFileTo check
// - sum = maxTo check (probably redu

func Test_CheckEmpty(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_CheckNormal(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_CheckGaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {20, 30},
	})
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_CheckStartFrom0(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{10, 20}, {20, 30},
	})
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_CheckAllowedNonStartFrom0(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})

	delFile(t, dirs.Snap, "v1.0-000000-000010-blocksidecars.idx") // blobsidecars start at 1942 step on mainnet after dencun upgrade
	delFile(t, dirs.Snap, "v1.0-000000-000010-blobsidecars.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	// but removing other files should still error
	delFile(t, dirs.Snap, "v1.0-000000-000010-beaconblocks.idx")
	delFile(t, dirs.Snap, "v1.0-000000-000010-beaconblocks.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_CheckOverlaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 15}, {10, 30},
	})
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_OneFileMissing(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	// delete one idx file
	delFile(t, dirs.Snap, "v1.0-000010-000020-beaconblocks.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))
	// delete seg file
	delFile(t, dirs.SnapCaplin, "v1.0-000010-000020-ActiveValidatorIndicies.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_LastFileMissingForOneEnum(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	// delete one idx file
	delFile(t, dirs.SnapCaplin, "v1.0-000020-000030-BlockRoot.idx")
	delFile(t, dirs.SnapCaplin, "v1.0-000020-000030-BlockRoot.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_VersionLessThanMin(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	touchFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	touchFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	touchFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	touchFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

func Test_VersionMoreThanCurrent(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	touchFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	touchFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	touchFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	touchFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false))

	delFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false))
}

type snapRange struct {
	fromStep, toStep uint64
}

func touchFile(t *testing.T, folder string, filename string) {
	t.Helper()
	fullpath := path.Join(folder, filename)
	f, err := os.Create(fullpath)
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func touchFiles(t *testing.T, dirs datadir.Dirs, ranges []snapRange) {
	t.Helper()

	for _, snapt := range snaptype.CaplinSnapshotTypes {
		dataFileTag := snapt.Name()
		indexTag := snapt.Indexes()[0].Name
		// 'v1.1-000000-000100-beaconblocks.idx'
		// 'v1.1-000000-000100-beaconblocks.seg'

		for _, r := range ranges {
			datafile := fmt.Sprintf("v1.0-%06d-%06d-%s.seg", r.fromStep, r.toStep, dataFileTag)
			indexfile := fmt.Sprintf("v1.0-%06d-%06d-%s.idx", r.fromStep, r.toStep, indexTag)

			touchFile(t, dirs.Snap, datafile)
			touchFile(t, dirs.Snap, indexfile)
		}
	}
	stateSnapTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(nil)

	for table := range stateSnapTypes.KeyValueGetters {
		// caplin/v1.1-000000-010500-ActiveValidatorIndicies.idx
		// caplin/v1.1-000000-010500-ActiveValidatorIndicies.seg

		for _, r := range ranges {
			datafile := fmt.Sprintf("v1.0-%06d-%06d-%s.seg", r.fromStep, r.toStep, table)
			indexfile := fmt.Sprintf("v1.0-%06d-%06d-%s.idx", r.fromStep, r.toStep, table)

			touchFile(t, dirs.SnapCaplin, datafile)
			touchFile(t, dirs.SnapCaplin, indexfile)
		}
	}
}

func delFile(t *testing.T, folder string, filename string) {
	t.Helper()
	fullpath := path.Join(folder, filename)
	require.NoError(t, dir.RemoveFile(fullpath))
}
