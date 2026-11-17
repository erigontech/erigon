package app

import (
	"fmt"
	"math"
	"os"
	"path"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
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
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, true, nil))
}

func Test_CheckPtcWindowEpochRequired(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	ranges := []snapRange{{0, 10}, {10, 20}}
	touchFiles(t, dirs, ranges)
	for _, r := range ranges {
		delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.seg", r.fromStep, r.toStep, kv.PtcWindowEpochTable))
		delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.idx", r.fromStep, r.toStep, kv.PtcWindowEpochTable))
	}
	err := checkIfCaplinSnapshotsPublishable(dirs, true, nil)
	require.ErrorContains(t, err, kv.PtcWindowEpochTable)
}

func Test_CheckPtcWindowEpochReachesCommonEnd(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{{0, 10}, {10, 20}, {20, 30}})
	delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-000020-000030-%s.seg", kv.PtcWindowEpochTable))
	delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-000020-000030-%s.idx", kv.PtcWindowEpochTable))
	err := checkIfCaplinSnapshotsPublishable(dirs, true, nil)
	require.ErrorContains(t, err, "must end at")
}

func Test_CheckPtcWindowEpochBeforeUnscheduledFork(t *testing.T) {
	for _, keepPtcRanges := range [][]snapRange{nil, {{0, 10}, {10, 20}}} {
		dirs := datadir.New(t.TempDir())
		ranges := []snapRange{{0, 10}, {10, 20}, {20, 30}}
		touchFiles(t, dirs, ranges)
		removePtcWindowEpochFiles(t, dirs, ranges)
		for _, r := range keepPtcRanges {
			touchCaplinStateFiles(t, dirs, kv.PtcWindowEpochTable, r)
		}

		cfg := clparams.MainnetBeaconConfig
		cfg.SlotsPerEpoch = 32
		cfg.GloasForkEpoch = math.MaxUint64
		require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, true, &cfg))
	}
}

func Test_CheckPtcWindowEpochBeforeFutureFork(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	ranges := []snapRange{{0, 10}, {10, 20}, {20, 30}}
	touchFiles(t, dirs, ranges)
	removePtcWindowEpochFiles(t, dirs, ranges)

	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 32
	cfg.GloasForkEpoch = 1000
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, true, &cfg))
}

func Test_CheckPtcWindowEpochRequiredAfterFork(t *testing.T) {
	tests := []struct {
		name       string
		keepRanges []snapRange
		wantError  string
	}{
		{name: "absent", wantError: kv.PtcWindowEpochTable},
		{name: "short", keepRanges: []snapRange{{0, 10}, {10, 20}}, wantError: "must end at"},
		{name: "complete", keepRanges: []snapRange{{0, 10}, {10, 20}, {20, 30}}},
		{name: "past common end", keepRanges: []snapRange{{0, 10}, {10, 20}, {20, 30}, {30, 40}}, wantError: "must end at"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			ranges := []snapRange{{0, 10}, {10, 20}, {20, 30}}
			touchFiles(t, dirs, ranges)
			removePtcWindowEpochFiles(t, dirs, ranges)
			for _, r := range test.keepRanges {
				touchCaplinStateFiles(t, dirs, kv.PtcWindowEpochTable, r)
			}

			cfg := clparams.MainnetBeaconConfig
			cfg.SlotsPerEpoch = 32
			cfg.GloasForkEpoch = 625
			err := checkIfCaplinSnapshotsPublishable(dirs, true, &cfg)
			if test.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.wantError)
			}
		})
	}
}

func Test_CheckPtcWindowEpochForkBoundary(t *testing.T) {
	for _, test := range []struct {
		name           string
		gloasForkEpoch uint64
		wantError      bool
	}{
		{name: "at common end", gloasForkEpoch: 30},
		{name: "before common end", gloasForkEpoch: 29, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			ranges := []snapRange{{0, 10}, {10, 20}, {20, 30}}
			touchFiles(t, dirs, ranges)
			removePtcWindowEpochFiles(t, dirs, ranges)

			cfg := clparams.MainnetBeaconConfig
			cfg.SlotsPerEpoch = 1000
			cfg.GloasForkEpoch = test.gloasForkEpoch
			err := checkIfCaplinSnapshotsPublishable(dirs, true, &cfg)
			if test.wantError {
				require.ErrorContains(t, err, kv.PtcWindowEpochTable)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_CheckPtcWindowEpochGapBeforeUnscheduledFork(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	ranges := []snapRange{{0, 10}, {10, 20}, {20, 30}}
	touchFiles(t, dirs, ranges)
	delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-000010-000020-%s.seg", kv.PtcWindowEpochTable))
	delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-000010-000020-%s.idx", kv.PtcWindowEpochTable))

	cfg := clparams.MainnetBeaconConfig
	cfg.SlotsPerEpoch = 32
	cfg.GloasForkEpoch = math.MaxUint64
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, true, &cfg))
}

func Test_CheckNormal(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_CheckGaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {20, 30},
	})
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_CheckStartFrom0(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{10, 20}, {20, 30},
	})
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_CheckAllowedNonStartFrom0(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})

	delFile(t, dirs.Snap, "v1.0-000000-000010-blocksidecars.idx") // blobsidecars start at 1942 step on mainnet after dencun upgrade
	delFile(t, dirs.Snap, "v1.0-000000-000010-blobsidecars.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	// but removing other files should still error
	delFile(t, dirs.Snap, "v1.0-000000-000010-beaconblocks.idx")
	delFile(t, dirs.Snap, "v1.0-000000-000010-beaconblocks.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_CheckOverlaps(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 15}, {10, 30},
	})
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_OneFileMissing(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	// delete one idx file
	delFile(t, dirs.Snap, "v1.0-000010-000020-beaconblocks.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
	// delete seg file
	delFile(t, dirs.SnapCaplin, "v1.0-000010-000020-ActiveValidatorIndicies.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_LastFileMissingForOneEnum(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	// delete one idx file
	delFile(t, dirs.SnapCaplin, "v1.0-000020-000030-BlockRoot.idx")
	delFile(t, dirs.SnapCaplin, "v1.0-000020-000030-BlockRoot.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_VersionLessThanMin(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	touchFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	touchFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.Snap, "v0.9-000010-000020-beaconblocks.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	touchFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	touchFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.SnapCaplin, "v0.9-000010-000020-ActiveValidatorIndicies.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

func Test_VersionMoreThanCurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, []snapRange{
		{0, 10}, {10, 20}, {20, 30},
	})
	touchFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	touchFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.Snap, "v20.0-000010-000020-beaconblocks.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	touchFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.seg")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.seg")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	touchFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.idx")
	require.Error(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))

	delFile(t, dirs.SnapCaplin, "v20.0-000010-000020-ActiveValidatorIndicies.idx")
	require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, false, nil))
}

// A pre-GLOAS datadir registers the GLOAS (EIP-7732) state tables but has no
// files for them. Publishable must tolerate fully-empty registered tables and
// must not depend on map iteration order (the empty tables must not poison the
// last-file-to reference of the present ones).
func Test_CheckEmptyFutureForkTablesTolerated(t *testing.T) {
	emptyTables := []string{
		kv.Builders, kv.BuildersDump,
		kv.BuilderPendingWithdrawals, kv.BuilderPendingWithdrawalsDump,
		kv.PayloadExpectedWithdrawals, kv.PayloadExpectedWithdrawalsDump,
		kv.ExecutionPayloadAvailabilityTable, kv.BuilderPendingPaymentsTable,
		kv.LatestExecutionPayloadBidTable,
	}
	ranges := []snapRange{{0, 10}, {10, 20}, {20, 30}}
	dirs := datadir.New(t.TempDir())
	touchFiles(t, dirs, ranges)
	for _, table := range emptyTables {
		for _, r := range ranges {
			delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.seg", r.fromStep, r.toStep, table))
			delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.idx", r.fromStep, r.toStep, table))
		}
	}
	for range 30 {
		require.NoError(t, checkIfCaplinSnapshotsPublishable(dirs, true, nil))
	}
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
	stateSnapTypes := snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil)

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

func touchCaplinStateFiles(t *testing.T, dirs datadir.Dirs, table string, r snapRange) {
	t.Helper()
	touchFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.seg", r.fromStep, r.toStep, table))
	touchFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.idx", r.fromStep, r.toStep, table))
}

func removePtcWindowEpochFiles(t *testing.T, dirs datadir.Dirs, ranges []snapRange) {
	t.Helper()
	for _, r := range ranges {
		delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.seg", r.fromStep, r.toStep, kv.PtcWindowEpochTable))
		delFile(t, dirs.SnapCaplin, fmt.Sprintf("v1.0-%06d-%06d-%s.idx", r.fromStep, r.toStep, kv.PtcWindowEpochTable))
	}
}

func delFile(t *testing.T, folder string, filename string) {
	t.Helper()
	fullpath := path.Join(folder, filename)
	require.NoError(t, dir.RemoveFile(fullpath))
}
