package heimdall

import (
	"context"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
)

func ValidateBorSpans(ctx context.Context, logger log.Logger, dirs datadir.Dirs, snaps *RoSnapshots, failFast bool) error {
	baseStore := NewMdbxStore(logger, dirs.DataDir, true, 32)
	snapshotStore := NewSpanSnapshotStore(baseStore.Spans(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] ValidateBorSpans: done", "err", err)
	return err
}

func ValidateBorCheckpoints(ctx context.Context, logger log.Logger, dirs datadir.Dirs, snaps *RoSnapshots, failFast bool) error {
	baseStore := NewMdbxStore(logger, dirs.DataDir, true, 32)
	snapshotStore := NewCheckpointSnapshotStore(baseStore.Checkpoints(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] ValidateBorCheckpoints: done", "err", err)
	return err
}

func ValidateBorMilestones(ctx context.Context, logger log.Logger, dirs datadir.Dirs, snaps *RoSnapshots, failFast bool) error {
	baseStore := NewMdbxStore(logger, dirs.DataDir, true, 32)
	snapshotStore := NewMilestoneSnapshotStore(baseStore.Milestones(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] ValidateBorMilestones: done", "err", err)
	return err
}
