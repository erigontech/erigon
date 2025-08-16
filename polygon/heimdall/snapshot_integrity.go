package heimdall

import (
	"context"

	"github.com/erigontech/erigon/db/datadir"
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
	logger.Info("[integrity] BorSpans: done", "err", err)
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
	logger.Info("[integrity] BorCheckpoints: done", "err", err)
	return err
}
