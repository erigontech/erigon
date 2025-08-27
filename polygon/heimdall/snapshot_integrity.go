package heimdall

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

func ValidateBorSpans(ctx context.Context, logger log.Logger, dirs datadir.Dirs, heimdallStore Store, snaps *RoSnapshots, failFast bool) error {
	snapshotStore := NewSpanSnapshotStore(heimdallStore.Spans(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] BorSpans: done", "err", err)
	return err
}

func ValidateBorCheckpoints(ctx context.Context, logger log.Logger, dirs datadir.Dirs, heimdallStore Store, snaps *RoSnapshots, failFast bool) error {
	snapshotStore := NewCheckpointSnapshotStore(heimdallStore.Checkpoints(), snaps)
	err := snapshotStore.Prepare(ctx)
	if err != nil {
		return err
	}
	defer snapshotStore.Close()
	err = snapshotStore.ValidateSnapshots(ctx, logger, failFast)
	logger.Info("[integrity] BorCheckpoints: done", "err", err)
	return err
}
