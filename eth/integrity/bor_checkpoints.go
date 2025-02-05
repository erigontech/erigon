package integrity

import (
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func ValidateBorCheckpoints(logger log.Logger, dirs datadir.Dirs, snaps *heimdall.RoSnapshots, failFast bool) error {
	baseStore := heimdall.NewMdbxStore(logger, dirs.DataDir, true, 32)
	snapshotStore := heimdall.NewCheckpointSnapshotStore(baseStore.Checkpoints(), snaps)
	err := snapshotStore.ValidateSnapshots(failFast)
	logger.Info("[integrity] ValidateBorCheckpoints: done", "err", err)
	return err
}
