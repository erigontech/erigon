package checkpoint_sync

import (
	"context"

	"github.com/erigontech/erigon/cl/phase1/core/state"
)

type CheckpointSyncer interface {
	GetLatestBeaconState(ctx context.Context) (*state.CachingBeaconState, error)
}
