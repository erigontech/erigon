package sync

import (
	"context"

	"github.com/ledgerwatch/erigon/polygon/heimdall"
)

//go:generate mockgen -typed=true -source=./heimdall_waypoints_fetcher.go -destination=./heimdall_waypoints_fetcher_mock.go -package=sync
type heimdallWaypointsFetcher interface {
	FetchCheckpointsFromBlock(ctx context.Context, startBlock uint64) (heimdall.Waypoints, error)
	FetchMilestonesFromBlock(ctx context.Context, startBlock uint64) (heimdall.Waypoints, error)
}
