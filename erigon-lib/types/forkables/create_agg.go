package forkables

import (
	"context"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
)

func OpenForkableAgg(ctx context.Context, chain string, stepSize uint64, dirs datadir.Dirs, chainDB kv.RwDB, logger log.Logger) (*state.ForkableAgg, error) {
	forkableAgg := state.NewForkableAgg(context.Background(), dirs, chainDB, logger)
	preverifiedCfg, err := downloadercfg.LoadSnapshotsHashes(context.Background(), dirs, chain)
	if err != nil {
		return nil, err
	}
	rcacheForkable, err := NewRcacheForkable(preverifiedCfg.Preverified.Items, dirs, stepSize, logger)
	if err != nil {
		return nil, err
	}
	forkableAgg.RegisterUnmarkedForkable(rcacheForkable)
	if err := forkableAgg.OpenFolder(); err != nil {
		return nil, err
	}

	return forkableAgg, nil
}
