//go:build !windows

package datasource

import (
	"context"
	"fmt"
	"time"

	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	kv2 "github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func infoStagesLite(tx kv.Tx) (info *StagesInfo, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered from panic: %v", r)
		}
	}()

	info = &StagesInfo{
		StagesProgress: make([]StageProgress, 0, len(stages.AllStages)),
	}

	for _, stage := range stages.AllStages {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		prunedTo, err := stages.GetStagePruneProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		info.StagesProgress = append(info.StagesProgress, StageProgress{
			Stage:    stage,
			Progress: progress,
			PrunedTo: prunedTo,
		})
	}

	return info, nil
}

// InfoAllStages opens the chaindata DB read-only and streams stage snapshots
// until the context is cancelled.
func InfoAllStages(ctx context.Context, logger log.Logger, dataDirPath string, infoCh chan<- *StagesInfo) error {
	dirs := datadir.New(dataDirPath)

	db, err := kv2.New(dbcfg.ChainDB, logger).
		Path(dirs.Chaindata).
		Accede(true).
		Readonly(true).
		Open(ctx)
	if err != nil {
		return fmt.Errorf("open chaindata: %w", err)
	}
	defer db.Close()

	const pollInterval = 5 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		err := db.View(ctx, func(tx kv.Tx) error {
			info, err := infoStagesLite(tx)
			if err != nil {
				return err
			}
			select {
			case infoCh <- info:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		if err != nil {
			logger.Warn("InfoAllStages poll failed", "err", err)
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
