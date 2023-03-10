package stagedsync

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"
)

type ParallelWorkerGroup struct {
	GroupCtx context.Context
	Done     <-chan error
}

func SpawnWorkers(ctx context.Context, start, end uint64, numPartitions int,
	worker func(uint64, uint64, int, context.Context) error,
	cleanup func()) *ParallelWorkerGroup {
	if uint64(numPartitions) > end-start+1 {
		numPartitions = int(end - start + 1)
	}
	partitionSize := (end - start + 1) / uint64(numPartitions)
	if partitionSize == 0 {
		partitionSize = 1
	}
	log.Info("Spawn parallel workers", "numPartitions", numPartitions, "partitionSize", partitionSize, "start", start, "end", end)
	g, gctx := errgroup.WithContext(ctx)
	for i := 0; i < numPartitions; i++ {
		partitionStart := start + uint64(i)*partitionSize
		partitionEnd := partitionStart + partitionSize - 1
		if i == numPartitions-1 {
			partitionEnd = end
		}
		i := i
		g.Go(func() error {
			startTime := time.Now()
			log.Info("Parallel worker start", "i", i, "partitionStart", partitionStart, "partitionEnd", partitionEnd)
			defer func() {
				log.Info("Parallel worker done", "i", i, "duration", time.Since(startTime))
			}()
			return worker(partitionStart, partitionEnd, i, gctx)
		})
	}
	doneCh := make(chan error, 1)
	ret := &ParallelWorkerGroup{
		GroupCtx: gctx,
		Done:     doneCh,
	}
	go func() {
		defer close(doneCh)
		doneCh <- g.Wait()
		cleanup()
	}()
	return ret
}

func SpawnWorkersWithRoTx(ctx context.Context,
	db kv.RwDB, start, end uint64, numPartitions int,
	worker func(uint64, uint64, int, context.Context, kv.Tx) error,
	cleanup func()) *ParallelWorkerGroup {
	return SpawnWorkers(ctx, start, end, numPartitions,
		func(partitionStart, partitionEnd uint64, i int, gctx context.Context) error {
			roTx, err := db.BeginRo(ctx)
			if err != nil {
				return err
			}
			defer roTx.Rollback()
			return worker(partitionStart, partitionEnd, i, gctx, roTx)
		}, cleanup)
}
