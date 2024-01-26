package integrity

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

func SnapBlocksRead(db kv.RoDB, blockReader services.FullBlockReader, ctx context.Context, failFast bool) error {
	defer log.Info("[integrity] SnapBlocksRead: done")
	logEvery := time.NewTicker(10 * time.Second)
	defer logEvery.Stop()

	maxBlockNum := blockReader.Snapshots().SegmentsMax()
	for i := uint64(0); i < maxBlockNum; i += 10_000 {
		if err := db.View(ctx, func(tx kv.Tx) error {
			b, err := blockReader.BlockByNumber(ctx, tx, i)
			if err != nil {
				return err
			}
			if b == nil {
				err := fmt.Errorf("block not found in snapshots: %d\n", i)
				if failFast {
					return err
				}
				log.Error("[integrity] SnapBlocksRead", "err", err)
			}
			return nil
		}); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-logEvery.C:
			log.Info("[integrity] SnapBlocksRead", "blockNum", fmt.Sprintf("%dK/%dK", i/1000, maxBlockNum/1000))
		default:
		}
	}
	return nil
}
