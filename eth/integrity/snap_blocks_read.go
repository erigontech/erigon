package integrity

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func SnapBlocksRead(db kv.RoDB, blockReader services.FullBlockReader, ctx context.Context) error {
	maxBlockNum := blockReader.Snapshots().SegmentsMax()
	for i := uint64(0); i < maxBlockNum; i += 10_000 {
		if err := db.View(ctx, func(tx kv.Tx) error {
			b, err := blockReader.BlockByNumber(ctx, tx, i)
			if err != nil {
				return err
			}
			if b == nil {
				return fmt.Errorf("block not found in snapshots: %d\n", i)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
