package state

import (
	"bytes"
	"context"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// toPrefix exclusive
func DeleteRangeFromTbl(ctx context.Context, tbl string, fromPrefix, toPrefix []byte, limit uint64, logEvery *time.Ticker, logger log.Logger, rwTx kv.RwTx) (delCount uint64, err error) {
	c, err := rwTx.RwCursor(tbl) // TODO: no dupsort tbl assumed
	if err != nil {
		return
	}
	defer c.Close()

	if logEvery == nil {
		logEvery = time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
	}

	// bigendianess assumed (for key comparison)
	// imo this can be generalized if needed, by using key comparison functions, which mdbx provides.
	for k, _, err := c.Seek(fromPrefix); k != nil && (toPrefix == nil || bytes.Compare(k, toPrefix) < 0) && limit > 0; k, _, err = c.Next() {
		if err != nil {
			return delCount, err
		}

		if err := c.DeleteCurrent(); err != nil {
			return delCount, err
		}
		limit--
		delCount++
		select {
		case <-logEvery.C:
			logger.Info("DeleteRange", "tbl", tbl, "from", fromPrefix, "to", toPrefix, "limit", limit, "del", delCount)
		case <-ctx.Done():
			logger.Info("DeleteRange cancelled", "tbl", tbl, "del", delCount)
			return delCount, ctx.Err()
		default:
		}
	}

	return
}
