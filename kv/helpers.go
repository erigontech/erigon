package kv

import (
	"context"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
)

// BigChunks - read `table` by big chunks - restart read transaction after each 5 minutes
func BigChunks(db RoDB, table string, from []byte, walker func(tx Tx, k, v []byte) (bool, error)) error {
	rollbackEvery := time.NewTicker(5 * time.Minute)

	var stop bool
	for !stop {
		if err := db.View(context.Background(), func(tx Tx) error {
			c, err := tx.Cursor(table)
			if err != nil {
				return err
			}
			defer c.Close()

			k, v, err := c.Seek(from)
		Loop:
			for ; k != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}

				ok, err := walker(tx, k, v)
				if err != nil {
					return err
				}
				if !ok {
					stop = true
					break
				}

				select {
				case <-rollbackEvery.C:
					break Loop
				default:
				}
			}

			if k == nil {
				stop = true
			}

			from = common.Copy(k) // next transaction will start from this key

			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}
