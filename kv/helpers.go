package kv

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
)

// BigChunks - read `table` by big chunks - restart read transaction after each 1 minutes
func BigChunks(db RoDB, table string, from []byte, walker func(tx Tx, k, v []byte) (bool, error)) error {
	rollbackEvery := time.NewTicker(1 * time.Minute)

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

				// break loop before walker() call, to make sure all keys are received by walker() exactly once
				select {
				case <-rollbackEvery.C:

					break Loop
				default:
				}

				ok, err := walker(tx, k, v)
				if err != nil {
					return err
				}
				if !ok {
					stop = true
					break
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

var (
	bytesTrue  = []byte{1}
	bytesFalse = []byte{0}
)

func bytes2bool(in []byte) bool {
	if bytes.Equal(in, bytesTrue) {
		return true
	}
	if bytes.Equal(in, bytesFalse) {
		return false
	}
	panic("db must have snapshot cfg record")
}

var ErrChanged = fmt.Errorf("key must not change")

// EnsureNotChangedBool - used to store immutable config flags in db. protects from human mistakes
func EnsureNotChangedBool(tx GetPut, bucket string, k []byte, value bool) error {
	v, err := tx.GetOne(bucket, k)
	if err != nil {
		return err
	}
	if v == nil {
		if value {
			v = bytesTrue
		} else {
			v = bytesFalse
		}
		if err := tx.Put(bucket, k, v); err != nil {
			return err
		}
	}

	enabled := bytes2bool(v)
	if value != enabled {
		return fmt.Errorf("%w: '%s' has value in db: %v, but got %v from outside", ErrChanged, k, enabled, value)
	}
	return nil
}
